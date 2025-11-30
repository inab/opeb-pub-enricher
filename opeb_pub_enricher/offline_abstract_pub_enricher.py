#!/usr/bin/env python3

from abc import abstractmethod
import configparser
import pathlib

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

import diskcache

if TYPE_CHECKING:
    from typing import (
        Iterable,
        Iterator,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        Union,
    )

    from .doi_cache import DOIChecker

    from .pub_cache import (
        GatheredCitations,
        GatheredCitRefs,
        GatheredReferences,
        IdMapping,
        IdMappingMinimal,
        PubDBCache,
        QueryId,
        Reference,
    )

    from .pub_common import (
        EnricherId,
        SourceId,
        UnqualifiedId,
    )


from .abstract_pub_enricher import AbstractPubEnricher

from . import pub_common


class OfflineAbstractPubEnricher(AbstractPubEnricher):
    BATCH_THRESHOLD = 10240

    @overload
    def __init__(
        self,
        cache: "str",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
        is_db_synchronous: "bool" = True,
    ): ...

    @overload
    def __init__(
        self,
        cache: "PubDBCache",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
        is_db_synchronous: "bool" = True,
    ): ...

    def __init__(
        self,
        cache: "Union[str, PubDBCache]",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
        is_db_synchronous: "bool" = True,
    ):
        super().__init__(
            cache,
            prefix=prefix,
            config=config,
            doi_checker=doi_checker,
            is_db_synchronous=is_db_synchronous,
        )

        # The section name is the symbolic name given to this class
        # section_name = self.Name()

        self._upstream_cache_dir = pathlib.Path(self.cache_dir) / (
            self.Name() + "_UPSTREAM"
        )
        self._upstream_cache_tracker = diskcache.Cache(
            self._upstream_cache_dir.as_posix(), eviction_policy="none"
        )

        with self._upstream_cache_tracker.transact():
            dir_entries = self.mirror_upstream(
                self._upstream_cache_dir,
                cast("Mapping[str, Tuple[bytes, int]]", self._upstream_cache_tracker),
            )

        if len(dir_entries) > 0:
            with self.pubC:
                self.digest_upstream_dir_entries(dir_entries)
                # pass

    @classmethod
    @abstractmethod
    def Name(cls) -> "EnricherId":
        return cast("EnricherId", "offline_abstract")

    @classmethod
    @abstractmethod
    def DefaultSource(cls) -> "SourceId":
        return cast("SourceId", "abstract")

    @classmethod
    def DefaultDeleteStaleCache(cls) -> "bool":
        return False

    def queryPubIdsBatch(self, query_ids: "Sequence[QueryId]") -> "Sequence[IdMapping]":
        self.logger.warning(
            f"This method was called to query about {len(query_ids)} potential identifiers"
        )
        return []

    def queryCitRefsBatch(
        self,
        query_citations_data: "Iterable[IdMappingMinimal]",
        minimal: "bool" = False,
        mode: "int" = 3,
    ) -> "Iterator[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]":
        # As
        self.logger.warning(
            f"This method was called to get citrefs for {len(list(query_citations_data))} entries"
        )
        return iter([])

    def populatePubIdsBatch(
        self, partial_mappings: "MutableSequence[IdMapping]"
    ) -> None:
        # title => title
        # fulljournalname => journal
        # sortpubdate => derived year
        # authors => authors
        # => pmid
        # => doi
        # => pmcid
        # => id (internal)

        # No work should be performed here
        self.logger.warning(
            f"This method was called to populate {len(partial_mappings)} partial mappings"
        )

    @abstractmethod
    def mirror_upstream(
        self,
        upstream_cache_dir: "pathlib.Path",
        upstream_cache_tracker: "Mapping[str, Tuple[bytes, int]]",
    ) -> "Sequence[Tuple[pathlib.Path, Tuple[bytes, int]]]":
        pass

    def digest_upstream_dir_entries(
        self, dir_entries: "Sequence[Tuple[pathlib.Path, Tuple[bytes, int]]]"
    ) -> "None":
        # Now, let's process this
        for entry, fingerprint in dir_entries:
            for mappings_batch_or_delete_list in self.digest_upstream_file(entry):
                if isinstance(mappings_batch_or_delete_list, dict):
                    self._commit_batch(mappings_batch_or_delete_list)
                elif isinstance(mappings_batch_or_delete_list, list):
                    self.pubC.removeCachedMappings(
                        mappings_batch_or_delete_list,
                        delete_stale_cache=self.DefaultDeleteStaleCache(),
                    )
                    self.pubC.clearCitRefs(
                        (
                            (
                                (p_elem["source"], p_elem["id"]),
                                False,
                            )
                            for p_elem in mappings_batch_or_delete_list
                        )
                    )
                    self.pubC.clearCitRefs(
                        (
                            (
                                (p_elem["source"], p_elem["id"]),
                                True,
                            )
                            for p_elem in mappings_batch_or_delete_list
                        )
                    )
                else:
                    self.logger.error("FIXME: an anomaly!!!!")

            # When it was properly processed is when the fingerprint is preserved
            with self._upstream_cache_tracker.transact():
                self._upstream_cache_tracker[entry.name] = fingerprint

        self.pubC.populate_citations_from_refs(
            self.Name(),
            self.DefaultSource(),
            timestamp=pub_common.Timestamps.BiggestTimestamp(),
        )

    @abstractmethod
    def digest_upstream_file(
        self,
        path: "pathlib.Path",
    ) -> "Iterator[Union[MutableMapping[UnqualifiedId, Tuple[IdMapping, Sequence[Reference]]], Sequence[IdMappingMinimal]]]":
        pass

    def _commit_batch(
        self,
        mappings_batch: "Mapping[UnqualifiedId, Tuple[IdMapping, Sequence[Reference]]]",
    ) -> "None":
        """Note: the batch must have unique values"""
        self.pubC.setCachedMappings(
            [mapping for mapping, references in mappings_batch.values()],
            mapping_timestamp=pub_common.Timestamps.BiggestTimestamp(),
            delete_stale_cache=self.DefaultDeleteStaleCache(),
        )

        the_source_id = self.DefaultSource()
        # This artificial separation is needed to avoid having the whole
        # list of cited manuscripts in memory
        self.pubC.clearCitRefs(
            (
                (
                    (the_source_id, pmid),
                    False,
                )
                for pmid in mappings_batch.keys()
            )
        )
        # This artificial separation is needed to avoid having the whole
        # list of cited manuscripts in memory
        self.pubC.setCitRefs_ll(
            (
                (
                    (the_source_id, mapping["id"]),
                    references,
                    False,
                )
                for mapping, references in mappings_batch.values()
            ),
            timestamp=pub_common.Timestamps.BiggestTimestamp(),
        )
