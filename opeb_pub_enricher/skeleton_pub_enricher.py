#!/usr/bin/python

from abc import ABC, abstractmethod
import configparser
import copy
import datetime
import http
import inspect
import json
import logging
import os
import socket
import time

from urllib import request
from urllib.error import (
    HTTPError,
    URLError,
)

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from types import TracebackType
    from typing import (
        Any,
        Container,
        Iterable,
        Iterator,
        Literal,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Set,
        Tuple,
        Type,
        TypeAlias,
        Union,
    )

    from .opeb_queries import (
        OpebEntryPub,
        ParsedOpebEntry,
    )

    from .pub_cache import (
        Citation,
        CitationCount,
        CitRefMinimal,
        CountPerYear,
        GatheredCitations,
        GatheredCitRefs,
        GatheredCitRefStats,
        GatheredReferences,
        IdMapping,
        IdMappingMinimal,
        QueryId,
        Reference,
        ReferenceCount,
        TransientCitRefRefs,
        TransientCitRefStats,
    )
    from .pub_common import (
        BrokenWinner,
        CURIE,
        DOIId,
        EnricherId,
        PMCId,
        PublishId,
        PubmedId,
        QualifiedId,
        QualifiedWinner,
        SourceId,
        UnqualifiedId,
    )

    MutablePartialMapping: TypeAlias = MutableMapping[str, Any]
    PartialMapping: TypeAlias = Mapping[str, Any]


from . import pub_common
from .doi_cache import DOIChecker
from .pub_cache import (
    PubDBCache,
)

# import threading
# import gc
#
# GC_THRESHOLD = 180
# def periodic_gc():
# while True:
# time.sleep(GC_THRESHOLD)
# gc.collect()
# print('----GC----',file=sys.stderr)
#
# gc_thread = threading.Thread(target=periodic_gc, name='Pub-GC', daemon=True)
# gc_thread.start()

# def get_size(obj, seen=None):
#    """Recursively finds size of objects"""
#    size = sys.getsizeof(obj)
#    if seen is None:
#        seen = set()
#    obj_id = id(obj)
#    if obj_id in seen:
#        return 0
#    # Important mark as seen *before* entering recursion to gracefully handle
#    # self-referential objects
#    seen.add(obj_id)
#    if isinstance(obj, dict):
#        size += sum([get_size(v, seen) for v in obj.values()])
#        size += sum([get_size(k, seen) for k in obj.keys()])
#    elif hasattr(obj, '__dict__'):
#        size += get_size(obj.__dict__, seen)
#    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
#        size += sum([get_size(i, seen) for i in obj])
#    return size


class PubEnricherException(Exception):
    pass


class SkeletonPubEnricher(ABC):
    DEFAULT_STEP_SIZE = 50
    DEFAULT_NUM_FILES_PER_DIR = 1000
    DEFAULT_MAX_RETRIES = 5

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
        cache: "Union[PubDBCache, str]",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
        is_db_synchronous: "bool" = True,
    ):
        # Getting a logger focused on specific classes
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        # The section name is the symbolic name given to this class
        section_name = self.Name()

        if isinstance(cache, PubDBCache):
            # Try using same checker instance everywhere
            self.cache_dir = cache.cache_dir
            doi_checker = cache.doi_checker
        elif doi_checker is not None:
            self.cache_dir = doi_checker.cache_dir
        else:
            self.cache_dir = cache
            doi_checker = DOIChecker(self.cache_dir)

        self.doi_checker = doi_checker

        if isinstance(cache, str):
            cache_prefix = prefix + "_" + section_name if prefix else section_name
            cache_prefix += "_"

            self.pubC = PubDBCache(
                section_name,
                cache_dir=self.cache_dir,
                prefix=cache_prefix,
                doi_checker=doi_checker,
                is_db_synchronous=is_db_synchronous,
            )
        else:
            self.pubC = cache

        # Load at least a config parser
        self.config = config if config else configparser.ConfigParser()

        # Adding empty sections, in order to avoid the NoSectionError exception
        if not self.config.has_section(section_name):
            self.config.add_section(section_name)

        self.step_size = self.config.getint(
            section_name, "step_size", fallback=self.DEFAULT_STEP_SIZE
        )
        self.num_files_per_dir = self.config.getint(
            section_name, "num_files_per_dir", fallback=self.DEFAULT_NUM_FILES_PER_DIR
        )

        # Maximum number of retries
        self.max_retries = self.config.getint(
            section_name, "retries", fallback=self.DEFAULT_MAX_RETRIES
        )

        # self.debug_cache_dir = os.path.join(cache_dir,'debug')
        # os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
        # self._debug_count = 0

        # The json encoder and decoder instances
        self.je = json.JSONEncoder(indent=4, sort_keys=True)
        self.jd = json.JSONDecoder()

        super().__init__()

    def __enter__(self) -> "SkeletonPubEnricher":
        self.pubC.__enter__()
        return self

    def __exit__(
        self,
        exc_type: "Optional[Type[BaseException]]",
        exc_val: "Optional[BaseException]",
        exc_tb: "Optional[TracebackType]",
    ) -> "Union[Literal[True], Any, None]":
        return self.pubC.__exit__(exc_type, exc_val, exc_tb)

    @classmethod
    @abstractmethod
    def Name(cls) -> "EnricherId":
        return cast("EnricherId", "skel")

    @classmethod
    @abstractmethod
    def DefaultSource(cls) -> "SourceId":
        return cast("SourceId", "skel")

    @classmethod
    def DefaultDeleteStaleCache(cls) -> "bool":
        return True

    @abstractmethod
    def queryPubIdsBatch(self, query_ids: "Sequence[QueryId]") -> "Sequence[IdMapping]":
        pass

    def cachedQueryPubIds(
        self,
        query_list: "Sequence[QueryId]",
        delete_stale_cache: "bool" = True,
    ) -> "Sequence[IdMapping]":
        """
        Caching version of queryPubIdsBatch.
        Order is not guaranteed
        """
        # First, gather all the ids on one list, prepared for the query
        # MED: prefix has been removed because there are some problems
        # on the server side

        q2e: "Set[PublishId]" = set()
        r2e: "Set[QualifiedId]" = set()
        result_array = []

        def _prefetchCaches(publish_id: "PublishId") -> "bool":
            # This one tells us the result was already got
            if publish_id in q2e:
                return True

            # This one signals the result is not in the cache
            internal_ids = self.pubC.getSourceIds(publish_id)
            if internal_ids is None:
                return False

            # Let's dig in the cached results
            validMappings = False
            mappings = []
            results = []

            for source_id_pair in internal_ids:
                # If the result is correct, skip
                if source_id_pair in r2e:
                    validMappings = True
                    continue

                mapping = self.pubC.getCachedMapping(source_id_pair)

                # If some of the mappings has been invalidated,
                # complain
                if mapping is None:
                    return False

                # If none of the mapping elements expired, register all!
                validMappings = True
                mappings.append(mapping)
                results.append(source_id_pair)

            # Now all the cached elements have passed the filter
            # let's register all the values
            if validMappings:
                q2e.add(publish_id)
                if len(results) > 0:
                    r2e.union(*results)
                    result_array.extend(mappings)

            return validMappings

        # Preparing the query ids
        query_ids = []
        # This set allows avoiding to issue duplicate queries
        set_query_ids = set()
        for query in query_list:
            query_id: "QueryId" = {}

            # This loop avoid resolving twice
            pubmed_id = query.get("pmid")
            if pubmed_id is not None and not _prefetchCaches(pubmed_id):
                pubmed_set_id = (pubmed_id, "pmid")
                set_query_ids.add(pubmed_set_id)
                query_id["pmid"] = pubmed_id

            doi_id = query.get("doi")
            if doi_id is not None:
                doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                if not _prefetchCaches(doi_id_norm):
                    doi_set_id = (doi_id_norm, "doi")
                    set_query_ids.add(doi_set_id)
                    query_id["doi"] = doi_id_norm

            pmc_id = query.get("pmcid")
            if pmc_id is not None:
                pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                if not _prefetchCaches(pmc_id_norm):
                    pmc_set_id = (pmc_id_norm, "pmcid")
                    set_query_ids.add(pmc_set_id)
                    query_id["pmcid"] = pmc_id_norm

            # Add it when there is something to query about
            if len(query_id) > 0:
                query_ids.append(query_id)

        # Now, with the unknown ones, let's ask the server
        if len(query_ids) > 0:
            try:
                # Needed to not overwhelm the underlying implementation
                for start in range(0, len(query_ids), self.step_size):
                    stop = start + self.step_size
                    query_ids_slice = query_ids[start:stop]

                    gathered_pubmed_pairs = self.queryPubIdsBatch(query_ids_slice)

                    if gathered_pubmed_pairs:
                        for mapping in gathered_pubmed_pairs:
                            # Cache management
                            self.pubC.setCachedMapping(
                                mapping, delete_stale_cache=delete_stale_cache
                            )

                        # Result management
                        result_array.extend(gathered_pubmed_pairs)
            except Exception as anyEx:
                self.logger.exception(
                    "Something unexpected happened in cachedQueryPubIds"
                )
                raise anyEx

        return result_array

    def reconcilePubIdsBatch(
        self, entries: "Sequence[ParsedOpebEntry]", delete_stale_cache: "bool" = True
    ) -> None:
        # First, gather all the ids on one list, prepared for the query
        # MED: prefix has been removed because there are some problems
        # on the server side

        p2e: "MutableMapping[PubmedId, MutableMapping[SourceId,IdMapping]]" = {}
        pmc2e: "MutableMapping[PMCId, MutableMapping[SourceId,IdMapping]]" = {}
        d2e: "MutableMapping[DOIId, MutableMapping[SourceId,IdMapping]]" = {}
        pubmed_pairs: "MutableSequence[IdMapping]" = []

        def _updateCaches(publish_id: "PublishId") -> "bool":
            internal_ids = self.pubC.getSourceIds(publish_id)
            if internal_ids is not None:
                validMappings = 0
                for internal_id in internal_ids:
                    source_id, _id = internal_id
                    mapping = self.pubC.getCachedMapping(internal_id)
                    # If the mapping did not expire, register it!
                    if mapping is not None:
                        validMappings += 1
                        pubmed_pairs.append(mapping)

                        pubmed_id = mapping.get("pmid")
                        if pubmed_id is not None:
                            p2e.setdefault(pubmed_id, {})[source_id] = mapping

                        doi_id = mapping.get("doi")
                        if doi_id is not None:
                            doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                            d2e.setdefault(doi_id_norm, {})[source_id] = mapping

                        pmc_id = mapping.get("pmcid")
                        if pmc_id is not None:
                            pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                            pmc2e.setdefault(pmc_id_norm, {})[source_id] = mapping

                return validMappings > 0
            else:
                return False

        # Preparing the query ids
        query_ids: "MutableSequence[QueryId]" = []
        # This set allows avoiding to issue duplicate queries
        set_query_ids: "Set[Tuple[PublishId, str]]" = set()
        for entry_pubs in map(lambda entry: entry["entry_pubs"], entries):
            for entry_pub in entry_pubs:
                query_id: "QueryId" = {}
                # This loop avoid resolving twice
                pubmed_id = entry_pub.get("pmid")
                if pubmed_id is not None and pubmed_id not in p2e:
                    pubmed_set_id = (pubmed_id, "pmid")
                    if pubmed_set_id not in set_query_ids:
                        if not _updateCaches(pubmed_id):
                            set_query_ids.add(pubmed_set_id)
                            query_id["pmid"] = pubmed_id

                doi_id = entry_pub.get("doi")
                if doi_id is not None:
                    doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                    doi_set_id = (doi_id_norm, "doi")
                    if (
                        doi_set_id not in set_query_ids
                        and doi_id_norm not in d2e
                        and not _updateCaches(doi_id_norm)
                    ):
                        set_query_ids.add(doi_set_id)
                        query_id["doi"] = doi_id_norm

                pmc_id = entry_pub.get("pmcid")
                if pmc_id is not None:
                    pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                    pmc_set_id = (pmc_id_norm, "pmcid")
                    if (
                        pmc_set_id not in set_query_ids
                        and pmc_id_norm not in pmc2e
                        and not _updateCaches(pmc_id_norm)
                    ):
                        set_query_ids.add(pmc_set_id)
                        query_id["pmcid"] = pmc_id_norm

                # Add it when there is something to query about
                if len(query_id) > 0:
                    query_ids.append(query_id)

        # Now, with the unknown ones, let's ask the server
        if len(query_ids) > 0:
            try:
                gathered_pubmed_pairs = self.queryPubIdsBatch(query_ids)

                # Cache management
                for mapping in gathered_pubmed_pairs:
                    _id = mapping["id"]
                    source_id = mapping["source"]
                    self.pubC.setCachedMapping(
                        mapping, delete_stale_cache=delete_stale_cache
                    )

                    pubmed_id = mapping.get("pmid")
                    if pubmed_id is not None:
                        p2e.setdefault(pubmed_id, {})[source_id] = mapping

                    pmc_id = mapping.get("pmcid")
                    if pmc_id is not None:
                        pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                        pmc2e.setdefault(pmc_id_norm, {})[source_id] = mapping

                    doi_id = mapping.get("doi")
                    if doi_id is not None:
                        doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                        d2e.setdefault(doi_id_norm, {})[source_id] = mapping

                    pubmed_pairs.append(mapping)

                    # self.logger.debug(json.dumps(entries,indent=4))
                # sys.exit(1)
            except Exception as anyEx:
                self.logger.exception(
                    "Something unexpected happened in reconcilePubIdsBatch"
                )
                raise anyEx

        # Reconciliation and checking missing ones
        for entry in entries:
            for entry_pub in entry["entry_pubs"]:
                broken_curie_ids: "MutableSequence[CURIE]" = []
                initial_curie_ids: "MutableSequence[CURIE]" = []

                results = []
                pubmed_id = entry_pub.get("pmid")
                if pubmed_id is not None:
                    curie_id = pub_common.pmid2curie(pubmed_id)
                    initial_curie_ids.append(curie_id)
                    if pubmed_id in p2e:
                        results.append(p2e[pubmed_id])
                    else:
                        broken_curie_ids.append(curie_id)

                doi_id = entry_pub.get("doi")
                if doi_id is not None:
                    curie_id = DOIChecker.doi2curie(doi_id)
                    initial_curie_ids.append(curie_id)
                    doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                    if doi_id_norm in d2e:
                        results.append(d2e[doi_id_norm])
                    else:
                        broken_curie_ids.append(curie_id)

                pmc_id = entry_pub.get("pmcid")
                if pmc_id is not None:
                    curie_id = pub_common.pmcid2curie(pmc_id)
                    initial_curie_ids.append(curie_id)
                    pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                    if pmc_id_norm in pmc2e:
                        results.append(pmc2e[pmc_id_norm])
                    else:
                        broken_curie_ids.append(curie_id)

                # Checking all the entries at once
                winner_set = None
                notFound = len(results) == 0
                for result in results:
                    if winner_set is None:
                        winner_set = result
                    elif winner_set != result:
                        winner = None
                        break

                winners: "MutableSequence[Union[BrokenWinner, QualifiedWinner]]" = []
                if winner_set is not None:
                    for winner in iter(winner_set.values()):
                        # Duplicating in order to augment it
                        new_winner = cast("QualifiedWinner", copy.deepcopy(winner))

                        curie_ids = []

                        pubmed_id = new_winner.get("pmid")
                        if pubmed_id is not None:
                            curie_id = pub_common.pmid2curie(pubmed_id)
                            curie_ids.append(curie_id)

                        doi_id = new_winner.get("doi")
                        if doi_id is not None:
                            curie_id = DOIChecker.doi2curie(doi_id)
                            curie_ids.append(curie_id)

                        pmc_id = new_winner.get("pmcid")
                        if pmc_id is not None:
                            curie_id = pub_common.pmcid2curie(pmc_id)
                            curie_ids.append(curie_id)

                        new_winner["curie_ids"] = curie_ids
                        new_winner["broken_curie_ids"] = broken_curie_ids
                        winners.append(new_winner)
                else:
                    broken_winner: "BrokenWinner" = {
                        "id": None,
                        "source": None,
                        "curie_ids": initial_curie_ids,
                        "broken_curie_ids": broken_curie_ids,
                        "pmid": pubmed_id,
                        "doi": doi_id,
                        "pmcid": pmc_id,
                        "reason": "",
                    }
                    # No possible result
                    if notFound:
                        broken_winner["reason"] = (
                            "notFound" if len(initial_curie_ids) > 0 else "noReference"
                        )
                    # There were mismatches
                    else:
                        broken_winner["reason"] = "mismatch"

                    winners.append(broken_winner)

                cast(
                    "MutableSequence[Union[QualifiedWinner, BrokenWinner]]",
                    entry_pub["found_pubs"],
                ).extend(winners)

    @abstractmethod
    def queryCitRefsBatch(
        self,
        query_citations_data: "Iterable[IdMappingMinimal]",
        minimal: "bool" = False,
        mode: "int" = 3,
    ) -> "Iterator[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]":
        """
        query_citations_data: An iterator of dictionaries with at least two keys: source and id
        minimal: Whether the list of citations and references is "minimal" (minimizing the number of queries) or not
        mode: 1 means only references, 2 means only citations, and 3 means both

        The results from the returned iterator do not maintain the same order as the queries from the input one
        """
        pass

    def clusteredSearchCitRefsBatch(
        self,
        query_citations_data: "Sequence[IdMappingMinimal]",
        query_hash: "MutableMapping[Tuple[UnqualifiedId, SourceId], MutableSequence[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]]",
        minimal: "bool" = False,
        mode: "int" = 3,
    ) -> None:
        # Update the cache with the new data
        if len(query_citations_data) > 0:
            try:
                new_citrefs = self.queryCitRefsBatch(
                    query_citations_data, minimal, mode
                )
            except Exception as anyEx:
                self.logger.exception(
                    "ERROR: Something went wrong with queryCitRefsBatch"
                )
                raise anyEx

            for new_citref in cast("Sequence[GatheredCitRefs]", new_citrefs):
                source_id = new_citref["source"]
                _id = new_citref["id"]

                if (mode & 2) != 0:
                    if "citations" in new_citref:
                        citations = new_citref["citations"]
                        citation_count = new_citref["citation_count"]
                        # There are cases where no citation could be fetched
                        # but it should also be cached
                        self.pubC.setCitationsAndCount(
                            (source_id, _id), citations, citation_count
                        )
                        for pub_cit in cast(
                            "Sequence[GatheredCitations]", query_hash[(_id, source_id)]
                        ):
                            pub_cit["citation_count"] = citation_count
                            pub_cit["citations"] = citations

                if (mode & 1) != 0:
                    if "references" in new_citref:
                        references = new_citref["references"]
                        reference_count = new_citref["reference_count"]
                        # There are cases where no reference could be fetched
                        # but it should also be cached
                        self.pubC.setReferencesAndCount(
                            (source_id, _id), references, reference_count
                        )
                        for pub_ref in cast(
                            "Sequence[GatheredReferences]", query_hash[(_id, source_id)]
                        ):
                            pub_ref["reference_count"] = reference_count
                            pub_ref["references"] = references

    def listReconcileCitRefMetricsBatch(
        self,
        pub_list: "Union[Sequence[OpebEntryPub], Sequence[IdMapping], Sequence[Union[BrokenWinner, QualifiedWinner]]]",
        verbosityLevel: "float" = 0,
        mode: "int" = 3,
        delete_stale_cache: "bool" = True,
    ) -> "Union[Sequence[GatheredCitRefs], Sequence[GatheredCitRefStats]]":
        """
        This method takes in batches of found publications and it retrieves citations from ids
        hitCount: number of times cited
                for each citation it retrieves
                        id: id of the paper it was cited in
                        source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
                        pubYear: year of publication
                        journalAbbreviation: Journal Abbreviations
        """

        query_citations_data: "MutableSequence[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]" = []
        query_hash: "MutableMapping[Tuple[UnqualifiedId, SourceId], MutableSequence[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]]" = {}
        for pub_field_raw in pub_list:
            if "id" in pub_field_raw:
                pub_field = cast("GatheredCitRefs", pub_field_raw)
                _id = pub_field["id"]  # 11932250
                source_id = pub_field["source"]

                citation_count: "Optional[CitationCount]" = None
                reference_count: "Optional[ReferenceCount]" = None
                if (mode & 2) != 0:
                    cits_count = self.pubC.getCitationsAndCount((source_id, _id))
                    if cits_count[0] is not None:
                        citations, citation_count = cits_count
                        # Save now
                        pub_field["citation_count"] = citation_count
                        pub_field["citations"] = citations

                if (mode & 1) != 0:
                    refs_count = self.pubC.getReferencesAndCount((source_id, _id))
                    if refs_count[0] is not None:
                        references, reference_count = refs_count
                        # Save now
                        pub_field["reference_count"] = reference_count
                        pub_field["references"] = references

                # Query later, without repetitions
                if ((mode & 2) != 0 and (citation_count is None)) or (
                    (mode & 1) != 0 and (reference_count is None)
                ):
                    query_list = query_hash.setdefault((_id, source_id), [])
                    if len(query_list) == 0:
                        query_citations_data.append(pub_field)
                    query_list.append(pub_field)

        minimal = verbosityLevel == -1
        # Update the cache with the new data
        self.clusteredSearchCitRefsBatch(
            query_citations_data, query_hash, minimal, mode
        )

        # If we have to return the digested stats, compute them here
        if verbosityLevel > -1 and verbosityLevel <= 0:
            for pub_field_s in cast("Sequence[TransientCitRefStats]", pub_list):
                if (mode & 2) != 0:
                    maybe_citations_raw = pub_field_s.pop("citations", None)
                    maybe_citations: "Optional[Sequence[Citation]]" = None
                    if maybe_citations_raw is not None:
                        # Detect whether the citations have the year field
                        if len(maybe_citations_raw) > 0 and (
                            "year" not in maybe_citations_raw[0]
                        ):
                            maybe_citations = self.populatePubIds(
                                maybe_citations_raw,
                                onlyYear=True,
                                delete_stale_cache=delete_stale_cache,
                            )
                        else:
                            maybe_citations = maybe_citations_raw

                    # Computing the stats
                    pub_field_s["citation_stats"] = (
                        None
                        if maybe_citations is None
                        else self._citrefStats(maybe_citations)
                    )

                if (mode & 1) != 0:
                    maybe_references_raw = pub_field_s.pop("references", None)
                    maybe_references: "Optional[Sequence[Reference]]" = None
                    if maybe_references_raw is not None:
                        # Detect whether the references have the year field
                        if len(maybe_references_raw) > 0 and (
                            "year" not in maybe_references_raw[0]
                        ):
                            maybe_references = self.populatePubIds(
                                maybe_references_raw,
                                onlyYear=True,
                                delete_stale_cache=delete_stale_cache,
                            )
                        else:
                            maybe_references = maybe_references_raw

                    # Computing the stats
                    pub_field_s["reference_stats"] = (
                        None
                        if maybe_references is None
                        else self._citrefStats(maybe_references)
                    )
        elif verbosityLevel > 1:
            populables: "MutableSequence[Union[Citation, Reference]]" = []
            nextLevelPop: "MutableSequence[Citation]" = []
            for pub_field in cast("Sequence[GatheredCitRefs]", pub_list):
                if (mode & 2) != 0:
                    maybe_citations = pub_field.get("citations")
                    if maybe_citations is not None:
                        populables.extend(maybe_citations)
                        if verbosityLevel >= 2:
                            nextLevelPop.extend(maybe_citations)

                if (mode & 1) != 0:
                    maybe_references = pub_field.get("references")
                    if maybe_references is not None:
                        populables.extend(maybe_references)
            # for pub_field in pub_list:
            # if (mode & 2) != 0:
            # citations = pub_field.get('citations')
            # if citations is not None:
            # self.populatePubIds(citations)
            #
            # if (mode & 1) != 0:
            # references = pub_field.get('references')
            # if references is not None:
            # self.populatePubIds(references)

            if populables:
                self.populatePubIds(cast("Sequence[MutablePartialMapping]", populables))

            if nextLevelPop:
                self.listReconcileCitRefMetricsBatch(
                    cast("Sequence[IdMapping]", nextLevelPop),
                    verbosityLevel=verbosityLevel - 1,
                    mode=mode,
                    delete_stale_cache=delete_stale_cache,
                )

        # This is needed for multiprocess approaches
        return cast(
            "Union[Sequence[GatheredCitRefs], Sequence[GatheredCitRefStats]]", pub_list
        )

    def listReconcileRefMetricsBatch(
        self, pub_list: "Sequence[IdMapping]", verbosityLevel: "float" = 0
    ) -> "Sequence[GatheredReferences]":
        """
        This method takes in batches of found publications and retrieves citations from ids
        hitCount: number of times cited
                for each citation it retives
                        id: id of the paper it was cited in
                        source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
                        pubYear: year of publication
                        journalAbbreviation: Journal Abbriviations
        """
        return cast(
            "Sequence[GatheredReferences]",
            self.listReconcileCitRefMetricsBatch(pub_list, verbosityLevel, 1),
        )

    def listReconcileCitMetricsBatch(
        self, pub_list: "Sequence[IdMapping]", verbosityLevel: "float" = 0
    ) -> "Sequence[GatheredCitations]":
        """
        This method takes in batches of found publications and retrieves citations from ids
        hitCount: number of times cited
                for each citation it retives
                        id: id of the paper it was cited in
                        source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
                        pubYear: year of publication
                        journalAbbreviation: Journal Abbriviations
        """
        return cast(
            "Sequence[GatheredCitations]",
            self.listReconcileCitRefMetricsBatch(pub_list, verbosityLevel, 2),
        )

    # This method does the different reads and retries
    # in case of partial contents
    def retriable_full_http_read(
        self,
        theRequest: "request.Request",
        timeout: "int" = 300,
        debug_url: "Optional[str]" = None,
    ) -> "bytes":
        if debug_url is None:
            debug_url = theRequest.full_url
            self.logger.debug(f"Using {debug_url} as default debug URL")
        self.logger.debug(f"Fetching {debug_url}")
        retries = 0

        retryexc: "Optional[BaseException]" = None
        retrymsg: "Optional[str]" = None
        while retries <= self.max_retries:
            additional_sleep = 0
            try:
                # The original bytes
                response: "bytes" = b""
                with request.urlopen(theRequest, timeout=timeout) as req:
                    while True:
                        try:
                            # Try getting it
                            responsePart = req.read()
                        except http.client.IncompleteRead as icread:
                            # Getting at least the partial content
                            response += icread.partial
                            continue
                        else:
                            # In this case, saving all
                            response += responsePart
                        break

                return response
            except HTTPError as e:
                retryexc = e
                if e.code >= 500:
                    retrymsg = "code {}".format(e.code)
                    if e.code in (500, 502, 503, 504):
                        # Giving the service additional time to recover could work
                        additional_sleep = 60
            except URLError as e:
                retryexc = e
                retryreason = str(e.reason)
                if "handshake operation timed out" in retryreason:
                    retrymsg = "handshake timeout"
                elif "is unreachable" in retryreason:
                    retrymsg = "network is unreachable"
                elif "failure in name resolution" in retryreason:
                    retrymsg = "failure in name resolution"

            except http.client.RemoteDisconnected as e:
                retrymsg = "remote disconnect"
                retryexc = e
            except socket.timeout as e:
                retrymsg = "socket timeout"
                retryexc = e
            except BaseException as be:
                retryexc = be

            retries += 1
            if (retrymsg is not None) and (retries <= self.max_retries):
                self.logger.debug(
                    f"Retry {retries} fetching {debug_url}, due {retrymsg}"
                )

                # Using a backoff time of 2 seconds when some recoverable error happens
                time.sleep(additional_sleep + 2**retries)

                retryexc = None
                retrymsg = None
            else:
                break

        if retryexc is None:
            retryexc = PubEnricherException("Untraced ERROR")

        self.logger.error(f"URL with ERROR: {debug_url} . Reason: {retryexc}")

        raise retryexc

    def _citrefStats(
        self, citrefs: "Iterable[Union[Citation, Reference]]"
    ) -> "Sequence[CountPerYear]":
        # Computing the stats
        citref_stats: "MutableMapping[int, int]" = {}
        for citref in citrefs:
            year = citref.get("year", -1)
            if year is None:
                year = -1
            if year in citref_stats:
                citref_stats[year] += 1
            else:
                citref_stats[year] = 1

        return [
            {"year": year, "count": citref_stats[year]}
            for year in sorted(citref_stats.keys())
        ]

    def flattenPubs(
        self, opeb_entries: "Sequence[ParsedOpebEntry]"
    ) -> "Sequence[OpebEntryPub]":
        """
        This method takes in batches of entries and retrives citations from ids
        hitCount: number of times cited
                for each citation it retrieves
                        id: id of the paper it was cited in
                        source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
                        pubYear: year of publication
                        journalAbbreviation: Journal Abbriviations
        """

        linear_pubs: "MutableSequence[OpebEntryPub]" = []
        for entry_pubs in map(
            lambda opeb_entry: opeb_entry["entry_pubs"], opeb_entries
        ):
            for entry_pub in entry_pubs:
                linear_pubs.extend(
                    cast("Sequence[OpebEntryPub]", entry_pub["found_pubs"])
                )

        return linear_pubs

    def reconcileCitRefMetricsBatch(
        self, opeb_entries: "Sequence[ParsedOpebEntry]", verbosityLevel: "float" = 0
    ) -> None:
        """
        This method takes in batches of entries and retrives citations from ids
        hitCount: number of times cited
                for each citation it retives
                        id: id of the paper it was cited in
                        source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
                        pubYear: year of publication
                        journalAbbreviation: Journal Abbriviations
        """

        linear_pubs = self.flattenPubs(opeb_entries)

        self.listReconcileCitRefMetricsBatch(linear_pubs, verbosityLevel)

    def _getUniqueNewPubs(
        self,
        query_pubs: "Sequence[CitRefMinimal]",
        query_refs: "Sequence[CitRefMinimal]",
        saved_pubs: "Container[str]",
        saved_comb: "Container[str]",
    ) -> "Union[Tuple[Sequence[CitRefMinimal], Sequence[CitRefMinimal]], Tuple[None, None]]":
        # The list of new citations to populate later
        new_pubs: "Sequence[CitRefMinimal]" = []
        if len(query_pubs) > 0:
            new_pubs = list(
                filter(
                    lambda pub: (pub.get("source") is not None)
                    and (
                        (pub.get("source", "") + ":" + pub.get("id", ""))
                        not in saved_pubs
                    ),
                    query_pubs,
                )
            )

        unique_pubs = {}
        for new_pub in new_pubs:
            new_key = new_pub.get("source", "") + ":" + new_pub.get("id", "")
            if new_key not in unique_pubs:
                unique_pubs[new_key] = new_pub

        # import pprint
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(query_pubs)

        new_ref_pubs: "Sequence[CitRefMinimal]" = []
        if len(query_refs) > 0:
            # pp.pprint(query_refs)
            new_ref_pubs = list(
                filter(
                    lambda pub: (pub.get("source") is not None)
                    and (
                        (pub.get("source", "") + ":" + pub.get("id", ""))
                        not in saved_comb
                    ),
                    query_refs,
                )
            )

        unique_ref_pubs = {}
        for new_ref_pub in new_ref_pubs:
            new_ref_key = (
                new_ref_pub.get("source", "") + ":" + new_ref_pub.get("id", "")
            )
            if (new_ref_key not in unique_pubs) and (
                new_ref_key not in unique_ref_pubs
            ):
                unique_ref_pubs[new_ref_key] = new_ref_pub

        if len(unique_pubs) == 0 and len(unique_ref_pubs) == 0:
            return None, None

        # The list to obtain the basic publication data
        # and the list of new citations to dig in later (as soft as possible)

        return list(unique_ref_pubs.values()), list(unique_pubs.values())

    @classmethod
    def populateMapping(
        cls,
        base_mapping: "PartialMapping",
        dest_mapping: "MutablePartialMapping",
        onlyYear: "bool" = False,
    ) -> None:
        if onlyYear:
            dest_mapping["year"] = base_mapping.get("year")
        else:
            dest_mapping.update(base_mapping)

    @abstractmethod
    def populatePubIdsBatch(
        self, partial_mappings: "MutableSequence[IdMapping]"
    ) -> None:
        pass

    def populatePubIds(
        self,
        partial_mappings: "Sequence[MutablePartialMapping]",
        onlyYear: "bool" = False,
        delete_stale_cache: "bool" = True,
    ) -> "Sequence[IdMapping]":
        populable_mappings = []

        for partial_mapping in partial_mappings:
            # We are interested only in the year facet
            # as it is a kind of indicator
            pubYear = partial_mapping.get("year")
            _id = partial_mapping.get("id")
            # There can be corrupted or incomplete entries
            # in the source
            source_id = partial_mapping.get("source")
            if (
                _id is not None
                and source_id is not None
                and (pubYear is None or not onlyYear)
            ):
                mapping = self.pubC.getCachedMapping((source_id, _id))

                # Not found or expired mapping?
                if mapping is None:
                    populable_mappings.append(partial_mapping)
                else:
                    self.populateMapping(mapping, partial_mapping, onlyYear)

        if len(populable_mappings) > 0:
            for start in range(0, len(populable_mappings), self.step_size):
                stop = start + self.step_size
                populable_mappings_slice = populable_mappings[start:stop]
                populable_mappings_clone_slice: "MutableSequence[IdMapping]" = list(
                    map(
                        lambda p_m: {"id": p_m["id"], "source": p_m["source"]},
                        populable_mappings_slice,
                    )
                )
                self.populatePubIdsBatch(populable_mappings_clone_slice)

                for p_m, p_m_c in zip(
                    populable_mappings_slice, populable_mappings_clone_slice
                ):
                    # It is a kind of indicator the 'year' flag
                    if p_m_c.get("year") is not None:
                        self.pubC.setCachedMapping(
                            p_m_c, delete_stale_cache=delete_stale_cache
                        )
                        self.populateMapping(p_m_c, p_m, onlyYear)

        return cast("Sequence[IdMapping]", partial_mappings)

    KEEP_REFS_KEYS = ("source", "id", "base_pubs")

    def _tidyCitRefRefs(
        self, citrefs: "Union[Sequence[Citation], Sequence[Reference], None]"
    ) -> "Sequence[CitRefMinimal]":
        retval: "MutableSequence[CitRefMinimal]" = []
        if citrefs is not None:
            for citref in citrefs:
                ret: "CitRefMinimal" = {
                    "id": cast("UnqualifiedId", ""),
                    "source": cast("SourceId", ""),
                    "base_pubs": [],
                }
                # Next block is highly inefficient so
                # mypy can properly approve it (sigh)
                counter = 2
                for key in self.KEEP_REFS_KEYS:
                    if key in citref:
                        if key == "source":
                            ret["source"] = citref["source"]
                            counter -= 1
                        elif key == "id":
                            ret["id"] = citref["id"]
                            counter -= 1
                        elif key == "base_pubs":
                            ret["base_pubs"] = citref["base_pubs"]

                if counter == 0:
                    retval.append(ret)

        return retval

    def reconcilePubIdsFlatFormat(
        self,
        entries: "Sequence[ParsedOpebEntry]",
        results_path: "str",
        verbosityLevel: "float" = 0,
    ) -> "None":
        # This unlinks the input from the output
        copied_entries = cast(
            "MutableSequence[ParsedOpebEntry]", copy.deepcopy(entries)
        )

        # The tools subdirectory
        tools_subpath = "tools"
        os.makedirs(
            os.path.abspath(os.path.join(results_path, tools_subpath)), exist_ok=True
        )

        saved_tools = []
        # Now, gather the tool publication entries
        filename_prefix = "pub_tool_"
        for start in range(0, len(copied_entries), self.step_size):
            stop = start + self.step_size
            entries_slice = copied_entries[start:stop]
            self.reconcilePubIdsBatch(entries_slice)

            copied_entries_slice = copy.deepcopy(entries_slice)
            for idx, entry in enumerate(copied_entries_slice):
                part_dest_file = os.path.join(
                    tools_subpath, filename_prefix + str(start + idx) + ".json"
                )
                dest_file = os.path.join(results_path, part_dest_file)
                saved_tools.append({"@id": entry["@id"], "file": part_dest_file})
                with open(dest_file, mode="w", encoding="utf-8") as outentry:
                    outentry.write(self.je.encode(entry))
            del copied_entries_slice

        # Recording what we have already fetched (and saved)
        saved_pubs = {}
        saved_comb: "MutableMapping[str, str]" = {}
        saved_comb_arr = []

        # The counter for the files being generated
        pub_counter = 0
        pubs_subpath = "pubs"
        query_refs: "MutableSequence[CitRefMinimal]" = []
        query_pubs = cast(
            "MutableSequence[CitRefMinimal]", self.flattenPubs(copied_entries)
        )

        depth = 0

        def _save_population_slice(
            population_slice: "Sequence[IdMapping]", not_last: "bool" = True
        ) -> "None":
            nonlocal pub_counter
            nonlocal pubs_subpath
            nonlocal saved_pubs
            nonlocal saved_comb
            nonlocal saved_comb_arr
            nonlocal query_pubs
            nonlocal query_refs

            for new_pub in cast("Sequence[TransientCitRefRefs]", population_slice):
                # Getting the name of the file
                new_key = new_pub.get("source", "") + ":" + new_pub.get("id", "")

                assert new_key not in saved_pubs
                if new_key in saved_comb:
                    new_pub_file = saved_comb[new_key]
                else:
                    if pub_counter % self.num_files_per_dir == 0:
                        pubs_subpath = "pubs_" + str(pub_counter)
                        os.makedirs(
                            os.path.abspath(os.path.join(results_path, pubs_subpath)),
                            exist_ok=True,
                        )
                    part_new_pub_file = os.path.join(
                        pubs_subpath, "pub_" + str(pub_counter) + ".json"
                    )
                    saved_comb_arr.append({"_id": new_key, "file": part_new_pub_file})
                    new_pub_file = os.path.join(results_path, part_new_pub_file)
                    saved_comb[new_key] = new_pub_file
                    pub_counter += 1

                reconciled = False
                if "references" in new_pub:
                    reconciled = True
                    # Fixing the output
                    tidied_refs = self._tidyCitRefRefs(new_pub.pop("references"))
                    new_pub["reference_refs"] = tidied_refs
                    if not_last:
                        query_refs.extend(tidied_refs)
                if not_last and ("citations" in new_pub):
                    reconciled = True
                    # Fixing the output
                    tidied_refs = self._tidyCitRefRefs(new_pub.pop("citations"))
                    new_pub["citation_refs"] = tidied_refs
                    query_pubs.extend(tidied_refs)

                with open(new_pub_file, mode="w", encoding="utf-8") as outentry:
                    outentry.write(self.je.encode(new_pub))

                if not_last and reconciled:
                    saved_pubs[new_key] = new_pub_file

        while (len(query_pubs) + len(query_refs)) > 0:
            unique_res = self._getUniqueNewPubs(
                query_pubs, query_refs, saved_pubs, saved_comb
            )

            query_pubs.clear()
            query_refs.clear()
            if unique_res[0] is None:
                break

            unique_to_ref_populate = cast(
                "MutableSequence[CitRefMinimal]", unique_res[0]
            )
            unique_to_reconcile = unique_res[1]

            not_last = depth < verbosityLevel
            if not_last:
                self.logger.debug(
                    f"Level {depth} Pop {len(unique_to_ref_populate) + len(unique_to_reconcile)} Rec {len(unique_to_reconcile)}"
                )

                # The ones to get both citations and references
                for start in range(0, len(unique_to_reconcile), self.step_size):
                    stop = start + self.step_size
                    # This unlinks the input from the output
                    unique_to_reconcile_slice = cast(
                        "Sequence[MutablePartialMapping]",
                        copy.deepcopy(unique_to_reconcile[start:stop]),
                    )

                    # Obtaining the publication data
                    idmapped_slice = self.populatePubIds(unique_to_reconcile_slice)

                    # The list of new citations AND references to dig in later (as soft as possible)
                    self.listReconcileCitRefMetricsBatch(idmapped_slice, -1)

                    # Saving (it works because all the elements in unique_to_reconcile are in unique_to_populate)
                    # and getting the next batch from those with references and/or citations
                    _save_population_slice(idmapped_slice)
                    del unique_to_reconcile_slice
                    del idmapped_slice
            else:
                unique_to_ref_populate.extend(unique_to_reconcile)
                self.logger.debug(f"Last Pop {len(unique_to_ref_populate)}")

            # The ones to get only references
            for start in range(0, len(unique_to_ref_populate), self.step_size):
                stop = start + self.step_size
                # This unlinks the input from the output
                unique_to_ref_populate_slice = cast(
                    "Sequence[MutablePartialMapping]",
                    copy.deepcopy(unique_to_ref_populate[start:stop]),
                )

                # Obtaining the publication data
                idmapped_slice = self.populatePubIds(unique_to_ref_populate_slice)

                # The list of ONLY references to dig in later (as soft as possible)
                self.listReconcileRefMetricsBatch(idmapped_slice, -1)

                # Saving (it works because all the elements in unique_to_reconcile are in unique_to_populate)
                # and getting the next batch from those with references and/or citations
                _save_population_slice(idmapped_slice, not_last)
                del unique_to_ref_populate_slice
                del idmapped_slice

            if not_last:
                depth += 1
            else:
                break

        self.logger.debug(f"Saved {pub_counter} publications")

        # self.logger.debug("Residuals {} {} {} {} {}".format(get_size(saved_pubs),get_size(saved_comb),get_size(saved_comb_arr),get_size(query_refs),get_size(query_pubs)))

        # Last, save the manifest file
        manifest_file = os.path.join(results_path, "manifest.json")
        with open(manifest_file, mode="w", encoding="utf-8") as manifile:
            manifile.write(
                self.je.encode(
                    {
                        "@timestamp": datetime.datetime.now().isoformat(),
                        "tools": saved_tools,
                        "publications": saved_comb_arr,
                    }
                )
            )

    # # Add to leaky code within python_script_being_profiled.py
    # from pympler import muppy, summary
    # all_objects = muppy.get_objects()
    # sum1 = summary.summarize(all_objects)
    #
    # # Prints out a summary of the large objects
    # summary.print_(sum1)
    #
    # ## Get references to certain types of objects such as dataframe
    # #dataframes = [ao for ao in all_objects if isinstance(ao, pd.DataFrame)]
    # #
    # #for d in dataframes:
    # #  print d.columns.values
    # #  print len(d)

    def reconcilePubIds(
        self,
        entries: "Sequence[ParsedOpebEntry]",
        results_path: "str",
        results_format: "str",
        verbosityLevel: "float" = 0,
    ) -> "None":
        """
        This method reconciles, for each entry, the pubmed ids
        and the DOIs it has. As it manipulates the entries, adding
        the reconciliation to 'found_pubs' key, it returns the same
        parameter as input
        """

        # As flat format is so different from the previous ones, use a separate codepath
        if results_format == "flat":
            self.reconcilePubIdsFlatFormat(entries, results_path, verbosityLevel)
        else:
            # print(len(fetchedEntries))
            # print(json.dumps(fetchedEntries,indent=4))
            if results_format == "single":
                jsonOutput = open(results_path, mode="w", encoding="utf-8")
                print("[", file=jsonOutput)
                printComma = False
            else:
                jsonOutput = None

            saved_results = []
            for start in range(0, len(entries), self.step_size):
                stop = start + self.step_size
                # This unlinks the input from the output
                entries_slice = copy.deepcopy(entries[start:stop])
                self.reconcilePubIdsBatch(entries_slice)
                self.reconcileCitRefMetricsBatch(entries_slice, verbosityLevel)
                self.pubC.sync()
                if jsonOutput is not None:
                    for entry in entries_slice:
                        if printComma:
                            print(",", file=jsonOutput)
                        else:
                            printComma = True
                        jsonOutput.write(self.je.encode(entry))
                elif results_format == "multiple":
                    filename_prefix = "entry_" if verbosityLevel == 0 else "fullentry_"
                    for idx, entry in enumerate(entries_slice):
                        rel_dest_file = filename_prefix + str(start + idx) + ".json"
                        saved_results.append(
                            {
                                "@id": entry["@id"],
                                "file": rel_dest_file,
                            }
                        )
                        dest_file = os.path.join(results_path, rel_dest_file)
                        with open(dest_file, mode="w", encoding="utf-8") as outentry:
                            outentry.write(self.je.encode(entry))

            if jsonOutput is not None:
                print("]", file=jsonOutput)
                jsonOutput.close()
            else:
                # Last, save the manifest file
                manifest_file = os.path.join(results_path, "manifest.json")
                with open(manifest_file, mode="w", encoding="utf-8") as manifile:
                    manifile.write(
                        self.je.encode(
                            {
                                "@timestamp": datetime.datetime.now().isoformat(),
                                "results": saved_results,
                            }
                        )
                    )
