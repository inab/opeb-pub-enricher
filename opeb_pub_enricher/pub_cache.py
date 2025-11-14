#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import inspect
import json
import logging
import os
import sqlite3
import zlib

from typing import (
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from types import (
        TracebackType,
    )
    from typing import (
        Any,
        Final,
        Iterable,
        Iterator,
        Literal,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        Type,
        TypeAlias,
        TypedDict,
        Union,
    )

    from .pub_common import (
        DOIId,
        EnricherId,
        MetaQualifiedId,
        PubmedId,
        PMCId,
        PublishId,
        QualifiedId,
        SourceId,
        UnqualifiedId,
    )

    # Alias types declaration
    # Citation = NewType('Citation',Dict[str,Any])
    CitationCount: TypeAlias = int
    # Reference = NewType('Reference',Dict[str,Any])
    ReferenceCount: TypeAlias = int

    class IdMappingMinimal(TypedDict):
        source: SourceId
        id: UnqualifiedId

    class CitRefBase(IdMappingMinimal):
        enricher: EnricherId

    class CitRefBaseHad(CitRefBase, total=False):
        had: bool

    class CitRefMinimal(IdMappingMinimal):
        base_pubs: MutableSequence[CitRefBaseHad]

    class Citation(CitRefMinimal):
        year: int

    class Reference(CitRefMinimal):
        year: int

    class CountPerYear(TypedDict):
        year: int
        count: int

    class PartialIdMappingMinimal(TypedDict, total=False):
        source: SourceId
        id: UnqualifiedId
        base_pubs: MutableSequence[CitRefBaseHad]

    class IdMapping(IdMappingMinimal, total=False):
        pmid: Optional[PubmedId]
        pmcid: Optional[PMCId]
        doi: Optional[DOIId]
        base_pubs: MutableSequence[CitRefBaseHad]
        year: Optional[int]
        title: Optional[str]
        journal: Optional[str]
        authors: Sequence[Optional[str]]
        enricher: str

    class CitationsCount(IdMapping):
        citation_count: CitationCount

    class GatheredCitations(CitationsCount):
        citations: Sequence[Citation]

    class GatheredCitationStats(CitationsCount):
        citation_stats: Optional[Sequence[CountPerYear]]

    class TransientCitationStats(CitationsCount, total=False):
        citations: Sequence[Citation]
        citation_stats: Optional[Sequence[CountPerYear]]

    class TransientCitationRefs(CitationsCount, total=False):
        citations: Sequence[Citation]
        citation_refs: Sequence[CitRefMinimal]

    class ReferencesCount(IdMapping):
        reference_count: ReferenceCount

    class GatheredReferences(ReferencesCount):
        references: Sequence[Reference]

    class GatheredReferenceStats(ReferencesCount):
        reference_stats: Optional[Sequence[CountPerYear]]

    class TransientReferenceStats(ReferencesCount, total=False):
        references: Sequence[Reference]
        reference_stats: Optional[Sequence[CountPerYear]]

    class TransientReferenceRefs(ReferencesCount, total=False):
        references: Sequence[Reference]
        reference_refs: Sequence[CitRefMinimal]

    class GatheredCitRefs(GatheredCitations, GatheredReferences):  # type: ignore[misc]
        pass

    class GatheredCitRefStats(GatheredCitationStats, GatheredReferenceStats):  # type: ignore[misc]
        pass

    class TransientCitRefStats(TransientCitationStats, TransientReferenceStats):  # type: ignore[misc]
        pass

    class TransientCitRefRefs(TransientCitationRefs, TransientReferenceRefs):  # type: ignore[misc]
        pass

    class QueryId(TypedDict, total=False):
        pmid: PubmedId
        pmcid: PMCId
        doi: DOIId

    class CitRefMapping(CitRefBase):
        is_cit: bool
        payload: Optional[bytes]  # zlib compressed list of citations or references
        last_fetched: datetime.datetime

    class AbstractEnricherQuery(TypedDict):
        enricher: EnricherId

    class RawSourceEnricherQuery(AbstractEnricherQuery, total=False):
        pub_id: PublishId

    class RemoveSourceEnricherQuery(AbstractEnricherQuery, total=False):
        source: SourceId
        id: UnqualifiedId
        pub_id: PublishId

    class LowerEnricherQuery(AbstractEnricherQuery, total=False):
        lower_enricher: EnricherId
        lower_source: SourceId
        lower_id: UnqualifiedId


from . import pub_common
from .pub_common import Timestamps
from .doi_cache import DOIChecker


CACHE_DAYS: "Final[int]" = 28


class PubDBCache(object):
    """
    The publications cache management code
    Currently, it stores the correspondence among PMIDs,
    PMC ids, DOIs and the internal identifier in the
    original source.
    Also, it stores the title, etc..
    Also, it stores the citations fetched from the original source
    """

    DEFAULT_CACHE_DB_FILE: "Final[str]" = "pubEnricher_CACHE.db"

    OLDEST_CACHE: "Final[datetime.timedelta]" = datetime.timedelta(days=CACHE_DAYS)

    def __init__(
        self,
        enricher_name: "EnricherId",
        cache_dir: "str" = ".",
        prefix: "Optional[str]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ):
        # Getting a logger focused on specific classes
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        # The enricher name, used as default for all the queries
        self.enricher_name = enricher_name
        self.cache_dir = cache_dir

        if doi_checker is None:
            doi_checker = DOIChecker(cache_dir)

        self.doi_checker = doi_checker

        # self.debug_cache_dir = os.path.join(cache_dir,'debug')
        # os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
        # self._debug_count = 0

        # Should we set a prefix for the shelves?
        if prefix is None:
            cache_db_file = self.DEFAULT_CACHE_DB_FILE
        else:
            cache_db_file = prefix + self.DEFAULT_CACHE_DB_FILE

        self.cache_db_file = os.path.join(cache_dir, cache_db_file)
        self.jd = json.JSONDecoder()
        self.je = json.JSONEncoder()

    def __enter__(self) -> "PubDBCache":
        existsCache = os.path.exists(self.cache_db_file) and (
            os.path.getsize(self.cache_db_file) > 0
        )
        initializeCache = not existsCache

        # Opening / creating the database, with normal locking
        # and date parsing
        self.conn = sqlite3.connect(
            self.cache_db_file,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
        )
        self.conn.execute("""PRAGMA locking_mode = NORMAL""")
        self.conn.execute("""PRAGMA journal_mode = WAL""")

        # Database structures
        with self.conn:
            cur = self.conn.cursor()
            if initializeCache:
                # Publication table
                cur.execute("""
CREATE TABLE pub (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	payload BLOB NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	PRIMARY KEY (enricher,id,source)
)
""")
                # IDMap
                # pub_id_type VARCHAR(32) NOT NULL,
                # PRIMARY KEY (pub_id,pub_id_type),
                cur.execute("""
CREATE TABLE idmap (
	pub_id VARCHAR(4096) NOT NULL,
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	PRIMARY KEY (pub_id,id,enricher,source),
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source)
)
""")
                # Denormalized citations and references
                # so we can register empty answers,
                # and get the whole list with a single query
                cur.execute("""
CREATE TABLE citref (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	is_cit BOOLEAN NOT NULL,
	payload BLOB,
	last_fetched TIMESTAMP NOT NULL,
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source)
)
""")
                # Index on the enricher, id and source
                cur.execute("""
CREATE INDEX citref_e_i_s ON citref(enricher,id,source)
""")
                # Lower Mappings
                cur.execute("""
CREATE TABLE lower_map (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	lower_enricher VARCHAR(32) NOT NULL,
	lower_id VARCHAR(4096) NOT NULL,
	lower_source VARCHAR(32) NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source),
	FOREIGN KEY (lower_enricher,lower_id,lower_source) REFERENCES pub(enricher,id,source)
)
""")
                # Index on the lower mapping
                cur.execute("""
CREATE INDEX lower_map_e_i_s ON lower_map(lower_enricher,lower_id,lower_source)
""")
            cur.close()

        return self

    def __exit__(  # type: ignore[return]
        self,
        exc_type: "Optional[Type[BaseException]]",
        exc_val: "Optional[BaseException]",
        exc_tb: "Optional[TracebackType]",
    ) -> "Union[Literal[True], Any, None]":
        self.conn.close()

    def sync(self) -> "None":
        # This method has become a no-op
        pass

    def getCitRefs(
        self, qual_list: "Iterable[QualifiedId]", is_cit: "bool"
    ) -> "Union[Iterator[Optional[Sequence[Citation]]], Iterator[Optional[Sequence[Reference]]]]":
        with self.conn:
            cur = self.conn.cursor()
            for source_id, _id in qual_list:
                # The DATETIME expression helps invalidating stale results
                cur.execute(
                    """
SELECT payload
FROM citref
WHERE
DATETIME('NOW','-{} DAYS') <= last_fetched
AND
enricher = :enricher
AND
id = :id
AND
source = :source
AND
is_cit = :is_cit
""".format(CACHE_DAYS),
                    {
                        "enricher": self.enricher_name,
                        "id": _id,
                        "source": source_id,
                        "is_cit": is_cit,
                    },
                )
                res = cur.fetchone()
                if res:
                    yield (
                        self.jd.decode(zlib.decompress(res[0]).decode("utf-8"))
                        if res[0] is not None
                        else []
                    )
                else:
                    yield None

    def setCitRefs(
        self,
        citref_list: "Iterable[Union[Tuple[QualifiedId,Sequence[Citation], Literal[True]], Tuple[QualifiedId, Sequence[Reference], Literal[False]]]]",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        with self.conn:
            cur = self.conn.cursor()
            for qual_id, citrefs, is_cit in citref_list:
                params: "CitRefMapping" = {
                    "enricher": self.enricher_name,
                    "source": qual_id[0],
                    "id": qual_id[1],
                    "is_cit": is_cit,
                    "payload": zlib.compress(
                        self.je.encode(citrefs).encode("utf-8"), zlib.Z_BEST_COMPRESSION
                    )
                    if citrefs is not None
                    else None,
                    "last_fetched": timestamp,
                }

                # First, remove
                cur.execute(
                    """
DELETE FROM citref
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND is_cit = :is_cit
""",
                    params,
                )

                # Then, insert
                cur.execute(
                    """
INSERT INTO citref(enricher,id,source,is_cit,payload,last_fetched) VALUES(:enricher,:id,:source,:is_cit,:payload,:last_fetched)
""",
                    params,
                )

    def getCitationsAndCount(
        self, qualified_id: "QualifiedId"
    ) -> "Union[Tuple[Sequence[Citation],CitationCount], Tuple[None, None]]":
        for citations in self.getCitRefs([qualified_id], True):
            if citations is not None:
                return citations, len(citations)
            else:
                break

        return None, None

    def setCitationsAndCount(
        self,
        qualified_id: "QualifiedId",
        citations: "Sequence[Citation]",
        citation_count: "CitationCount",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self.setCitRefs([(qualified_id, citations, True)], timestamp)

    def getReferencesAndCount(
        self, qualified_id: "QualifiedId"
    ) -> "Union[Tuple[Sequence[Reference],ReferenceCount], Tuple[None, None]]":
        for references in self.getCitRefs([qualified_id], False):
            if references is not None:
                return references, len(references)
            else:
                break

        return None, None

    def setReferencesAndCount(
        self,
        qualified_id: "QualifiedId",
        references: "Sequence[Reference]",
        reference_count: "ReferenceCount",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self.setCitRefs([(qualified_id, references, False)], timestamp)

    def getRawCachedMappings_TL(
        self, qual_list: "Iterable[QualifiedId]"
    ) -> "Iterator[Union[Tuple[datetime.datetime, Optional[IdMapping]], Tuple[None, None]]]":
        """
        This method does not invalidate the cache
        """

        cur = self.conn.cursor()
        for source_id, _id in qual_list:
            cur.execute(
                """
SELECT last_fetched, payload
FROM pub
WHERE
enricher = :enricher
AND
id = :id
AND
source = :source
""",
                {"enricher": self.enricher_name, "id": _id, "source": source_id},
            )
            res = cur.fetchone()
            if res:
                yield (
                    Timestamps.UTCTimestamp(res[0]),
                    self.jd.decode(zlib.decompress(res[1]).decode("utf-8")),
                )
            else:
                yield None, None

    def getRawCachedMappings(
        self, qual_list: "Iterable[QualifiedId]"
    ) -> "Iterator[Union[Tuple[datetime.datetime, Optional[IdMapping]], Tuple[None, None]]]":
        """
        This method does not invalidate the cache
        """

        with self.conn:
            yield from self.getRawCachedMappings_TL(qual_list)

    def getRawCachedMapping_TL(
        self, qualified_id: "QualifiedId"
    ) -> "Union[Tuple[datetime.datetime, Optional[IdMapping]], Tuple[None, None]]":
        for raw_get in self.getRawCachedMappings_TL([qualified_id]):
            return raw_get
        else:
            return None, None

    def getRawCachedMapping(
        self, qualified_id: "QualifiedId"
    ) -> "Union[Tuple[datetime.datetime, Optional[IdMapping]], Tuple[None, None]]":
        for raw_get in self.getRawCachedMappings([qualified_id]):
            return raw_get
        else:
            return None, None

    def getCachedMapping(self, qualified_id: "QualifiedId") -> "Optional[IdMapping]":
        mapping_timestamp, mapping = self.getRawCachedMapping(qualified_id)

        # Invalidate cache
        if (
            mapping_timestamp is not None
            and (Timestamps.UTCTimestamp() - mapping_timestamp) > self.OLDEST_CACHE
        ):
            mapping = None

        return mapping

    def getRawSourceIds_TL(
        self, publish_id_iter: "Iterable[PublishId]"
    ) -> "Iterator[Sequence[Tuple[datetime.datetime,QualifiedId]]]":
        """
        This method does not invalidate the cache
        """
        cur = self.conn.cursor()
        params: "RawSourceEnricherQuery" = {"enricher": self.enricher_name}
        for publish_id in publish_id_iter:
            params["pub_id"] = publish_id
            retval = []
            for res in cur.execute(
                """
SELECT last_fetched, source, id
FROM idmap
WHERE
enricher = :enricher
AND
pub_id = :pub_id
""",
                params,
            ):
                retval.append((Timestamps.UTCTimestamp(res[0]), (res[1], res[2])))
            yield retval

    def getRawSourceIds(
        self, publish_id: "PublishId"
    ) -> "Sequence[Tuple[datetime.datetime,QualifiedId]]":
        """
        This method does not invalidate the cache
        """
        with self.conn:
            for listRes in self.getRawSourceIds_TL([publish_id]):
                return listRes
            else:
                return []

    def getSourceIds(self, publish_id: "PublishId") -> "Sequence[QualifiedId]":
        internal_ids = []

        # Invalidate cache
        for timestamp_internal_id, internal_id in self.getRawSourceIds(publish_id):
            if (
                timestamp_internal_id is not None
                and (Timestamps.UTCTimestamp() - timestamp_internal_id)
                <= self.OLDEST_CACHE
            ):
                internal_ids.append(internal_id)

        return internal_ids

    def appendSourceIds_TL(
        self,
        publish_id_iter: "Iterable[PublishId]",
        source_id: "SourceId",
        _id: "UnqualifiedId",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> "None":
        cur = self.conn.cursor()

        params = {
            "enricher": self.enricher_name,
            "id": _id,
            "source": source_id,
            "last_fetched": timestamp,
        }

        # In case of stale cache, remove all
        cur.execute(
            """
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
            params,
        )

        # Now, try storing specifically these
        for publish_id in publish_id_iter:
            params["pub_id"] = publish_id

            cur.execute(
                """
INSERT INTO idmap(pub_id,enricher,id,source,last_fetched) VALUES(:pub_id,:enricher,:id,:source,:last_fetched)
""",
                params,
            )

    def removeSourceIds_TL(
        self,
        publish_id_iter: "Iterable[PublishId]",
        source_id: "SourceId",
        _id: "UnqualifiedId",
    ) -> "None":
        cur = self.conn.cursor()

        params: "RemoveSourceEnricherQuery" = {
            "enricher": self.enricher_name,
            "id": _id,
            "source": source_id,
        }

        # In case of stale cache, remove all
        cur.execute(
            """
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
            params,
        )

        # Now, try removing specifically these
        for publish_id in publish_id_iter:
            params["pub_id"] = publish_id

            cur.execute(
                """
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND pub_id = :pub_id
""",
                params,
            )

    def getRawCachedMappingsFromPartial(
        self, partial_mapping: "PartialIdMappingMinimal"
    ) -> "Sequence[IdMapping]":
        """
        This method returns one or more cached mappings, based on the partial
        mapping provided. First attempt is using the id, then it tries through
        base_pubs information. Last, it derives	on the pmid, pmcid and doi to
        rescue, if available.

        This method does not invalidate caches
        """
        mappings: "MutableSequence[IdMapping]" = []
        mapping_ids: "MutableSequence[QualifiedId]" = []
        with self.conn:
            partial_mapping_id = partial_mapping.get("id")
            partial_mapping_source = partial_mapping.get("source")
            if partial_mapping_id is not None and partial_mapping_source is not None:
                _, mapping = self.getRawCachedMapping_TL(
                    (partial_mapping_source, partial_mapping_id)
                )
                if mapping:
                    mappings.append(mapping)

            # Now, trying with the identifiers of the mapped publications (if it is the case)
            if not mappings:
                base_pubs = partial_mapping.get("base_pubs", [])
                if base_pubs:
                    for base_pub in base_pubs:
                        base_pub_enricher = base_pub.get("enricher")
                        base_pub_source = base_pub.get("source")
                        base_pub_id = base_pub.get("id")
                        if (
                            base_pub_enricher is None
                            or base_pub_source is None
                            or base_pub_id is None
                        ):
                            continue
                        for internal_ids in self.getRawMetaSourceIds_TL(
                            [(base_pub_enricher, base_pub_source, base_pub_id)]
                        ):
                            if internal_ids:
                                mapping_ids.extend(
                                    map(
                                        lambda internal_id: internal_id[1], internal_ids
                                    )
                                )

            # Last resort
            if not mappings and (
                partial_mapping.get("pmid")
                or partial_mapping.get("pmcid")
                or partial_mapping.get("doi")
            ):
                for field_name in ("pmid", "pmcid", "doi"):
                    _theId = partial_mapping.get(field_name)
                    if _theId:
                        for internal_ids_res in self.getRawSourceIds_TL(
                            [cast("PublishId", _theId)]
                        ):
                            # Only return when internal_ids is a
                            if internal_ids_res:
                                mapping_ids.extend(
                                    map(
                                        lambda internal_id_res_elem: internal_id_res_elem[
                                            1
                                        ],
                                        internal_ids_res,
                                    )
                                )

            if mapping_ids:
                # Trying to avoid duplicates
                mapping_ids_unique = set(mapping_ids)
                for maybe_id_mapping in map(
                    lambda _iId: self.getRawCachedMapping_TL(_iId)[1],
                    mapping_ids_unique,
                ):
                    if maybe_id_mapping is not None:
                        mappings.append(maybe_id_mapping)

        return mappings

    def getRawMetaSourceIds_TL(
        self, lower_iter: "Iterable[MetaQualifiedId]"
    ) -> "Iterator[Sequence[Tuple[datetime.datetime,QualifiedId]]]":
        """
        This method does not invalidate caches
        """
        cur = self.conn.cursor()
        params: "LowerEnricherQuery" = {"enricher": self.enricher_name}
        for lower_enricher, lower_source, lower_id in lower_iter:
            params["lower_enricher"] = lower_enricher
            params["lower_source"] = lower_source
            params["lower_id"] = lower_id
            retval = []
            for res in cur.execute(
                """
SELECT last_fetched, source, id
FROM lower_map
WHERE
enricher = :enricher
AND
lower_enricher = :lower_enricher
AND
lower_source = :lower_source
AND
lower_id = :lower_id
""",
                params,
            ):
                retval.append((Timestamps.UTCTimestamp(res[0]), (res[1], res[2])))
            yield retval

    def getRawMetaSourceIds(
        self, lower: "MetaQualifiedId"
    ) -> "Sequence[Tuple[datetime.datetime,QualifiedId]]":
        """
        This method does not invalidate caches
        """
        with self.conn:
            for retval in self.getRawMetaSourceIds_TL([lower]):
                return retval
            else:
                return []

    def getMetaSourceIds(self, lower: "MetaQualifiedId") -> "Sequence[QualifiedId]":
        meta_ids = []
        for timestamp_meta_id, meta_id in self.getRawMetaSourceIds(lower):
            # Invalidate cache
            if (
                timestamp_meta_id is not None
                and (Timestamps.UTCTimestamp() - timestamp_meta_id) <= self.OLDEST_CACHE
            ):
                meta_ids.append(meta_id)

        return meta_ids

    def appendMetaSourceIds_TL(
        self,
        lower_iter: "Iterable[MetaQualifiedId]",
        source_id: "SourceId",
        _id: "UnqualifiedId",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> "None":
        cur = self.conn.cursor()

        params = {
            "enricher": self.enricher_name,
            "id": _id,
            "source": source_id,
            "last_fetched": timestamp,
        }

        # In case of stale cache, remove all
        cur.execute(
            """
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
            params,
        )

        # Now, try storing specifically these
        for lower_enricher, lower_source, lower_id in lower_iter:
            params["lower_enricher"] = lower_enricher
            params["lower_source"] = lower_source
            params["lower_id"] = lower_id

            cur.execute(
                """
INSERT INTO lower_map(enricher,id,source,lower_enricher,lower_id,lower_source,last_fetched) VALUES(:enricher,:id,:source,:lower_enricher,:lower_id,:lower_source,:last_fetched)
""",
                params,
            )

    def removeMetaSourceIds_TL(
        self,
        lower_iter: "Iterable[MetaQualifiedId]",
        source_id: "SourceId",
        _id: "UnqualifiedId",
    ) -> None:
        cur = self.conn.cursor()

        params = {"enricher": self.enricher_name, "id": _id, "source": source_id}

        # In case of stale cache, remove all
        cur.execute(
            """
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
            params,
        )

        # Now, try removing specifically these
        for lower_enricher, lower_source, lower_id in lower_iter:
            params["lower_enricher"] = lower_enricher
            params["lower_source"] = lower_source
            params["lower_id"] = lower_id

            cur.execute(
                """
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND lower_enricher = :lower_enricher
AND lower_id = :lower_id
AND lower_source = :lower_source
""",
                params,
            )

    def setCachedMappings(
        self,
        mapping_iter: "Iterable[IdMapping]",
        mapping_timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> "None":
        for mapping in mapping_iter:
            # Before anything, get the previous mapping before updating it
            _id = mapping["id"]
            source_id = mapping["source"]

            with self.conn:
                old_mapping_timestamp, old_mapping = self.getRawCachedMapping_TL(
                    (source_id, _id)
                )

                cur = self.conn.cursor()
                params = {
                    "enricher": mapping.get("enricher", self.enricher_name),
                    "source": mapping["source"],
                    "id": mapping["id"],
                    "payload": zlib.compress(
                        self.je.encode(mapping).encode("utf-8"), zlib.Z_BEST_COMPRESSION
                    ),
                    "last_fetched": mapping_timestamp,
                }

                # First, remove all the data from the previous mappings
                cur.execute(
                    """
DELETE FROM pub
WHERE enricher = :enricher
AND id = :id
AND source = :source
""",
                    params,
                )

                # Then, insert
                cur.execute(
                    """
INSERT INTO pub(enricher,id,source,payload,last_fetched) VALUES(:enricher,:id,:source,:payload,:last_fetched)
""",
                    params,
                )

                # Then, cleanup of sourceIds cache
                pubmed_id = mapping.get("pmid")
                pmc_id = mapping.get("pmcid")
                pmc_id_norm = pub_common.normalize_pmcid(pmc_id) if pmc_id else None
                doi_id = mapping.get("doi")
                doi_id_norm = self.doi_checker.normalize_doi(doi_id) if doi_id else None

                old_pubmed_id: "Optional[PubmedId]" = None
                old_doi_id: "Optional[DOIId]" = None
                old_pmc_id: "Optional[PMCId]" = None
                if old_mapping is not None:
                    old_pubmed_id = old_mapping.get("pmid")
                    old_doi_id = old_mapping.get("doi")
                    old_pmc_id = old_mapping.get("pmcid")
                old_doi_id_norm = (
                    self.doi_checker.normalize_doi(old_doi_id) if old_doi_id else None
                )
                old_pmc_id_norm = (
                    pub_common.normalize_pmcid(old_pmc_id) if old_pmc_id else None
                )

                removable_ids = []
                appendable_ids = []
                for old_id, new_id in [
                    (old_pubmed_id, pubmed_id),
                    (old_doi_id_norm, doi_id_norm),
                    (old_pmc_id_norm, pmc_id_norm),
                ]:
                    # Code needed for mismatches
                    if old_id is not None and old_id != new_id:
                        removable_ids.append(old_id)

                    if new_id is not None and old_id != new_id:
                        appendable_ids.append(new_id)

                if removable_ids:
                    self.removeSourceIds_TL(removable_ids, source_id, _id)
                if appendable_ids:
                    self.appendSourceIds_TL(
                        appendable_ids, source_id, _id, timestamp=mapping_timestamp
                    )

                # Let's manage also the lower mappings, from base_pubs

                # Creating the sets
                oldLowerSet = set()
                if old_mapping:
                    old_base_pubs = old_mapping.get("base_pubs", [])
                    for old_lower in old_base_pubs:
                        if old_lower.get("id"):
                            oldLowerSet.add(
                                (
                                    old_lower["enricher"],
                                    old_lower["source"],
                                    old_lower["id"],
                                )
                            )

                newLowerSet = set()
                new_base_pubs = mapping.get("base_pubs", [])
                for new_lower in new_base_pubs:
                    if new_lower.get("id"):
                        newLowerSet.add(
                            (
                                new_lower["enricher"],
                                new_lower["source"],
                                new_lower["id"],
                            )
                        )

                # This set has the entries to be removed
                toRemoveSet = oldLowerSet - newLowerSet
                self.removeMetaSourceIds_TL(toRemoveSet, source_id, _id)

                # This set has the entries to be added
                toAddSet = newLowerSet - oldLowerSet
                self.appendMetaSourceIds_TL(toAddSet, source_id, _id, mapping_timestamp)

    def setCachedMapping(
        self,
        mapping: "IdMapping",
        mapping_timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self.setCachedMappings([mapping], mapping_timestamp)
