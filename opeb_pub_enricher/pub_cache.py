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

try:
    from itertools import batched as itertools_batched
except ImportError:
    # python 3.11 and older do not have `batched`
    # Next code is borrowed from
    # https://docs.python.org/3.13/library/itertools.html#itertools.batched
    import itertools

    def itertools_batched(  # type: ignore[no-redef]
        iterable: "Iterable[Any]",
        n: "int",
        *,
        strict: "bool" = False,
    ) -> "Iterator[Any]":
        # batched('ABCDEFG', 3) → ABC DEF G
        if n < 1:
            raise ValueError("n must be at least one")
        iterator = iter(iterable)
        # walrus operator is available since python 3.8
        while batch := tuple(itertools.islice(iterator, n)):
            if strict and len(batch) != n:
                raise ValueError("batched(): incomplete batch")
            yield batch


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

    REFERENCES_TABLE: "Final[str]" = "pub_references"
    CITATIONS_TABLE: "Final[str]" = "pub_citations"

    def __init__(
        self,
        enricher_name: "EnricherId",
        cache_dir: "str" = ".",
        prefix: "Optional[str]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
        is_db_synchronous: "bool" = True,
    ):
        # Getting a logger focused on specific classes
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        # The enricher name, used as default for all the queries
        if os.path.isabs(cache_dir):
            abs_cache_dir = cache_dir
        else:
            abs_cache_dir = os.path.abspath(cache_dir)

        self.enricher_name = enricher_name
        self.cache_dir = abs_cache_dir
        self.is_db_synchronous = is_db_synchronous

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
        self.conn.execute(
            f"""PRAGMA synchronous = {"NORMAL" if self.is_db_synchronous else "OFF"}"""
        )
        self.conn.execute(f"""PRAGMA journal_size_limit = {64 * 1024 * 1024}""")
        self.conn.execute("""PRAGMA temp_store = 1""")
        self.conn.execute(f"""PRAGMA temp_store_directory = '{self.cache_dir}'""")

        # Database structures
        with self.conn:
            cur = self.conn.cursor()
            if initializeCache:
                # Publication table
                cur.execute("""\
CREATE TABLE pub (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	payload BLOB NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	PRIMARY KEY (enricher,source,id)
)
""")
                #                # Index on the last_fetched
                #                cur.execute("""\
                # CREATE INDEX pub_l_f ON pub(last_fetched)
                # """)
                # IDMap
                # pub_id_type VARCHAR(32) NOT NULL,
                # PRIMARY KEY (pub_id,pub_id_type),
                cur.execute("""\
CREATE TABLE idmap (
	pub_id VARCHAR(4096) NOT NULL,
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	PRIMARY KEY (pub_id,enricher)
)
""")

                #                # Index on the pub_id
                #                cur.execute("""\
                # CREATE INDEX idmap_pub_id ON idmap(pub_id)
                # """)
                #                # Index on the enricher
                #                cur.execute("""\
                # CREATE INDEX idmap_enricher ON idmap(enricher)
                # """)
                # Index on the id and the source
                cur.execute("""\
CREATE INDEX idmap_id_source ON idmap(id,source)
""")
                #                # Index on the last_fetched
                #                cur.execute("""\
                # CREATE INDEX idmap_l_f ON idmap(last_fetched)
                # """)
                # Denormalized citations and references
                # so we can register empty answers,
                # and get the whole list with a single query
                for tablename in (self.REFERENCES_TABLE, self.CITATIONS_TABLE):
                    cur.execute(f"""\
CREATE TABLE {tablename} (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	payload BLOB,
	last_fetched TIMESTAMP NOT NULL,
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source) ON DELETE CASCADE ON UPDATE CASCADE
)
""")
                    # Index on the enricher, id and source
                    cur.execute(f"""\
CREATE INDEX {tablename}_e_i_s ON {tablename}(enricher,id,source)
""")
                #                    # Index on the last_fetched
                #                    cur.execute(f"""\
                # CREATE INDEX {tablename}_l_f ON {tablename}(last_fetched)
                # """)
                # Lower Mappings
                cur.execute("""\
CREATE TABLE lower_map (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	lower_enricher VARCHAR(32) NOT NULL,
	lower_id VARCHAR(4096) NOT NULL,
	lower_source VARCHAR(32) NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (lower_enricher,lower_id,lower_source) REFERENCES pub(enricher,id,source) ON DELETE CASCADE ON UPDATE CASCADE
)
""")
                # Index on the lower mapping
                cur.execute("""\
CREATE INDEX lower_map_e_i_s ON lower_map(lower_enricher,lower_id,lower_source)
""")
            #                # Index on the last_fetched
            #                cur.execute("""\
            # CREATE INDEX lower_map_l_f ON lower_map(last_fetched)
            # """)

            # Provide a migration path
            # from citref to references and citations
            for _ in cur.execute(
                """SELECT name FROM sqlite_master WHERE type='table' AND name='citref'"""
            ):
                cur.execute(f"""\
INSERT INTO {self.REFERENCES_TABLE}(enricher, id, source, payload, last_fetched)
SELECT enricher, id, source, payload, last_fetched
FROM citref
WHERE NOT is_cit
                """)
                cur.execute(f"""\
INSERT INTO {self.CITATIONS_TABLE}(enricher, id, source, payload, last_fetched)
SELECT enricher, id, source, payload, last_fetched
FROM citref
WHERE is_cit
                """)
                cur.execute("""DROP TABLE citref""")
                break

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

    def getAllReferences(
        self,
        source_id: "SourceId",
        delete_stale_cache: "bool" = True,
    ) -> "Iterator[Optional[Tuple[UnqualifiedId, Sequence[Reference]]]]":
        return self._getAllCitRefs(
            source_id,
            table=self.REFERENCES_TABLE,
            delete_stale_cache=delete_stale_cache,
        )

    def getAllCitations(
        self,
        source_id: "SourceId",
        delete_stale_cache: "bool" = True,
    ) -> "Iterator[Optional[Tuple[UnqualifiedId, Sequence[Citation]]]]":
        return self._getAllCitRefs(
            source_id,
            table=self.CITATIONS_TABLE,
            delete_stale_cache=delete_stale_cache,
        )

    def _getAllCitRefs(
        self,
        source_id: "SourceId",
        table: "str",
        delete_stale_cache: "bool" = True,
    ) -> "Union[Iterator[Optional[Tuple[UnqualifiedId, Sequence[Citation]]]], Iterator[Optional[Tuple[UnqualifiedId, Sequence[Reference]]]]]":
        with self.conn:
            cur = self.conn.cursor()
            # The DATETIME expression helps invalidating stale results
            for res in cur.execute(
                """\
SELECT id, payload
FROM {}
WHERE
DATETIME('NOW','-{} DAYS') <= last_fetched
AND
enricher = :enricher
AND
source = :source
""".format(table, CACHE_DAYS)
                if delete_stale_cache
                else """\
SELECT id, payload
FROM {}
WHERE
enricher = :enricher
AND
source = :source
""".format(table),
                {
                    "enricher": self.enricher_name,
                    "source": source_id,
                },
            ):
                yield (
                    res[0],
                    self.jd.decode(zlib.decompress(res[1]).decode("utf-8"))
                    if res[1] is not None
                    else [],
                )
            else:
                yield None

    def _getCitRefs(
        self,
        qual_list: "Iterable[QualifiedId]",
        table: "str",
        delete_stale_cache: "bool" = True,
    ) -> "Union[Iterator[Optional[Sequence[Citation]]], Iterator[Optional[Sequence[Reference]]]]":
        with self.conn:
            cur = self.conn.cursor()
            for source_id, _id in qual_list:
                # The DATETIME expression helps invalidating stale results
                cur.execute(
                    """\
SELECT payload
FROM {}
WHERE
DATETIME('NOW','-{} DAYS') <= last_fetched
AND
enricher = :enricher
AND
id = :id
AND
source = :source
""".format(table, CACHE_DAYS)
                    if delete_stale_cache
                    else """\
SELECT payload
FROM {}
WHERE
enricher = :enricher
AND
id = :id
AND
source = :source
""".format(table),
                    {
                        "enricher": self.enricher_name,
                        "id": _id,
                        "source": source_id,
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

    def clearReferences(
        self,
        qualid_list: "Iterable[QualifiedId]",
    ) -> "None":
        with self.conn:
            cur = self.conn.cursor()

            # Remove
            cur.executemany(
                """\
DELETE FROM {}
WHERE enricher = :enricher
AND id = :id
AND source = :source
""".format(self.REFERENCES_TABLE),
                (
                    {
                        "enricher": self.enricher_name,
                        "source": qual_id[0],
                        "id": qual_id[1],
                    }
                    for qual_id in qualid_list
                ),
            )

    def clearCitations(
        self,
        qualid_list: "Iterable[QualifiedId]",
    ) -> "None":
        with self.conn:
            cur = self.conn.cursor()

            # Remove
            cur.executemany(
                """\
DELETE FROM {}
WHERE enricher = :enricher
AND id = :id
AND source = :source
""".format(self.CITATIONS_TABLE),
                (
                    {
                        "enricher": self.enricher_name,
                        "source": qual_id[0],
                        "id": qual_id[1],
                    }
                    for qual_id in qualid_list
                ),
            )

    def _setCitRefs_ll(
        self,
        citref_list: "Union[Iterable[Tuple[QualifiedId,Sequence[Citation]]], Iterable[Tuple[QualifiedId, Sequence[Reference]]]]",
        table: "str",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> "None":
        with self.conn:
            cur = self.conn.cursor()

            # Then, insert
            cur.executemany(
                """\
INSERT INTO {}(enricher,id,source,payload,last_fetched) VALUES(:enricher,:id,:source,:payload,:last_fetched)
""".format(table),
                (
                    {
                        "enricher": self.enricher_name,
                        "source": qual_id[0],
                        "id": qual_id[1],
                        "payload": zlib.compress(
                            self.je.encode(citrefs).encode("utf-8"),
                            zlib.Z_BEST_COMPRESSION,
                        )
                        if citrefs is not None
                        else None,
                        "last_fetched": timestamp,
                    }
                    for qual_id, citrefs in citref_list
                ),
            )

    def setReferences_ll(
        self,
        references_list: "Iterable[Tuple[QualifiedId, Sequence[Reference]]]",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> "None":
        self._setCitRefs_ll(
            references_list, table=self.REFERENCES_TABLE, timestamp=timestamp
        )

    def _setCitRefs(
        self,
        citref_list: "Union[Iterable[Tuple[QualifiedId,Sequence[Citation]]], Iterable[Tuple[QualifiedId, Sequence[Reference]]]]",
        table: "str",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        with self.conn:
            cur = self.conn.cursor()
            params_list = [
                {
                    "enricher": self.enricher_name,
                    "source": qual_id[0],
                    "id": qual_id[1],
                    "payload": zlib.compress(
                        self.je.encode(citrefs).encode("utf-8"), zlib.Z_BEST_COMPRESSION
                    )
                    if citrefs is not None
                    else None,
                    "last_fetched": timestamp,
                }
                for qual_id, citrefs in citref_list
            ]

            # First, remove
            cur.executemany(
                """\
DELETE FROM {}
WHERE enricher = :enricher
AND id = :id
AND source = :source
""".format(table),
                params_list,
            )

            # Then, insert
            cur.executemany(
                """\
INSERT INTO {}(enricher,id,source,payload,last_fetched) VALUES(:enricher,:id,:source,:payload,:last_fetched)
""".format(table),
                params_list,
            )

    def getCitationsAndCount(
        self,
        qualified_id: "QualifiedId",
        delete_stale_cache: "bool" = True,
    ) -> "Union[Tuple[Sequence[Citation],CitationCount], Tuple[None, None]]":
        for citations in self._getCitRefs(
            [qualified_id],
            table=self.CITATIONS_TABLE,
            delete_stale_cache=delete_stale_cache,
        ):
            if citations is not None:
                return citations, len(citations)
            else:
                break

        return None, None

    def setCitationsBatch(
        self,
        citations_batch: "Iterable[Tuple[QualifiedId, Sequence[Citation]]]",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self._setCitRefs(
            citations_batch,
            table=self.CITATIONS_TABLE,
            timestamp=timestamp,
        )

    def setCitationsAndCount(
        self,
        qualified_id: "QualifiedId",
        citations: "Sequence[Citation]",
        citation_count: "CitationCount",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self.setCitationsBatch([(qualified_id, citations)], timestamp=timestamp)

    def getReferencesAndCount(
        self,
        qualified_id: "QualifiedId",
        delete_stale_cache: "bool" = True,
    ) -> "Union[Tuple[Sequence[Reference],ReferenceCount], Tuple[None, None]]":
        for references in self._getCitRefs(
            [qualified_id],
            table=self.REFERENCES_TABLE,
            delete_stale_cache=delete_stale_cache,
        ):
            if references is not None:
                return references, len(references)
            else:
                break

        return None, None

    def setReferencesBatch(
        self,
        references_batch: "Iterable[Tuple[QualifiedId, Sequence[Reference]]]",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self._setCitRefs(
            references_batch,
            table=self.REFERENCES_TABLE,
            timestamp=timestamp,
        )

    def setReferencesAndCount(
        self,
        qualified_id: "QualifiedId",
        references: "Sequence[Reference]",
        reference_count: "ReferenceCount",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> None:
        self.setReferencesBatch([(qualified_id, references)], timestamp=timestamp)

    def getRawCachedMappings_TL(
        self, qual_list: "Iterable[QualifiedId]"
    ) -> "Iterator[Union[Tuple[datetime.datetime, Optional[IdMapping]], Tuple[None, None]]]":
        """
        This method does not invalidate the cache
        """

        cur = self.conn.cursor()
        for source_id, _id in qual_list:
            cur.execute(
                """\
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
                """\
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

    def appendSourceIds(
        self,
        append_source_ids_batch: "Sequence[Tuple[Sequence[PublishId], QualifiedId]]",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
        delete_stale_cache: "bool" = True,
    ) -> "None":
        with self.conn:
            self.appendSourceIds_TL(
                append_source_ids_batch,
                timestamp=timestamp,
                delete_stale_cache=delete_stale_cache,
            )

    def appendSourceIds_TL(
        self,
        append_source_ids_batch: "Sequence[Tuple[Sequence[PublishId], QualifiedId]]",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
        delete_stale_cache: "bool" = True,
    ) -> "None":
        cur = self.conn.cursor()

        # Now, try storing specifically these
        cur.executemany(
            """\
INSERT INTO idmap(pub_id,enricher,id,source,last_fetched)
VALUES(:pub_id,:enricher,:id,:source,:last_fetched)
ON CONFLICT DO
UPDATE SET
id=excluded.id,
source=excluded.source,
last_fetched=excluded.last_fetched
""",
            (
                {
                    "enricher": self.enricher_name,
                    "last_fetched": timestamp,
                    "id": qualified_id[1],
                    "source": qualified_id[0],
                    "pub_id": publish_id,
                }
                for publish_id_iter, qualified_id in append_source_ids_batch
                for publish_id in publish_id_iter
            ),
        )

        if delete_stale_cache:
            # In case of stale cache, remove all
            cur.executemany(
                """\
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
                (
                    {
                        "enricher": self.enricher_name,
                        "id": qualified_id[1],
                        "source": qualified_id[0],
                    }
                    for publish_id_iter, qualified_id in append_source_ids_batch
                ),
            )

    def removeSourceIds_TL(
        self,
        remove_source_ids_batch: "Sequence[Tuple[Sequence[PublishId], QualifiedId]]",
        delete_stale_cache: "bool" = True,
    ) -> "None":
        cur = self.conn.cursor()

        # In case of stale cache, remove all
        if delete_stale_cache:
            cur.executemany(
                """\
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
                (
                    {
                        "enricher": self.enricher_name,
                        "id": qualified_id[1],
                        "source": qualified_id[0],
                    }
                    for publish_id_iter, qualified_id in remove_source_ids_batch
                ),
            )

        # Now, try removing specifically these
        cur.executemany(
            """\
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND pub_id = :pub_id
""",
            (
                {
                    "enricher": self.enricher_name,
                    "id": qualified_id[1],
                    "source": qualified_id[0],
                    "pub_id": publish_id,
                }
                for publish_id_iter, qualified_id in remove_source_ids_batch
                for publish_id in publish_id_iter
            ),
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
                """\
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
        delete_stale_cache: "bool" = True,
    ) -> "None":
        cur = self.conn.cursor()

        params = {
            "enricher": self.enricher_name,
            "id": _id,
            "source": source_id,
            "last_fetched": timestamp,
        }

        # In case of stale cache, remove all
        if delete_stale_cache:
            cur.execute(
                """\
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
                params,
            )

        # Now, try storing specifically these
        cur.executemany(
            """\
INSERT INTO lower_map(enricher,id,source,lower_enricher,lower_id,lower_source,last_fetched) VALUES(:enricher,:id,:source,:lower_enricher,:lower_id,:lower_source,:last_fetched)
""",
            (
                {
                    "lower_enricher": lower_enricher,
                    "lower_source": lower_source,
                    "lower_id": lower_id,
                    **params,
                }
                for lower_enricher, lower_source, lower_id in lower_iter
            ),
        )

    def removeMetaSourceIds_TL(
        self,
        lower_iter: "Iterable[MetaQualifiedId]",
        source_id: "SourceId",
        _id: "UnqualifiedId",
        delete_stale_cache: "bool" = True,
    ) -> None:
        cur = self.conn.cursor()

        params = {"enricher": self.enricher_name, "id": _id, "source": source_id}

        # In case of stale cache, remove all
        if delete_stale_cache:
            cur.execute(
                """\
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),
                params,
            )

        # Now, try removing specifically these
        cur.executemany(
            """\
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND lower_enricher = :lower_enricher
AND lower_id = :lower_id
AND lower_source = :lower_source
""",
            (
                {
                    "lower_enricher": lower_enricher,
                    "lower_source": lower_source,
                    "lower_id": lower_id,
                    **params,
                }
                for lower_enricher, lower_source, lower_id in lower_iter
            ),
        )

    def setCachedMappings(
        self,
        mappings: "Sequence[IdMapping]",
        mapping_timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
        delete_stale_cache: "bool" = True,
    ) -> "None":
        """
        WARNING: a UNIQUE constraint is fired if more than one mapping
        has the very same identifiers
        """
        if self.logger.getEffectiveLevel() <= logging.DEBUG:
            seen = set()
            dupes = set()

            for x in (mapping["id"] for mapping in mappings):
                if x in seen:
                    dupes.add(x)
                else:
                    seen.add(x)

            if len(seen) > 0 and len(dupes) > 0:
                self.logger.error(f"Dups: {dupes}")

        with self.conn:
            # Before anything, get the previous mappings before updating them
            old_mappings = [
                old_mapping
                for old_mapping_timestamp, old_mapping in self.getRawCachedMappings_TL(
                    ((mapping["source"], mapping["id"]) for mapping in mappings)
                )
            ]
            # First, remove all the data from the previous mappings
            cur = self.conn.cursor()
            cur.executemany(
                """\
DELETE FROM pub
WHERE enricher = :enricher
AND id = :id
AND source = :source
""",
                (
                    {
                        "enricher": mapping.get("enricher", self.enricher_name),
                        "source": mapping["source"],
                        "id": mapping["id"],
                    }
                    for mapping in mappings
                ),
            )
            # Then, insert them
            cur.executemany(
                """\
INSERT INTO pub(enricher,id,source,payload,last_fetched) VALUES(:enricher,:id,:source,:payload,:last_fetched)
""",
                (
                    {
                        "enricher": mapping.get("enricher", self.enricher_name),
                        "source": mapping["source"],
                        "id": mapping["id"],
                        "payload": zlib.compress(
                            self.je.encode(mapping).encode("utf-8"),
                            zlib.Z_BEST_COMPRESSION,
                        ),
                        "last_fetched": mapping_timestamp,
                    }
                    for mapping in mappings
                ),
            )

            remove_source_ids_batch = []
            append_source_ids_batch = []
            for old_mapping, mapping in zip(old_mappings, mappings):
                # Before anything, get the previous mapping before updating it
                _id = mapping["id"]
                source_id = mapping["source"]
                qualified_id = (source_id, _id)

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
                    remove_source_ids_batch.append((removable_ids, qualified_id))
                if appendable_ids:
                    append_source_ids_batch.append((appendable_ids, qualified_id))

            if len(remove_source_ids_batch) > 0:
                self.removeSourceIds_TL(
                    remove_source_ids_batch, delete_stale_cache=delete_stale_cache
                )

            if len(append_source_ids_batch) > 0:
                self.appendSourceIds_TL(
                    append_source_ids_batch,
                    timestamp=mapping_timestamp,
                    delete_stale_cache=delete_stale_cache,
                )

            # Let's manage also the lower mappings, from base_pubs
            for old_mapping, mapping in zip(old_mappings, mappings):
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
                if len(toRemoveSet) > 0:
                    self.removeMetaSourceIds_TL(
                        toRemoveSet,
                        source_id,
                        _id,
                        delete_stale_cache=delete_stale_cache,
                    )
                # This set has the entries to be added
                toAddSet = newLowerSet - oldLowerSet
                if len(toAddSet) > 0:
                    self.appendMetaSourceIds_TL(
                        toAddSet,
                        source_id,
                        _id,
                        timestamp=mapping_timestamp,
                        delete_stale_cache=delete_stale_cache,
                    )

    def setCachedMapping(
        self,
        mapping: "IdMapping",
        mapping_timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
        delete_stale_cache: "bool" = True,
    ) -> None:
        self.setCachedMappings(
            [mapping],
            mapping_timestamp=mapping_timestamp,
            delete_stale_cache=delete_stale_cache,
        )

    def removeCachedMappings(
        self,
        mappings: "Sequence[IdMapping]",
        delete_stale_cache: "bool" = True,
    ) -> "None":
        with self.conn:
            # Before anything, get the previous mappings before updating them
            old_mappings = [
                old_mapping
                for old_mapping_timestamp, old_mapping in self.getRawCachedMappings_TL(
                    ((mapping["source"], mapping["id"]) for mapping in mappings)
                )
            ]
            # First, remove all the data from the previous mappings
            cur = self.conn.cursor()
            cur.executemany(
                """\
DELETE FROM pub
WHERE enricher = :enricher
AND id = :id
AND source = :source
""",
                (
                    {
                        "enricher": mapping.get("enricher", self.enricher_name),
                        "source": mapping["source"],
                        "id": mapping["id"],
                    }
                    for mapping in mappings
                ),
            )

            remove_source_ids_batch = []
            for old_mapping, mapping in zip(old_mappings, mappings):
                # Before anything, get the previous mapping before updating it
                _id = mapping["id"]
                source_id = mapping["source"]
                qualified_id = (source_id, _id)

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
                for old_id in [
                    old_pubmed_id,
                    old_doi_id_norm,
                    old_pmc_id_norm,
                ]:
                    # Code needed for mismatches
                    if old_id is not None:
                        removable_ids.append(old_id)

                if removable_ids:
                    remove_source_ids_batch.append((removable_ids, qualified_id))

            if len(remove_source_ids_batch) > 0:
                self.removeSourceIds_TL(
                    remove_source_ids_batch, delete_stale_cache=delete_stale_cache
                )

            # Let's manage also the lower mappings, from base_pubs
            for old_mapping in old_mappings:
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

                # This set has the entries to be removed
                if len(oldLowerSet) > 0:
                    self.removeMetaSourceIds_TL(
                        oldLowerSet,
                        source_id,
                        _id,
                        delete_stale_cache=delete_stale_cache,
                    )

    def populate_citations_from_refs(
        self,
        enricher_id: "EnricherId",
        source_id: "SourceId",
        timestamp: "datetime.datetime" = Timestamps.UTCTimestamp(),
    ) -> "None":
        # This method creates its own temporary database
        temp_db_file = self.cache_db_file + "_TEMP"
        temp_conn = sqlite3.connect(
            temp_db_file,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
        )
        temp_conn.execute("""PRAGMA locking_mode = EXCLUSIVE""")
        temp_conn.execute("""PRAGMA journal_mode = OFF""")
        temp_conn.execute("""PRAGMA synchronous = OFF""")
        temp_conn.execute(f"""PRAGMA cache_size = {10 * 1024}""")
        temp_conn.execute("""PRAGMA temp_store = 1""")
        temp_conn.execute(f"""PRAGMA temp_store_directory = '{self.cache_dir}'""")

        # Database structures
        with temp_conn, self.conn:
            cur = self.conn.cursor()

            # Remove all citations
            cur.execute(
                """\
DELETE FROM {}
WHERE enricher = :enricher
AND source = :source
""".format(self.CITATIONS_TABLE),
                {
                    "enricher": enricher_id,
                    "source": source_id,
                },
            )

            curtemp = temp_conn.cursor()
            # Publication table
            curtemp.execute("""DROP TABLE IF EXISTS tempcits""")
            # TEMP citations table
            curtemp.execute("""\
CREATE TABLE tempcits (
	pmid VARCHAR(4096) NOT NULL,
    cit_pmid VARCHAR(4096) NOT NULL
)
""")
            curtemp.close()
            temp_conn.commit()
            curtemp = temp_conn.cursor()

            for batch in itertools_batched(
                cur.execute(
                    """\
SELECT id, payload
FROM {}
WHERE
enricher = :enricher
AND
source = :source
                    """.format(self.REFERENCES_TABLE),
                    {"enricher": enricher_id, "source": source_id},
                ),
                n=10 * 1024,
            ):
                curtemp.executemany(
                    """\
INSERT INTO tempcits VALUES(?,?)
                    """,
                    (
                        (
                            ref["id"],
                            res[0],
                        )
                        for res in batch
                        for ref in self.jd.decode(
                            zlib.decompress(res[1]).decode("utf-8")
                        )
                    ),
                )

            curtemp.execute("""CREATE INDEX tempcits_pmid ON TEMPCITS(pmid)""")

            # The last executemany was exploding in memory with this
            # because although the input is an iterable, the whole
            # iterable is translated into a list.
            for batch in itertools_batched(
                (
                    {
                        "enricher": enricher_id,
                        "source": source_id,
                        "id": lastres[0],
                        "payload": zlib.compress(
                            ("[" + lastres[1] + "]").encode("utf-8"),
                            zlib.Z_BEST_COMPRESSION,
                        ),
                        "last_fetched": timestamp,
                    }
                    for lastres in curtemp.execute(
                        """\
SELECT pmid, '{{"id": "' || GROUP_CONCAT(cit_pmid, '", "source": "{0}"}},{{"id": "') || '", "source": "{0}"}}'
FROM tempcits
GROUP BY pmid
                    """.format(source_id)
                    )
                ),
                n=100 * 1024,
            ):
                cur.executemany(
                    """\
INSERT INTO {}(enricher,id,source,payload,last_fetched) VALUES(:enricher,:id,:source,:payload,:last_fetched)
""".format(self.CITATIONS_TABLE),
                    batch,
                )
        temp_conn.close()
        if os.path.exists(temp_db_file):
            os.unlink(temp_db_file)
