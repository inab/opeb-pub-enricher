#!/usr/bin/env python3

import configparser
import gzip
import pathlib
import urllib.parse
import urllib.request

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

import binary_lftp

import diskcache
import lxml.etree

if TYPE_CHECKING:
    from typing import (
        Final,
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

    from .skeleton_pub_enricher import (
        MutablePartialMapping,
    )


from .abstract_pub_enricher import AbstractPubEnricher

from .skeleton_pub_enricher import (
    PubEnricherException,
)

from . import pub_common


class OfflinePubmedEnricher(AbstractPubEnricher):
    PUBMED_BASELINE_URL = "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    PUBMED_UPDATEFILES_URL = "ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"

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

        self.ftp_cache_dir = pathlib.Path(self.cache_dir) / (self.Name() + "_FTP")
        self.ftp_cache_tracker = diskcache.Cache(
            self.ftp_cache_dir.as_posix(), eviction_policy="none"
        )

        dir_entries = self._mirror_pubmed()

        if len(dir_entries) > 0:
            self._digest_pubmed_dir_entries(dir_entries)
            # pass

    # Do not change these constants!!!
    OFFLINE_PUBMED_SOURCE: "Final[EnricherId]" = cast("EnricherId", "offline_pubmed")
    PUBMED_SOURCE: "Final[EnricherId]" = cast("EnricherId", "pubmed")

    @classmethod
    def Name(cls) -> "EnricherId":
        return cls.OFFLINE_PUBMED_SOURCE

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

    def _mirror_pubmed(self) -> "Sequence[pathlib.Path]":
        # TODO: mirror Pubmed dumps on instantiation
        parsed_baseline_url = urllib.parse.urlparse(self.PUBMED_BASELINE_URL)
        parsed_updatefiles_url = urllib.parse.urlparse(self.PUBMED_UPDATEFILES_URL)

        baseline_cache_dir = self.ftp_cache_dir / "BASELINE"
        baseline_cache_dir.mkdir(parents=True, exist_ok=True)
        updatefiles_cache_dir = self.ftp_cache_dir / "UPDATEFILES"
        updatefiles_cache_dir.mkdir(parents=True, exist_ok=True)

        command_script = f"""\
open {parsed_baseline_url.netloc}
mirror -c -e --scan-all-first --verbose {parsed_baseline_url.path} {baseline_cache_dir.as_posix()}
close
open {parsed_updatefiles_url.netloc}
mirror -c -e --scan-all-first --verbose {parsed_updatefiles_url.path} {updatefiles_cache_dir.as_posix()}
close
quit
    """
        exitcode = binary_lftp.run_lftp_script(command_script)
        if exitcode != 0:
            raise PubEnricherException(
                f"Failed mirroring of PubMed (lftp exitcode {exitcode})"
            )

        # TODO: incrementally process Pubmed dumps if raw mirror is newer than processed one
        # Last pass: find the target files
        dir_entries = []
        for entry in pub_common.scantree(self.ftp_cache_dir):
            if entry.is_file(follow_symlinks=False):
                if entry.name.startswith("pubmed") and entry.name.endswith(".xml.gz"):
                    # Save for later processing
                    dir_entries.append(pathlib.Path(entry.path))

        # Order by name in place, not full name
        dir_entries.sort(key=lambda ent: ent.name)

        return dir_entries

    def _digest_pubmed_dir_entries(
        self, dir_entries: "Sequence[pathlib.Path]"
    ) -> "None":
        citations_cache_dir = pathlib.Path(self.cache_dir) / (
            self.Name() + "_Digest.db"
        )
        citations_cache = diskcache.Cache(
            citations_cache_dir.as_posix(), eviction_policy="none"
        )
        # In case something was still there
        citations_cache.clear()

        # Now, let's process this
        for entry in dir_entries:
            self._digest_pubmed_file(entry, citations_cache)

        # And postprocess
        the_source_id = cast("SourceId", self.PUBMED_SOURCE)
        # This artificial separation is needed to avoid having the whole
        # list of cited manuscripts in memory
        self.pubC.clearCitRefs(
            (
                (
                    (the_source_id, cast("UnqualifiedId", pmid)),
                    True,
                )
                for pmid in citations_cache
            )
        )
        # This artificial separation is needed to avoid having the whole
        # list of cited manuscripts in memory
        self.pubC.setCitRefs_ll(
            (
                (
                    (the_source_id, cast("UnqualifiedId", pmid)),
                    citations_cache[pmid],
                    True,
                )
                for pmid in citations_cache
            )
        )

        # citations_cache.clear()

    def __commit_batch(
        self,
        mappings_batch: "Mapping[UnqualifiedId, Tuple[IdMapping, Sequence[Reference]]]",
        citations_cache: "diskcache.Cache",
    ) -> "None":
        """Note: the batch must have unique values"""
        self.pubC.setCachedMappings(
            [mapping for mapping, references in mappings_batch.values()]
        )

        the_source_id = cast("SourceId", self.PUBMED_SOURCE)
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
            )
        )
        # And saving the reverse ones
        pre_citations: "MutableMapping[UnqualifiedId, MutableSequence[UnqualifiedId]]" = dict()
        for mapping, references in mappings_batch.values():
            if len(references) > 0:
                pmid = mapping["id"]
                for ref_e in references:
                    pre_citations.setdefault(ref_e["id"], []).append(pmid)

        with citations_cache.transact():
            for pmid, partial_citations in pre_citations.items():
                citations = citations_cache.get(pmid)
                if citations is None:
                    citations = partial_citations
                else:
                    citations.extend(partial_citations)

                citations_cache[pmid] = citations

    def _digest_pubmed_file(
        self, path: "pathlib.Path", citations_cache: "diskcache.Cache"
    ) -> "None":
        with gzip.open(path, mode="rb") as pH:
            mappings_batch: "MutableMapping[UnqualifiedId, Tuple[IdMapping, Sequence[Reference]]]" = dict()
            for _, elem in lxml.etree.iterparse(
                pH, tag=("PubmedArticle", "PubmedBookArticleDeleteCitation")
            ):
                pmid: "Optional[UnqualifiedId]" = None
                year: "Optional[int]" = None
                authors: "MutableSequence[Optional[str]]" = []
                if elem.tag == "PubmedArticle":
                    mcit = elem.find("MedlineCitation")
                    assert mcit is not None
                    art = mcit.find("Article")
                    assert art is not None

                    year_str = art.findtext("./Journal/JournalIssue/PubDate/Year")
                    if year_str is not None:
                        year = int(year_str)
                    for artda in art.findall("ArticleDate"):
                        the_year_str = artda.findtext("Year")
                        if the_year_str is not None:
                            the_year = int(the_year_str)
                            if year is None or year > the_year:
                                year = the_year

                    title = art.findtext("ArticleTitle")
                    journal = art.findtext("./Journal/Title")

                    pmid = mcit.findtext("PMID")

                    aulist = art.find("AuthorList")
                    if aulist is not None:
                        for au_elem in aulist:
                            auname = au_elem.findtext("LastName")
                            if auname is None:
                                auname = au_elem.findtext("CollectiveName")
                            authors.append(auname)

                    # Starting point: known pubmed id
                    mapping: "IdMapping" = {
                        "source": cast("SourceId", self.PUBMED_SOURCE),
                        "id": pmid,
                        "pmid": pmid,
                        "year": year,
                        "title": title,
                        "journal": journal,
                        "authors": authors,
                    }

                    # Setting up the correspondences among bibliographic identifiers
                    pdat = elem.find("PubmedData")
                    for arti in pdat.findall("./ArticleIdList/ArticleId"):
                        id_type = arti.get("IdType")
                        if id_type == "pubmed":
                            mapping["pmid"] = arti.text
                        elif id_type == "doi":
                            mapping["doi"] = arti.text
                        elif id_type == "pmc":
                            mapping["pmcid"] = arti.text

                    # And now, the references
                    # (the citations should be computed later)
                    references: "MutableSequence[MutablePartialMapping]" = []
                    for ref_e in pdat.findall(
                        "./ReferenceList/Reference/ArticleIdList/ArticleId"
                    ):
                        if ref_e.get("IdType") == self.PUBMED_SOURCE:
                            references.append(
                                {"id": ref_e.text, "source": self.PUBMED_SOURCE}
                            )

                    mappings_batch[pmid] = (
                        mapping,
                        cast("Sequence[Reference]", references),
                    )

                elif elem.tag == "DeleteCitation":
                    the_source_id = cast("SourceId", self.PUBMED_SOURCE)
                    self.pubC.removeCachedMappings(
                        [
                            {
                                "id": p_elem.text,
                                "source": the_source_id,
                            }
                            for p_elem in elem
                        ]
                    )
                    self.pubC.clearCitRefs(
                        (
                            (
                                (the_source_id, cast("UnqualifiedId", p_elem.text)),
                                False,
                            )
                            for p_elem in elem
                        )
                    )
                    self.pubC.clearCitRefs(
                        (
                            (
                                (the_source_id, cast("UnqualifiedId", p_elem.text)),
                                True,
                            )
                            for p_elem in elem
                        )
                    )
                    for p_elem in elem:
                        pmid = p_elem.text
                        if pmid in citations_cache:
                            del citations_cache[pmid]
                elif elem.tag == "PubmedBookArticle":
                    bdoc = elem.find("BookDocument")
                    assert bdoc is not None

                    book = bdoc.find("Book")
                    assert book is not None

                    year_str = book.findtext("./PubDate/Year")
                    if year_str is not None:
                        year = int(year_str)

                    title = book.findtext("ArticleTitle")
                    journal = None

                    pmid = bdoc.findtext("PMID")

                    for aulist in bdoc.findall("AuthorList"):
                        for au_elem in aulist:
                            auname = au_elem.findtext("LastName")
                            if auname is None:
                                auname = au_elem.findtext("CollectiveName")
                            authors.append(auname)

                    # Starting point: known pubmed id
                    mapping = {
                        "source": cast("SourceId", self.PUBMED_SOURCE),
                        "id": pmid,
                        "pmid": pmid,
                        "year": year,
                        "title": title,
                        "journal": journal,
                        "authors": authors,
                    }

                    # Setting up the correspondences among bibliographic identifiers
                    pdat = elem.find("PubmedBookData")
                    for arti in pdat.findall("./ArticleIdList/ArticleId"):
                        id_type = arti.get("IdType")
                        if id_type == "pubmed":
                            mapping["pmid"] = arti.text
                        elif id_type == "doi":
                            mapping["doi"] = arti.text
                        elif id_type == "pmc":
                            mapping["pmcid"] = arti.text

                    # And now, the references which appear in the book
                    # (the citations should be computed later)
                    references = []
                    for ref_e in bdoc.findall(
                        "./ReferenceList/Reference/ArticleIdList/ArticleId"
                    ):
                        if ref_e.get("IdType") == self.PUBMED_SOURCE:
                            references.append(
                                {"id": ref_e.text, "source": self.PUBMED_SOURCE}
                            )

                    mappings_batch[pmid] = (
                        mapping,
                        cast("Sequence[Reference]", references),
                    )

                # Propagating contents
                if pmid is not None:
                    if len(mappings_batch) >= self.BATCH_THRESHOLD:
                        self.__commit_batch(mappings_batch, citations_cache)
                        mappings_batch = dict()

                elem.clear(keep_tail=True)

            # Remaining
            if len(mappings_batch) > 0:
                self.__commit_batch(mappings_batch, citations_cache)
