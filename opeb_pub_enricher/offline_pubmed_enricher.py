#!/usr/bin/env python3

import gzip
import hashlib
import pathlib
import urllib.parse
import urllib.request

from typing import (
    cast,
    TYPE_CHECKING,
)

import binary_lftp

import lxml.etree

if TYPE_CHECKING:
    from typing import (
        Final,
        Iterator,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        Union,
    )

    from .pub_cache import (
        IdMapping,
        IdMappingMinimal,
        Reference,
    )

    from .pub_common import (
        EnricherId,
        QualifiedId,
        SourceId,
        UnqualifiedId,
    )

    from .skeleton_pub_enricher import (
        MutablePartialMapping,
    )


from .offline_abstract_pub_enricher import OfflineAbstractPubEnricher

from .skeleton_pub_enricher import (
    PubEnricherException,
)

from . import pub_common


class OfflinePubmedEnricher(OfflineAbstractPubEnricher):
    PUBMED_BASELINE_URL: "Final[str]" = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    PUBMED_UPDATEFILES_URL: "Final[str]" = (
        "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
    )

    BATCH_THRESHOLD = 10240

    # Do not change these constants!!!
    OFFLINE_PUBMED_SOURCE: "Final[EnricherId]" = cast("EnricherId", "offline_pubmed")
    PUBMED_SOURCE: "Final[SourceId]" = cast("SourceId", "pubmed")

    @classmethod
    def Name(cls) -> "EnricherId":
        return cls.OFFLINE_PUBMED_SOURCE

    @classmethod
    def DefaultSource(cls) -> "SourceId":
        return cls.PUBMED_SOURCE

    @classmethod
    def ProvidesReferences(cls) -> "bool":
        return True

    def mirror_upstream(
        self,
        upstream_cache_dir: "pathlib.Path",
        upstream_cache_tracker: "Mapping[str, Tuple[bytes, int, float]]",
    ) -> "Sequence[Tuple[pathlib.Path, Tuple[bytes, int, float], bool]]":
        # TODO: mirror Pubmed dumps on instantiation
        parsed_baseline_url = urllib.parse.urlparse(self.PUBMED_BASELINE_URL)
        parsed_updatefiles_url = urllib.parse.urlparse(self.PUBMED_UPDATEFILES_URL)

        baseline_cache_dir = upstream_cache_dir / "BASELINE"
        baseline_cache_dir.mkdir(parents=True, exist_ok=True)
        updatefiles_cache_dir = upstream_cache_dir / "UPDATEFILES"
        updatefiles_cache_dir.mkdir(parents=True, exist_ok=True)

        command_script = f"""\
open {parsed_baseline_url.scheme}://{parsed_baseline_url.netloc}
mirror -e --scan-all-first --delete-first --verbose {parsed_baseline_url.path} {baseline_cache_dir.as_posix()}
close
open {parsed_updatefiles_url.scheme}://{parsed_updatefiles_url.netloc}
mirror -e --scan-all-first --delete-first --verbose {parsed_updatefiles_url.path} {updatefiles_cache_dir.as_posix()}
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
        for entry in pub_common.scantree(upstream_cache_dir):
            if entry.is_file(follow_symlinks=False):
                if entry.name.startswith("pubmed") and entry.name.endswith(".xml.gz"):
                    cached_fingerprint: "Optional[Tuple[bytes, int, float]]" = (
                        upstream_cache_tracker.get(entry.name)
                    )

                    entry_stat = entry.stat()
                    # Fail fast path
                    failed_fingerprint = (
                        cached_fingerprint is None
                        or cached_fingerprint[1] != entry_stat.st_size
                        or len(cached_fingerprint) == 2
                        or cached_fingerprint[2] != entry_stat.st_ctime
                    )
                    if failed_fingerprint:
                        with open(entry.path, mode="rb") as cH:
                            h = hashlib.sha1()
                            while True:
                                chunk = cH.read(1024 * 1024)
                                if chunk is None or len(chunk) == 0:
                                    break

                                h.update(chunk)

                            fingerprint: "Tuple[bytes, int, float]" = (
                                h.digest(),
                                entry_stat.st_size,
                                entry_stat.st_ctime,
                            )

                        # Save for later processing
                        dir_entries.append(
                            (
                                pathlib.Path(entry.path),
                                fingerprint,
                                cached_fingerprint is None
                                or cached_fingerprint[0] != fingerprint[0]
                                or cached_fingerprint[1] != fingerprint[1],
                            )
                        )

        # Order by name in place, not full name
        dir_entries.sort(key=lambda ent: ent[0].name)

        return dir_entries

    def digest_upstream_file(
        self,
        path: "pathlib.Path",
    ) -> "Iterator[Union[MutableMapping[QualifiedId, Tuple[IdMapping, Sequence[Reference]]], Sequence[IdMappingMinimal]]]":
        with gzip.open(path, mode="rb") as pH:
            mappings_batch: "MutableMapping[QualifiedId, Tuple[IdMapping, Sequence[Reference]]]" = dict()
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
                        "source": self.PUBMED_SOURCE,
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
                            if ref_e.text is not None:
                                references.append(
                                    {"id": ref_e.text, "source": self.PUBMED_SOURCE}
                                )
                            else:
                                self.logger.warning(
                                    f"FIXME: Have a look at PMID {pmid} references"
                                )

                    mappings_batch[(mapping["source"], mapping["id"])] = (
                        mapping,
                        cast("Sequence[Reference]", references),
                    )

                elif elem.tag == "DeleteCitation":
                    the_source_id = self.DefaultSource()
                    yield [
                        {
                            "id": p_elem.text,
                            "source": the_source_id,
                        }
                        for p_elem in elem
                    ]
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
                        "source": self.PUBMED_SOURCE,
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
                            if ref_e.text is not None:
                                references.append(
                                    {"id": ref_e.text, "source": self.PUBMED_SOURCE}
                                )
                            else:
                                self.logger.warning(
                                    f"FIXME: Have a look at PMID {pmid} references"
                                )

                    mappings_batch[(mapping["source"], mapping["id"])] = (
                        mapping,
                        cast("Sequence[Reference]", references),
                    )

                # Propagating contents
                if pmid is not None:
                    if len(mappings_batch) >= self.BATCH_THRESHOLD:
                        yield mappings_batch
                        mappings_batch = dict()

                elem.clear(keep_tail=True)

            # Remaining
            if len(mappings_batch) > 0:
                yield mappings_batch
