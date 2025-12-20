#!/usr/bin/env python3

import csv
import gzip
import hashlib
import pathlib
import tarfile
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
        PublishId,
        QualifiedId,
        SourceId,
        UnqualifiedId,
    )


from .europepmc_enricher import EuropePMCEnricher

from .offline_abstract_pub_enricher import OfflineAbstractPubEnricher

from .skeleton_pub_enricher import (
    PubEnricherException,
)

from . import pub_common


class OfflineEuropePMCEnricher(EuropePMCEnricher, OfflineAbstractPubEnricher):
    """This class adds offline capabilities"""

    PMC_LITE_METADATA_BASENAME: "Final[str]" = "PMCLiteMetadata.tgz"
    PMC_LITE_METADATA_URL: "Final[str]" = (
        "https://ftp.ebi.ac.uk/pub/databases/pmc/PMCLiteMetadata/"
        + PMC_LITE_METADATA_BASENAME
    )
    DOI_METADATA_BASENAME: "Final[str]" = "PMID_PMCID_DOI.csv.gz"
    DOI_METADATA_URL: "Final[str]" = (
        "https://ftp.ebi.ac.uk/pub/databases/pmc/DOI/" + DOI_METADATA_BASENAME
    )

    BATCH_THRESHOLD = 10240

    # Do not change these constants!!!
    OFFLINE_EUROPEPMC_ENRICHER: "Final[EnricherId]" = cast(
        "EnricherId", "offline_europepmc"
    )
    DEFAULT_EUROPEPMC_SOURCE: "Final[SourceId]" = cast("SourceId", "MED")

    @classmethod
    def Name(cls) -> "EnricherId":
        return cls.OFFLINE_EUROPEPMC_ENRICHER

    @classmethod
    def DefaultSource(cls) -> "SourceId":
        return cls.DEFAULT_EUROPEPMC_SOURCE

    @classmethod
    def ProvidesReferences(cls) -> "bool":
        return False

    def mirror_upstream(
        self,
        upstream_cache_dir: "pathlib.Path",
        upstream_cache_tracker: "Mapping[str, Tuple[bytes, int, float]]",
    ) -> "Sequence[Tuple[pathlib.Path, Tuple[bytes, int, float], bool]]":
        parsed_pmc_lite_metadata_url = urllib.parse.urlparse(self.PMC_LITE_METADATA_URL)
        parsed_doi_metadata_url = urllib.parse.urlparse(self.DOI_METADATA_URL)

        pmc_lite_cache_dir = upstream_cache_dir / "PMC_LITE"
        pmc_lite_cache_dir.mkdir(parents=True, exist_ok=True)
        doi_metadata_cache_dir = upstream_cache_dir / "DOI_METADATA"
        doi_metadata_cache_dir.mkdir(parents=True, exist_ok=True)

        command_script = f"""\
open {parsed_pmc_lite_metadata_url.scheme}://{parsed_pmc_lite_metadata_url.netloc}
mirror --verbose -f {parsed_pmc_lite_metadata_url.path} -O {pmc_lite_cache_dir.as_posix()}
close
open {parsed_doi_metadata_url.scheme}://{parsed_doi_metadata_url.netloc}
mirror --verbose -f {parsed_doi_metadata_url.path} -O {doi_metadata_cache_dir.as_posix()}
close
quit
    """
        exitcode = binary_lftp.run_lftp_script(command_script)
        if exitcode != 0:
            raise PubEnricherException(
                f"Failed mirroring of EuropePMC (lftp exitcode {exitcode})"
            )

        # TODO: incrementally process Pubmed dumps if raw mirror is newer than processed one
        # Last pass: find the target files
        dir_entries = []
        for entry in pub_common.scantree(upstream_cache_dir):
            if entry.is_file(follow_symlinks=False):
                if entry.name in (
                    self.DOI_METADATA_BASENAME,
                    self.PMC_LITE_METADATA_BASENAME,
                ):
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

        return dir_entries

    def digest_upstream_file(
        self,
        path: "pathlib.Path",
    ) -> "Iterator[Union[MutableMapping[QualifiedId, Tuple[IdMapping, Sequence[Reference]]], Sequence[IdMappingMinimal], Sequence[Tuple[Sequence[PublishId], QualifiedId]]]]":
        if path.name == self.PMC_LITE_METADATA_BASENAME:
            yield from self._digest_pmc_lite_metadata(path)
        elif path.name == self.DOI_METADATA_BASENAME:
            yield from self._digest_doi_metadata(path)

    def _digest_pmc_lite_metadata(
        self,
        path: "pathlib.Path",
    ) -> "Iterator[MutableMapping[QualifiedId, Tuple[IdMapping, Sequence[Reference]]]]":
        with tarfile.open(path, mode="r|*", bufsize=1024 * 1024) as tar:
            for tarinfo in tar:
                if tarinfo.isfile() and tarinfo.name.endswith(".xml"):
                    mappings_batch: "MutableMapping[QualifiedId, Tuple[IdMapping, Sequence[Reference]]]" = dict()
                    self.logger.warning(f"Parsing {tarinfo.path} {path.as_posix()}")
                    for _, elem in lxml.etree.iterparse(
                        tar.extractfile(tarinfo), tag=("PMC_ARTICLE",), recover=True
                    ):
                        pmid: "Optional[UnqualifiedId]" = None
                        year: "Optional[int]" = None
                        authors: "MutableSequence[Optional[str]]" = []

                        the_id = elem.findtext("id")
                        the_source = elem.findtext("source")

                        year_str = elem.findtext("PubYear")
                        if year_str is not None and len(year_str) > 0:
                            year = int(year_str)

                        title = elem.findtext("title")
                        journal = elem.findtext("JournalTitle")

                        aulist = elem.find("AuthorList")
                        if aulist is not None:
                            for au_elem in aulist:
                                auname = au_elem.findtext("LastName")
                                if auname is None:
                                    auname = au_elem.findtext("CollectiveName")
                                authors.append(auname)

                        # Starting point: known pubmed id
                        mapping: "IdMapping" = {
                            "source": the_source,
                            "id": the_id,
                            "year": year,
                            "title": title,
                            "journal": journal,
                            "authors": authors,
                        }

                        # Setting up the correspondences among bibliographic identifiers
                        pmid = elem.findtext("pmid")
                        if pmid is not None:
                            mapping["pmid"] = pmid
                        pmcid = elem.findtext("pmcid")
                        if pmcid is not None:
                            mapping["pmcid"] = pmcid
                        doi = elem.findtext("DOI")
                        if doi is not None:
                            mapping["doi"] = doi

                        # No references are provided in this format
                        mappings_batch[(the_source, the_id)] = (
                            mapping,
                            cast("Sequence[Reference]", []),
                        )

                        elem.clear(keep_tail=True)

                        # Propagating contents
                        if len(mappings_batch) >= self.BATCH_THRESHOLD:
                            yield mappings_batch
                            mappings_batch = dict()

                    # Remaining
                    if len(mappings_batch) > 0:
                        yield mappings_batch

    def _digest_doi_metadata(
        self,
        path: "pathlib.Path",
    ) -> "Iterator[Sequence[Tuple[Sequence[PublishId], QualifiedId]]]":
        batch_answer: "MutableSequence[Tuple[Sequence[PublishId], QualifiedId]]" = []
        total_read: "int" = 0
        with gzip.open(path, mode="rt", encoding="latin1") as dmH:
            csv_reader = csv.reader(dmH, dialect="excel")
            header = []
            pmid_col = -1
            pmcid_col = -1
            doi_col = -1

            for entry in csv_reader:
                if pmid_col >= 0:
                    total_read += 1
                    if total_read % 100000 == 0:
                        self.logger.warning(f"Read {total_read}")
                    pmid: "str" = entry[pmid_col]
                    pmcid: "str" = entry[pmcid_col]
                    doi: "str" = entry[doi_col]

                    if len(pmid) > 0 or (len(pmcid) > 0 and len(doi) > 0):
                        other_ids: "MutableSequence[PublishId]" = []
                        if len(pmid) != 0:
                            other_ids.append(pmid)
                        if len(pmcid) != 0:
                            other_ids.append(pmcid)
                        if len(doi) != 0:
                            other_ids.append(pmcid)

                        batch_answer.append(
                            (
                                other_ids,
                                (
                                    cast("SourceId", "MED" if len(pmid) > 0 else ""),
                                    cast("UnqualifiedId", pmid),
                                ),
                            )
                        )

                        # Propagating contents
                        if len(batch_answer) >= self.BATCH_THRESHOLD:
                            yield batch_answer
                            batch_answer = []

                else:
                    header = entry
                    for i_col, colname in enumerate(header):
                        if colname == "PMID":
                            pmid_col = i_col
                        elif colname == "PMCID":
                            pmcid_col = i_col
                        elif colname == "DOI":
                            doi_col = i_col

        self.logger.warning(f"Total read: {total_read}")
        if len(batch_answer) > 0:
            yield batch_answer
