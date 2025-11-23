#!/usr/bin/python

import time
import math
import re
import configparser

from urllib import request
from urllib import parse
from urllib.error import (
    HTTPError,
)

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

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
        Citation,
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


class EuropePMCEnricher(AbstractPubEnricher):
    DEFAULT_CITREF_PAGESIZE = 100

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
        # self.debug_cache_dir = os.path.join(cache_dir,'debug')
        # os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
        # self._debug_count = 0

        super().__init__(
            cache,
            prefix=prefix,
            config=config,
            doi_checker=doi_checker,
            is_db_synchronous=is_db_synchronous,
        )

        # The section name is the symbolic name given to this class
        section_name = self.Name()

        self.citref_step_size = self.config.getint(
            section_name, "citref_step_size", fallback=self.DEFAULT_CITREF_PAGESIZE
        )

    @classmethod
    def Name(cls) -> "EnricherId":
        return cast("EnricherId", "europepmc")

    # Documentation at: https://europepmc.org/RestfulWebService#search
    # Documentation at: https://europepmc.org/docs/EBI_Europe_PMC_Web_Service_Reference.pdf
    OPENPMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

    def populatePubIdsBatch(
        self, partial_mappings: "MutableSequence[IdMapping]"
    ) -> None:
        if len(partial_mappings) > 0:
            # Preparing the query ids
            raw_query_ids = []

            # In order to reduce the query string, group by source
            clustered_partial_mappings: "MutableMapping[SourceId, MutableSequence[UnqualifiedId]]" = {}
            for partial_mapping in partial_mappings:
                maybe_source_id = partial_mapping.get("source")
                if maybe_source_id is not None:
                    the_id = partial_mapping.get("id")
                    assert the_id is not None
                    clustered_partial_mappings.setdefault(maybe_source_id, []).append(
                        the_id
                    )

            for source_id, clustered_partial in clustered_partial_mappings.items():
                ext_string = (
                    '( "' + '" or "'.join(clustered_partial) + '" )'
                    if len(clustered_partial) > 1
                    else '"' + clustered_partial[0] + '"'
                )
                raw_query_ids.append(
                    '( SRC:"' + source_id + '" EXT_ID:' + ext_string + " )"
                )

            # Now, with the unknown ones, let's ask the server
            theQuery = {
                "format": "json",
                "pageSize": 1000,
                "query": " or ".join(raw_query_ids),
            }

            searchURL = (
                self.OPENPMC_SEARCH_URL
                + "?"
                + parse.urlencode(theQuery, encoding="utf-8")
            )
            # sys.exit(1)

            # Queries with retries
            searchReq = request.Request(searchURL)
            raw_json_pubs_mappings = self.retriable_full_http_read(
                searchReq, debug_url=searchURL
            )

            # debug_cache_filename = os.path.join(self.debug_cache_dir,str(self._debug_count) + '.json')
            # self._debug_count += 1
            # with open(debug_cache_filename,mode="wb") as d:
            # d.write(raw_json_pubs_mappings)

            pubs_mappings = self.jd.decode(raw_json_pubs_mappings.decode("utf-8"))

            # Avoiding to hit the server too fast
            time.sleep(self.request_delay)

            resultList = pubs_mappings.get("resultList")
            if resultList is not None and "result" in resultList:
                internal_ids_dict: "Mapping[Tuple[UnqualifiedId, SourceId], IdMapping]" = {
                    (partial_mapping["id"], partial_mapping["source"]): partial_mapping
                    for partial_mapping in partial_mappings
                }
                # Gathering results
                for result in resultList["result"]:
                    _id = result["id"]
                    source_id = result["source"]
                    maybe_partial_mapping = internal_ids_dict.get((_id, source_id))
                    if maybe_partial_mapping is not None:
                        pubmed_id = result.get("pmid")
                        doi_id = result.get("doi")
                        pmc_id = result.get("pmcid")

                        # Let's sanitize the ids
                        if pubmed_id is not None:
                            pubmed_id = pubmed_id.strip()
                        if doi_id is not None:
                            doi_id = doi_id.strip()
                        if pmc_id is not None:
                            pmc_id = pmc_id.strip()

                        authors = list(
                            filter(
                                lambda author: len(author) > 0,
                                re.split(r"[.,]+\s*", result.get("authorString", "")),
                            )
                        )

                        maybe_partial_mapping["title"] = result.get("title")
                        maybe_partial_mapping["journal"] = result.get("journalTitle")
                        pubYear = result.get("pubYear")
                        if pubYear is not None:
                            pubYear = int(pubYear)
                        maybe_partial_mapping["year"] = pubYear
                        maybe_partial_mapping["authors"] = authors
                        maybe_partial_mapping["pmid"] = pubmed_id
                        maybe_partial_mapping["doi"] = doi_id
                        maybe_partial_mapping["pmcid"] = pmc_id

                # print(json.dumps(entries,indent=4))

    def queryPubIdsBatch(self, query_ids: "Sequence[QueryId]") -> "Sequence[IdMapping]":
        # Preparing the query ids
        raw_query_ids = []

        # Clustering the queries in order to reduce the query string
        q_pubmed_ids: "MutableSequence[str]" = []
        q_doi_ids: "MutableSequence[str]" = []
        q_pmc_ids: "MutableSequence[str]" = []
        for query_id in query_ids:
            pubmed_id_g = query_id.get("pmid")
            if pubmed_id_g is not None:
                q_pubmed_ids.append(str(pubmed_id_g))

            doi_id_norm = query_id.get("doi")
            if doi_id_norm is not None:
                q_doi_ids.append(doi_id_norm)

            pmc_id_g = query_id.get("pmcid")
            if pmc_id_g is not None:
                q_pmc_ids.append(str(pmc_id_g))

        if q_pubmed_ids:
            qstr_pubmed = (
                '("' + '" or "'.join(q_pubmed_ids) + '")'
                if len(q_pubmed_ids) > 1
                else '"' + q_pubmed_ids[0] + '"'
            )
            raw_query_ids.append("( SRC:MED EXT_ID:" + qstr_pubmed + " )")

        if q_doi_ids:
            qstr_doi = (
                '("' + '" or "'.join(q_doi_ids) + '")'
                if len(q_doi_ids) > 1
                else '"' + q_doi_ids[0] + '"'
            )
            raw_query_ids.append("DOI:" + qstr_doi)

        if q_pmc_ids:
            qstr_pmc = (
                '("' + '" or "'.join(q_pmc_ids) + '")'
                if len(q_pmc_ids) > 1
                else '"' + q_pmc_ids[0] + '"'
            )
            raw_query_ids.append("PMCID:" + qstr_pmc)

        # Now, with the unknown ones, let's ask the server
        mappings: "MutableSequence[IdMapping]" = []
        if len(raw_query_ids) > 0:
            theQuery = {
                "format": "json",
                "pageSize": 1000,
                "query": " or ".join(raw_query_ids),
            }
            searchURL = (
                self.OPENPMC_SEARCH_URL
                + "?"
                + parse.urlencode(theQuery, encoding="utf-8")
            )

            # Queries with retries
            searchReq = request.Request(searchURL)
            raw_json_pubs_mappings = self.retriable_full_http_read(
                searchReq, debug_url=searchURL
            )

            # debug_cache_filename = os.path.join(self.debug_cache_dir,str(self._debug_count) + '.json')
            # self._debug_count += 1
            # with open(debug_cache_filename,mode="wb") as d:
            # d.write(raw_json_pubs_mappings)

            pubs_mappings = self.jd.decode(raw_json_pubs_mappings.decode("utf-8"))

            # Avoiding to hit the server too fast
            time.sleep(self.request_delay)

            resultList = pubs_mappings.get("resultList")
            if resultList is not None and "result" in resultList:
                # Gathering results
                for result in resultList["result"]:
                    _id = result["id"]
                    pubmed_id_g = result.get("pmid")
                    pubmed_id: "Optional[str]" = None
                    doi_id = result.get("doi")
                    pmc_id_g = result.get("pmcid")
                    pmc_id: "Optional[str]" = None

                    # Let's sanitize the ids
                    if pubmed_id_g is not None:
                        pubmed_id = str(pubmed_id_g).strip()
                    if doi_id is not None:
                        doi_id = doi_id.strip()
                    if pmc_id_g is not None:
                        pmc_id = str(pmc_id_g).strip()

                    source_id = result.get("source")
                    authors = list(
                        filter(
                            lambda author: len(author) > 0,
                            re.split(r"[.,]+\s*", result.get("authorString", "")),
                        )
                    )

                    if (
                        pubmed_id is not None
                        or doi_id is not None
                        or pmc_id is not None
                    ):
                        pubYear = result.get("pubYear")
                        if pubYear is not None:
                            pubYear = int(pubYear)
                        else:
                            self.logger.debug(f"EuropePMC {source_id} {_id}")
                        mapping: "IdMapping" = {
                            "id": _id,
                            "title": result.get("title"),
                            "journal": result.get("journalTitle"),
                            "source": source_id,
                            "year": pubYear,
                            "authors": authors,
                            "pmid": pubmed_id,
                            "doi": doi_id,
                            "pmcid": pmc_id,
                        }

                        mappings.append(mapping)
            # self.logger.debug(json.dumps(entries,indent=4))

        # self.logger.debug(json.dumps(mappings,indent=4,sort_keys=True))
        # sys.exit(1)

        return mappings

    # Documentation at: https://europepmc.org/RestfulWebService#cites
    # Url used to retrive the citations, i.e MED is publications from PubMed and MEDLINE view https://europepmc.org/RestfulWebService;jsessionid=7AD7C81CF5F041840F59CF49ABB29994#cites
    CITREF_ENDPOINT_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/"

    def querySingleCitRef(
        self, source_id: "str", _id: "str", query_mode: "bool"
    ) -> "Tuple[Optional[Sequence[Union[Citation, Reference]]],int]":
        if query_mode:
            query = "citations"
            citref_list_key = "citationList"
            citref_key = "citation"
        else:
            query = "references"
            citref_list_key = "referenceList"
            citref_key = "reference"

        pageSize = self.citref_step_size
        page: "Optional[int]" = 1
        citref_count: "int" = -1
        pages = -1
        citrefs: "Optional[MutableSequence[Union[Citation, Reference]]]" = []
        while page is not None and page != 0:
            partialURL = "/".join(
                map(
                    lambda elem: parse.quote(str(elem), safe=""),
                    [source_id, _id, query, page, pageSize, "json"],
                )
            )
            citref_url = parse.urljoin(self.CITREF_ENDPOINT_URL, partialURL)

            # Queries with retries
            citrefReq = request.Request(citref_url)
            try:
                raw_json_citrefs = self.retriable_full_http_read(
                    citrefReq, debug_url=citref_url
                )

                # debug_cache_filename = os.path.join(self.debug_cache_dir,'cite_' + str(self._debug_count) + '.json')
                # self._debug_count += 1
                # with open(debug_cache_filename,mode="wb") as d:
                # d.write(raw_json_citrefs)

                citref_res = self.jd.decode(raw_json_citrefs.decode("utf-8"))

                # Avoiding to hit the server too fast
                time.sleep(self.request_delay)
            except HTTPError as e:
                if e.code == 404:
                    citrefs = None
                    # Needed to properly cache the negative result
                    citref_count = 0
                    # We need to go out the outer loop
                    break
                else:
                    raise e

            if citref_count == -1:
                citref_count = citref_res.get("hitCount", 0)

                if citref_count == 0:
                    page = 0
                    pages = 0
                else:
                    pages = math.ceil(citref_count / pageSize)

            if citref_list_key in citref_res:
                if citref_key in citref_res[citref_list_key]:
                    citref_list = citref_res[citref_list_key][citref_key]
                    for citref in citref_list:
                        pubYear = citref["pubYear"]
                        filtered_citref: "Union[Citation, Reference]" = {  # type: ignore[assignment]
                            "id": citref.get("id"),
                            "source": citref.get("source"),
                        }
                        if pubYear is not None:
                            filtered_citref["year"] = int(pubYear)

                        assert citrefs is not None
                        citrefs.append(filtered_citref)

            if page < pages:
                # Next Page
                page += 1
            else:
                page = None

        return citrefs, citref_count

    # Documentation at: https://europepmc.org/RestfulWebService#cites
    CITATION_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/"

    def queryCitRefsBatch(
        self,
        query_citrefs_data: "Iterable[IdMappingMinimal]",
        minimal: "bool" = False,
        mode: "int" = 3,
    ) -> "Iterator[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]":
        for pub_field in query_citrefs_data:
            _id = pub_field.get("id")  # 11932250
            if _id is not None:
                source_id = pub_field["source"]
                citref: "GatheredCitRefs" = {  # type: ignore[typeddict-item]
                    "id": _id,
                    "source": source_id,
                }

                if (mode & 2) != 0 and (pub_field.get("citations") is None):
                    citations, citation_count = self.querySingleCitRef(
                        source_id, _id, True
                    )
                    if citations is None:
                        citations = []
                    citref["citations"] = citations
                    citref["citation_count"] = citation_count

                if (mode & 1) != 0 and (pub_field.get("references") is None):
                    references, reference_count = self.querySingleCitRef(
                        source_id, _id, False
                    )
                    if references is None:
                        references = []
                    citref["references"] = references
                    citref["reference_count"] = reference_count

                yield citref
