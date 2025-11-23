#!/usr/bin/python

import json
import time
import configparser

from urllib import request
from urllib import parse

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
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
    )

    from .pub_common import (
        DOIId,
        EnricherId,
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


class PubmedEnricher(AbstractPubEnricher):
    # Due restrictions in the service usage
    # there cannot be more than 3 queries per second in
    # unregistered mode, and no more than 10 queries per second
    # in regisitered mode.
    UNREGISTERED_MIN_DELAY: "Final[float]" = 0.34
    REGISTERED_MIN_DELAY: "Final[float]" = 0.1

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

        self.api_key = self.config.get(section_name, "api_key")
        self.elink_step_size = self.config.getint(
            section_name, "elink_step_size", fallback=self.step_size
        )
        # Due restrictions in the service usage
        # there cannot be more than 3 queries per second in
        # unregistered mode, and no more than 10 queries per second
        # in regisitered mode.
        min_request_delay = (
            self.REGISTERED_MIN_DELAY if self.api_key else self.UNREGISTERED_MIN_DELAY
        )
        if self.request_delay < min_request_delay:
            self.request_delay = min_request_delay

    # Do not change this constant!!!
    PUBMED_SOURCE: "Final[EnricherId]" = cast("EnricherId", "pubmed")

    @classmethod
    def Name(cls) -> "EnricherId":
        return cls.PUBMED_SOURCE

    # Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESummary_
    PUB_ID_SUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    def populatePubIdsBatch(self, mappings: "MutableSequence[IdMapping]") -> None:
        if len(mappings) > 0:
            internal_ids = [mapping["id"] for mapping in mappings]
            theQuery = {
                "db": "pubmed",
                "id": " ".join(internal_ids),
                "retmode": "json",
                "retmax": 100000,
                "rettype": "abstract",
            }

            if self.api_key:
                theQuery["api_key"] = self.api_key

            summary_url_data = parse.urlencode(theQuery)
            debug_summary_url = self.PUB_ID_SUMMARY_URL + "?" + summary_url_data

            # Queries with retries
            entriesReq = request.Request(
                self.PUB_ID_SUMMARY_URL, data=summary_url_data.encode("utf-8")
            )
            retries = 0
            while retries <= self.max_retries:
                # Avoiding to hit the server too fast
                time.sleep(self.request_delay)

                raw_pubmed_mappings = self.retriable_full_http_read(
                    entriesReq, debug_url=debug_summary_url
                )

                try:
                    pubmed_mappings = self.jd.decode(
                        raw_pubmed_mappings.decode("utf-8")
                    )
                    break
                except json.decoder.JSONDecodeError:
                    retries += 1
                    retrymsg = "PubMed mappings JSON decoding error"
                    self.logger.debug(
                        f"Retry {retries}, due {retrymsg}. Dump:\n{raw_pubmed_mappings!r}"
                    )

            results = pubmed_mappings.get("result")
            if results is not None:
                uids = cast("Sequence[str]", results.get("uids", []))
                internal_ids_dict: "Mapping[UnqualifiedId, IdMapping]" = {
                    mapping["id"]: mapping for mapping in mappings
                }
                for uid in uids:
                    _id = cast("UnqualifiedId", str(uid))
                    result = results[_id]
                    mapping = internal_ids_dict.get(_id)
                    assert mapping is not None
                    # mapping = {
                    # 'id': _id,
                    ##	'title': result['title'],
                    ##	'journal': result.get('journalTitle'),
                    # 'source': self.PUBMED_SOURCE,
                    # 'query': query_str,
                    ##	'year': int(result['pubYear']),
                    ##	'pmid': pubmed_id,
                    ##	'doi': doi_id,
                    ##	'pmcid': pmc_id
                    # }
                    # mapping['source'] = self.PUBMED_SOURCE
                    mapping["title"] = result.get("title")
                    mapping["journal"] = result.get("fulljournalname")

                    # Computing the publication year
                    pubdate = result.get("sortpubdate")
                    pubyear = None

                    if pubdate is not None:
                        pubyear = int(pubdate.split("/")[0])

                    mapping["year"] = pubyear

                    mapping["authors"] = [
                        author.get("name") for author in result.get("authors", [])
                    ]

                    # Rescuing the identifiers
                    pubmed_id: "Optional[str]" = None
                    doi_id: "Optional[DOIId]" = None
                    pmc_id: "Optional[str]" = None
                    articleids = result.get("articleids")
                    if articleids is not None:
                        for articleid in result["articleids"]:
                            idtype = articleid.get("idtype")
                            if idtype is not None:
                                if idtype == "pubmed":
                                    pubmed_id = articleid.get("value")
                                    # Let's sanitize the id
                                    if pubmed_id is not None:
                                        pubmed_id = pubmed_id.strip()
                                elif idtype == "doi":
                                    # Let's sanitize the id
                                    if doi_id is not None:
                                        doi_id = doi_id.strip()
                                    doi_id = articleid.get("value")
                                elif idtype == "pmc":
                                    pmc_id = articleid.get("value")
                                    # Let's sanitize the id
                                    if pmc_id is not None:
                                        pmc_id = pmc_id.strip()

                    mapping["pmid"] = pubmed_id
                    mapping["doi"] = doi_id
                    mapping["pmcid"] = pmc_id

                # print(json.dumps(pubmed_mappings,indent=4))
                # sys.exit(1)

    # Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESearch_
    # Documentation: https://www.nlm.nih.gov/bsd/mms/medlineelements.html
    PUB_ID_CONVERTER_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    def queryPubIdsBatch(self, query_ids: "Sequence[QueryId]") -> "Sequence[IdMapping]":
        # Preparing the query ids
        raw_query_ids = []

        for query_id in query_ids:
            pubmed_id = query_id.get("pmid")
            if pubmed_id is not None:
                queryStr = str(pubmed_id) + "[pmid]"
                raw_query_ids.append(queryStr)

            doi_id_norm = query_id.get("doi")
            if doi_id_norm is not None:
                queryStr = '"' + doi_id_norm + '"[doi]'
                raw_query_ids.append(queryStr)

            pmc_id = query_id.get("pmcid")
            if pmc_id is not None:
                queryStr = str(pmc_id) + "[pmc]"
                raw_query_ids.append(queryStr)

        # Now, with the unknown ones, let's ask the server
        mappings_retval: "Sequence[IdMapping]" = []
        if len(raw_query_ids) > 0:
            # Step one: get the internal identifiers corresponding to the input queries
            theIdQuery = {
                "db": "pubmed",
                "term": " OR ".join(raw_query_ids),
                "format": "json",
            }

            if self.api_key:
                theIdQuery["api_key"] = self.api_key

            converter_url_data = parse.urlencode(theIdQuery)
            debug_converter_url = self.PUB_ID_CONVERTER_URL + "?" + converter_url_data

            # Queries with retries
            converterReq = request.Request(
                self.PUB_ID_CONVERTER_URL, data=converter_url_data.encode("utf-8")
            )

            retries = 0
            while retries <= self.max_retries:
                # Avoiding to hit the server too fast
                time.sleep(self.request_delay)

                raw_id_mappings = self.retriable_full_http_read(
                    converterReq, debug_url=debug_converter_url
                )

                try:
                    id_mappings = self.jd.decode(raw_id_mappings.decode("utf-8"))
                    break
                except json.decoder.JSONDecodeError:
                    retries += 1
                    retrymsg = "PubMed raw id mappings JSON decoding error"
                    self.logger.debug(
                        f"Retry {retries}, due {retrymsg}. Dump:\n{raw_id_mappings!r}"
                    )

            mappings = []
            # We record the unpaired DOIs
            eresult = id_mappings.get("esearchresult")
            if eresult is not None:
                idlist = eresult.get("idlist")

                # First, record the
                if idlist is not None:
                    translationstack = eresult.get("translationstack")
                    # This is a very minimal mapping
                    # needed to enrich and relate
                    if translationstack is not None:
                        for _id, query_str in zip(idlist, translationstack):
                            mapping = {
                                "id": str(_id),
                                "source": self.PUBMED_SOURCE,
                                #'query': query_str,
                            }
                            mappings.append(mapping)
                    else:
                        for _id in idlist:
                            # This is a very minimal mapping
                            # needed to enrich and relate
                            mapping = {
                                "id": str(_id),
                                "source": self.PUBMED_SOURCE,
                            }
                            mappings.append(mapping)

            # print(json.dumps(entries,indent=4))

            # Step two: get all the information of these input queries
            mappings_retval = self.populatePubIds(mappings)

        return mappings_retval

    # Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ELink_
    # Documented at: https://eutils.ncbi.nlm.nih.gov/entrez/query/static/entrezlinks.html
    # The drawback of this service is that it we are not careful enough,
    # it merges the answers from several queries
    ELINKS_URL: "Final[str]" = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    )
    ELINK_QUERY_MAPPINGS: "Final[Mapping[str, Tuple[str, str]]]" = {
        "pubmed_pubmed_citedin": pub_common.CITATIONS_KEYS,
        "pubmed_pubmed_refs": pub_common.REFERENCES_KEYS,
    }

    LINKNAME_MODE_MAP: "Final[Mapping[int, str]]" = {
        1: "pubmed_pubmed_refs",
        2: "pubmed_pubmed_citedin",
        3: "pubmed_pubmed_citedin,pubmed_pubmed_refs",
    }

    def queryCitRefsBatch(
        self,
        query_citations_data: "Iterable[IdMappingMinimal]",
        minimal: "bool" = False,
        mode: "int" = 3,
    ) -> "Iterator[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]":
        # First, saving the queries to issue
        raw_ids: "MutableSequence[UnqualifiedId]" = []
        query_hash: "MutableMapping[UnqualifiedId, IdMappingMinimal]" = {}

        search_linkname = self.LINKNAME_MODE_MAP.get(mode, self.LINKNAME_MODE_MAP[3])
        assert search_linkname is not None
        search_linknames = search_linkname.split(r",")

        for query in query_citations_data:
            raw_ids.append(query["id"])
            query_hash[query["id"]] = query

        # Second, query by batches
        for start in range(0, len(raw_ids), self.elink_step_size):
            stop = start + self.elink_step_size
            raw_ids_slice = raw_ids[start:stop]

            theLinksQuery = {
                "dbfrom": "pubmed",
                "linkname": search_linkname,
                "id": raw_ids_slice,
                "db": "pubmed",
                "retmode": "json",
            }

            if self.api_key:
                theLinksQuery["api_key"] = self.api_key

            elink_url_data = parse.urlencode(theLinksQuery, doseq=True)
            debug_elink_url = self.ELINKS_URL + "?" + elink_url_data

            # Queries with retries
            elinksReq = request.Request(
                self.ELINKS_URL, data=elink_url_data.encode("utf-8")
            )
            retries = 0
            raw_json_citations: "Optional[Mapping[str, Any]]" = None
            while retries <= self.max_retries:
                # Avoiding to hit the server too fast
                time.sleep(self.request_delay)

                raw_json_citation_refs = self.retriable_full_http_read(
                    elinksReq, debug_url=debug_elink_url
                )

                try:
                    raw_json_citations = self.jd.decode(
                        raw_json_citation_refs.decode("utf-8")
                    )
                    break
                except json.decoder.JSONDecodeError:
                    retries += 1
                    retrymsg = "PubMed citations JSON decoding error"
                    self.logger.debug(
                        f"Retry {retries}, due {retrymsg}. Dump:\n{raw_json_citation_refs!r}"
                    )

            if retries > self.max_retries:
                raise PubEnricherException(
                    f"Max {self.max_retries} reached requesting {self.ELINKS_URL + '?' + elink_url_data}, due {retrymsg}. Dump of last try:\n{raw_json_citation_refs!r}"
                )

            assert raw_json_citations is not None, (
                f"Assertion error requesting {self.ELINKS_URL + '?' + elink_url_data}"
            )

            linksets = raw_json_citations.get("linksets")
            if linksets is not None:
                cite_res_arr = []
                citrefsG: "MutableSequence[MutablePartialMapping]" = []
                for linkset in linksets:
                    ids = linkset.get("ids", [])
                    if len(ids) > 0:
                        _id = cast("UnqualifiedId", str(ids[0]))
                        linksetdbs = linkset.get("linksetdbs", [])

                        query = query_hash[_id]
                        source_id = query["source"]
                        cite_res: "GatheredCitRefs" = {  # type: ignore[typeddict-item]
                            "id": _id,
                            "source": source_id,
                        }

                        left_linknames = search_linknames.copy()

                        # The fetched results
                        if len(linksetdbs) > 0:
                            for linksetdb in linksetdbs:
                                linkname = linksetdb["linkname"]
                                left_linknames.remove(linkname)

                                mping = self.ELINK_QUERY_MAPPINGS.get(
                                    linkname, (None, None)
                                )
                                if mping[0] is not None and mping[0] not in query:
                                    citrefs_key, citrefs_count_key = mping
                                    # import sys
                                    # self.logger.debug(query_hash)
                                    links = linksetdb.get("links", [])

                                    citrefs: "Sequence[IdMappingMinimal]" = list(
                                        map(
                                            lambda uid: {
                                                "id": cast(
                                                    "UnqualifiedId", str(uid)
                                                ),  # _id
                                                "source": source_id,
                                            },
                                            links,
                                        )
                                    )

                                    cite_res[citrefs_key] = citrefs  # type: ignore[literal-required]
                                    cite_res[citrefs_count_key] = len(citrefs)  # type: ignore[literal-required]
                                    # To the batch of queries
                                    if not minimal:
                                        citrefsG.extend(
                                            cast(
                                                "Sequence[MutablePartialMapping]",
                                                citrefs,
                                            )
                                        )

                        # the unfetched ones with no error code
                        if len(left_linknames) > 0:
                            for linkname in left_linknames:
                                mping = self.ELINK_QUERY_MAPPINGS.get(
                                    linkname, (None, None)
                                )
                                assert mping[0] is not None
                                assert mping[1] is not None
                                citrefs_key, citrefs_count_key = mping

                                cite_res[citrefs_key] = None  # type: ignore[literal-required]
                                cite_res[citrefs_count_key] = 0  # type: ignore[literal-required]

                        # Now, issue the batch query
                        if not minimal and (len(citrefsG) > 0):
                            self.populatePubIds(citrefsG, onlyYear=True)

                        # Saving it for later processing
                        cite_res_arr.append(cite_res)

                if citrefsG:
                    # Now, issue the last batch query
                    self.populatePubIds(citrefsG, onlyYear=True)

                # And propagate the batch of results!
                for cite_res in cite_res_arr:
                    yield cite_res
