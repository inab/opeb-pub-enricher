#!/usr/bin/python

import datetime
import http.client
import time
import urllib.error

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import configparser
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
        TypedDict,
        Union,
    )

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
        EnricherId,
        SourceId,
        UnqualifiedId,
    )

    from .doi_cache import DOIChecker

    class SPARQLResult(TypedDict):
        results: Mapping[str, Any]


import SPARQLWrapper
import SPARQLWrapper.SPARQLExceptions

from .abstract_pub_enricher import AbstractPubEnricher

from . import pub_common


def _extractYear(pubdateStr: "str") -> "int":
    if pubdateStr[0] == "t":
        pubdate = datetime.datetime.fromtimestamp(
            float(pubdateStr[1:]), datetime.timezone.utc
        )
    else:
        pubdate = datetime.datetime.strptime(pubdateStr, "%Y-%m-%dT%H:%M:%SZ")
    return pubdate.year


class WikidataEnricher(AbstractPubEnricher):
    @overload
    def __init__(
        self,
        cache: "str",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ): ...

    @overload
    def __init__(
        self,
        cache: "PubDBCache",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ): ...

    def __init__(
        self,
        cache: "Union[PubDBCache, str]",
        prefix: "Optional[str]" = None,
        config: "Optional[configparser.ConfigParser]" = None,
        doi_checker: "Optional[DOIChecker]" = None,
    ):
        # self.debug_cache_dir = os.path.join(cache_dir,'debug')
        # os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
        # self._debug_count = 0

        super().__init__(cache, prefix=prefix, config=config, doi_checker=doi_checker)

        # The section name is the symbolic name given to this class
        section_name = self.Name()

        self.wikidata_step_size = self.config.getint(
            section_name, "wikidata_step_size", fallback=self.step_size
        )

        self.wikidata_query_step_size = self.config.getint(
            section_name, "wikidata_query_step_size", fallback=self.wikidata_step_size
        )

    # Do not change this constant!!!
    WIKIDATA_SOURCE: "Final[EnricherId]" = cast("EnricherId", "wikidata")
    WIKIDATA_SPARQL_ENDPOINT: "Final[str]" = "https://query.wikidata.org/sparql"

    @classmethod
    def Name(cls) -> "EnricherId":
        return cls.WIKIDATA_SOURCE

    def _retriableSPARQLQuery(
        self, theQuery: "str", theDelay: "Optional[float]" = None
    ) -> "SPARQLResult":
        self.logger.debug(f"SPARQL Query {theQuery}")

        retries = 0
        results: "SPARQLResult" = {"results": {}}
        while retries <= self.max_retries:
            retryexc: "Optional[BaseException]" = None
            retrymsg = None
            retrysecs = None

            # https://www.mediawiki.org/w/index.php?title=Topic:V1zau9rqd4ritpug&topic_showPostId=v33czgrn0vmkzwkg#flow-post-v33czgrn0vmkzwkg
            sparql = SPARQLWrapper.SPARQLWrapper(
                self.WIKIDATA_SPARQL_ENDPOINT, agent=self.useragent
            )
            sparql.setRequestMethod(SPARQLWrapper.POSTDIRECTLY)

            sparql.setQuery(theQuery)
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            try:
                results = cast("SPARQLResult", sparql.query().convert())

                # Avoiding to hit the server too fast
                if theDelay is None:
                    theDelay = self.request_delay
                time.sleep(theDelay)

                break
            except SPARQLWrapper.SPARQLExceptions.EndPointInternalError as sqe:
                retryexc = sqe
                retrymsg = "endpoint internal error"

                # Using a backoff time of 2 seconds when 500 or 502 errors are hit
                retrysecs = 2 + 2**retries
            except http.client.IncompleteRead as ir:
                retryexc = ir
                retrymsg = "incomplete read"

                # Using a backoff time of 2 seconds when 500 or 502 errors are hit
                retrysecs = 2 + 2**retries
            except urllib.error.HTTPError as he:
                retryexc = he
                if he.code == 429:
                    retrysecs = he.headers.get("Retry-After")
                    if retrysecs is not None:
                        # We add half a second, as the server sends only the integer part
                        # and some corner 0 seconds cases have happened
                        retrysecs = float(retrysecs) + 0.5
                        retrymsg = "code {}".format(he.code)
                elif he.code == 504:
                    retrymsg = "code {}".format(he.code)

                    # Using a backoff time of 2 seconds when 500 or 502 errors are hit
                    retrysecs = 2 + 2**retries
            except BaseException as be:
                retryexc = be

            retries += 1
            if (retrysecs is not None) and (retries <= self.max_retries):
                self.logger.debug(
                    f"Retry {retries} waits {retrysecs} seconds, due {retrymsg}"
                )

                time.sleep(retrysecs)
            else:
                if retryexc is None:
                    retryexc = Exception("Untraced sparql ERROR")

                self.logger.error("Query with ERROR: " + theQuery)

                raise retryexc
        return results

    def populatePubIdsBatch(self, mappings: "MutableSequence[IdMapping]") -> None:
        populateQuery = """
SELECT	?internal_id
	?internal_idLabel
	?pubmed_id
	?doi_id
	?pmc_id
	(GROUP_CONCAT(?author ; SEPARATOR=";") as ?authors)
	?publication_date
	?journal
WHERE {{
	# The query values will go here
	VALUES (?internal_id) {{
		#(<http://www.wikidata.org/entity/Q38485402>)
{0}
	}}
	# Ignoring the class
	?internal_id wdt:P31 ?internal_id_class .
	OPTIONAL {{ ?internal_id wdt:P698 ?pubmed_id. }}
	OPTIONAL {{ ?internal_id wdt:P356 ?doi_id. }}
	OPTIONAL {{ ?internal_id wdt:P932 ?pmc_id. }}
	OPTIONAL {{ ?internal_id wdt:P2093 ?author. }}
	OPTIONAL {{ ?internal_id wdt:P577 ?publication_date. }}
	OPTIONAL {{
		?internal_id wdt:P1433 ?journal_id.
		?journal_id wdt:P1476 ?journal .
	}}
	SERVICE wikibase:label {{
		bd:serviceParam wikibase:language "en".
	}}
}} GROUP BY ?internal_id ?internal_idLabel ?pubmed_id ?doi_id ?pmc_id ?publication_date ?journal
""".format("\n".join(("\t( <" + mapping["id"] + "> )" for mapping in mappings)))

        results = self._retriableSPARQLQuery(populateQuery)

        internal_ids_dict = {mapping["id"]: mapping for mapping in mappings}
        for result in results["results"]["bindings"]:
            _id = result["internal_id"]["value"]
            mapping = internal_ids_dict.get(_id)
            assert mapping is not None

            titleV = result.get("internal_idLabel")
            mapping["title"] = titleV["value"] if titleV else None

            journalV = result.get("journal")
            mapping["journal"] = journalV["value"] if journalV else None

            pubdateV = result.get("publication_date")
            pubyear = _extractYear(pubdateV["value"]) if pubdateV else None

            mapping["year"] = pubyear

            authorsV = result.get("authors")
            mapping["authors"] = (
                authorsV["value"].split(";")
                if authorsV and authorsV["value"] != ""
                else []
            )

            pubmed_idV = result.get("pubmed_id")
            # Let's sanitize the id
            mapping["pmid"] = pubmed_idV["value"].strip() if pubmed_idV else None

            doi_idV = result.get("doi_id")
            # Let's sanitize the id
            mapping["doi"] = doi_idV["value"].strip() if doi_idV else None

            pmc_idV = result.get("pmc_id")
            # Let's sanitize the id
            mapping["pmcid"] = (
                pub_common.normalize_pmcid(pmc_idV["value"].strip())
                if pmc_idV
                else None
            )

    def queryPubIdsBatch(self, query_ids: "Sequence[QueryId]") -> "Sequence[IdMapping]":
        # Preparing the query ids
        raw_query_pubmed_ids: "MutableSequence[str]" = []
        raw_query_doi_ids: "MutableSequence[str]" = []
        raw_query_pmc_ids: "MutableSequence[str]" = []

        for query_id in query_ids:
            pubmed_id = query_id.get("pmid")
            if pubmed_id is not None:
                raw_query_pubmed_ids.append(str(pubmed_id))

            doi_id_norm = query_id.get("doi")
            if doi_id_norm is not None:
                raw_query_doi_ids.append(doi_id_norm)

            pmc_id_norm = query_id.get("pmcid")
            if pmc_id_norm is not None:
                pmc_id_wikidata = pub_common.denormalize_pmcid(pmc_id_norm)
                raw_query_pmc_ids.append(str(pmc_id_wikidata))

        # Preparing the query by the different ids
        union_query = []
        if raw_query_pubmed_ids:
            raw_query_pubmed = """
		SELECT DISTINCT ?internal_id
		WHERE {{
			VALUES (?query_pubmed_id) {{
				{0}
			}}
			?internal_id wdt:P698 ?query_pubmed_id.
		}}
""".format(
                "\n".join(
                    map(
                        lambda pubmed_id: '("' + pubmed_id.replace('"', '\\"') + '")',
                        raw_query_pubmed_ids,
                    )
                )
            )
            union_query.append(raw_query_pubmed)

        if raw_query_doi_ids:
            raw_query_doi = """
		SELECT DISTINCT ?internal_id
		WHERE {{
			VALUES (?query_doi_id) {{
				{0}
			}}
			?internal_id wdt:P356 ?query_doi_id.
		}}
""".format(
                "\n".join(
                    map(
                        lambda pubmed_id: '("' + pubmed_id.replace('"', '\\"') + '")',
                        raw_query_doi_ids,
                    )
                )
            )
            union_query.append(raw_query_doi)

        if raw_query_pmc_ids:
            raw_query_pmc = """
		SELECT DISTINCT ?internal_id
		WHERE {{
			VALUES (?query_pmc_id) {{
				{0}
			}}
			?internal_id wdt:P932 ?query_pmc_id.
		}}
""".format(
                "\n".join(
                    map(
                        lambda pmc_id: '("' + pmc_id.replace('"', '\\"') + '")',
                        raw_query_pmc_ids,
                    )
                )
            )
            union_query.append(raw_query_pmc)

        if len(union_query) > 0:
            # Now, with the unknown ones, let's ask the server
            if len(union_query) == 1:
                # No additional wrap is needed
                queryQuery = union_query[0]
            else:
                # Prepared the union query
                union_q = "\n\t} UNION {\n".join(union_query)

                queryQuery = """
SELECT	DISTINCT ?internal_id
WHERE {{
	{{
	{0}
	}}
}}
""".format(union_q)

            results = self._retriableSPARQLQuery(queryQuery)

            mapping_ids = [
                result["internal_id"]["value"]
                for result in results["results"]["bindings"]
            ]
        else:
            mapping_ids = []

        # The default return value
        mappings: "MutableSequence[IdMapping]" = []

        if len(mapping_ids) > 0:
            populateQuery = """
SELECT	?internal_id
	?internal_idLabel
	?pubmed_id
	?doi_id
	?pmc_id
	(GROUP_CONCAT(?author ; SEPARATOR=";") as ?authors)
	?publication_date
	?journal
WHERE {{
	# The query values will go here
	VALUES (?internal_id) {{
		#(<http://www.wikidata.org/entity/Q38485402>)
{0}
	}}
	# Ignoring the class
	?internal_id wdt:P31 ?internal_id_class .
	OPTIONAL {{ ?internal_id wdt:P698 ?pubmed_id. }}
	OPTIONAL {{ ?internal_id wdt:P356 ?doi_id. }}
	OPTIONAL {{ ?internal_id wdt:P932 ?pmc_id. }}
	OPTIONAL {{ ?internal_id wdt:P2093 ?author. }}
	OPTIONAL {{ ?internal_id wdt:P577 ?publication_date. }}
	OPTIONAL {{
		?internal_id wdt:P1433 ?journal_id.
		?journal_id wdt:P1476 ?journal .
	}}
	SERVICE wikibase:label {{
		bd:serviceParam wikibase:language "en".
	}}
}} GROUP BY ?internal_id ?internal_idLabel ?pubmed_id ?doi_id ?pmc_id ?publication_date ?journal
""".format("\n".join(("\t( <" + mapping_id + "> )" for mapping_id in mapping_ids)))

            results = self._retriableSPARQLQuery(populateQuery)

            for result in results["results"]["bindings"]:
                mapping: "IdMapping" = {
                    "id": result["internal_id"]["value"],
                    "source": cast("SourceId", self.WIKIDATA_SOURCE),
                }

                titleV = result.get("internal_idLabel")
                mapping["title"] = titleV["value"] if titleV else None

                journalV = result.get("journal")
                mapping["journal"] = journalV["value"] if journalV else None

                pubdateV = result.get("publication_date")
                pubyear = _extractYear(pubdateV["value"]) if pubdateV else None

                mapping["year"] = pubyear

                authorsV = result.get("authors")
                mapping["authors"] = (
                    authorsV["value"].split(";")
                    if authorsV and authorsV["value"] != ""
                    else []
                )

                pubmed_idV = result.get("pubmed_id")
                # Let's sanitize the id
                mapping["pmid"] = pubmed_idV["value"].strip() if pubmed_idV else None

                doi_idV = result.get("doi_id")
                # Let's sanitize the id
                mapping["doi"] = doi_idV["value"].strip() if doi_idV else None

                pmc_idV = result.get("pmc_id")
                # Let's sanitize the id
                mapping["pmcid"] = (
                    pub_common.normalize_pmcid(pmc_idV["value"].strip())
                    if pmc_idV
                    else None
                )

                mappings.append(mapping)

        return mappings

    def queryCitRefsBatch(
        self,
        query_citations_data: "Iterable[IdMappingMinimal]",
        minimal: "bool" = False,
        mode: "int" = 3,
    ) -> "Iterator[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]":
        """
        This method always issues the search for both citations
        and references because it is easier (and it is not
        computationally expensive)
        """
        # First, saving the queries to issue
        results_list: "MutableSequence[Union[GatheredReferences, GatheredCitations, GatheredCitRefs]]" = []
        for query in query_citations_data:
            _id = query["id"]
            # Initializing the values
            result: "GatheredCitRefs" = {  # type: ignore[typeddict-item]
                "id": _id,
                "source": query["source"],
            }
            for citrefs_key, citrefs_count_key in (
                pub_common.REFERENCES_KEYS,
                pub_common.CITATIONS_KEYS,
            ):
                result.setdefault(citrefs_key, [])  # type: ignore[misc]
                result.setdefault(citrefs_count_key, 0)  # type: ignore[misc]

            results_list.append(result)

        def _queryAndProcessCitRefs(
            theQuery: "str",
            theStoreKeys: "Tuple[str, str]",
            results_hash: "MutableMapping[UnqualifiedId, Union[GatheredCitRefs, GatheredCitations, GatheredReferences]]",
        ) -> "None":
            citrefs_key, citrefs_count_key = theStoreKeys

            sparql_results = self._retriableSPARQLQuery(theQuery)

            for res in sparql_results["results"]["bindings"]:
                internal_id = res["internal_id"]["value"]
                citref_id = res["_id"]["value"]

                citref_dateV = res.get("_id_date")
                citref_year = (
                    _extractYear(citref_dateV["value"]) if citref_dateV else None
                )

                result = results_hash[internal_id]
                result[citrefs_key].append(  # type: ignore[literal-required]
                    {
                        "id": citref_id,
                        "source": self.WIKIDATA_SOURCE,
                        "year": citref_year,
                    }
                )
                result[citrefs_count_key] += 1  # type: ignore[literal-required]

        # Second, query by batches
        for start in range(0, len(results_list), self.wikidata_step_size):
            stop = start + self.wikidata_step_size

            results_slice = results_list[start:stop]
            results_slice_hash = {result["id"]: result for result in results_slice}

            raw_ids_slice = [result["id"] for result in results_slice]
            raw_ids_slice_prepared = "\n".join(
                ("\t( <" + _id + "> )" for _id in raw_ids_slice)
            )

            refsQuery = """
SELECT	?internal_id (?ref_id as ?_id) (?ref_publication_date as ?_id_date)
WHERE {{
	# The query values will go here
	VALUES (?internal_id) {{
		#(<http://www.wikidata.org/entity/Q38485402>)
{0}
	}}
	?internal_id wdt:P2860 ?ref_id.
	OPTIONAL {{ ?ref_id wdt:P577 ?ref_publication_date. }}
}}
""".format(raw_ids_slice_prepared)

            # Enrich with the references
            _queryAndProcessCitRefs(
                refsQuery, pub_common.REFERENCES_KEYS, results_slice_hash
            )

            citsQuery = """
SELECT	?internal_id (?cit_id as ?_id) (?cit_publication_date as ?_id_date)
WHERE {{
	# The query values will go here
	VALUES (?internal_id) {{
		#(<http://www.wikidata.org/entity/Q38485402>)
{0}
	}}
	# Ignoring the class
	?cit_id wdt:P2860 ?internal_id.
	OPTIONAL {{ ?cit_id wdt:P577 ?cit_publication_date. }}
}}
""".format(raw_ids_slice_prepared)

            # And now with the citations
            _queryAndProcessCitRefs(
                citsQuery, pub_common.CITATIONS_KEYS, results_slice_hash
            )

            # Emitting the already processed results
            for result_y in results_slice:
                yield result_y
