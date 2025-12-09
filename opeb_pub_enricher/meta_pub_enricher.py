#!/usr/bin/python

from collections import OrderedDict
import copy

import multiprocessing
import traceback

from typing import (
    cast,
    overload,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import configparser
    from types import (
        TracebackType,
    )
    from typing import (
        Any,
        Callable,
        Final,
        Iterable,
        Iterator,
        Literal,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Set,
        Tuple,
        Type,
        Union,
    )

    from .opeb_queries import (
        OpebEntryPub,
    )

    from .pub_cache import (
        Citation,
        CitRefBaseHad,
        GatheredCitations,
        GatheredCitRefs,
        GatheredCitRefStats,
        GatheredReferences,
        IdMapping,
        IdMappingMinimal,
        PartialIdMappingMinimal,
        QueryId,
        Reference,
    )

    from .pub_common import (
        BrokenWinner,
        CURIE,
        EnricherId,
        QualifiedWinner,
        SourceId,
    )

    from .abstract_pub_enricher import (
        ImplementedEnricher,
    )

from .pub_cache import PubDBCache
from .pub_common import (
    CITATIONS_KEYS,
    REFERENCES_KEYS,
)
from .doi_cache import DOIChecker
from .skeleton_pub_enricher import SkeletonPubEnricher
from .europepmc_enricher import EuropePMCEnricher
from .pubmed_enricher import PubmedEnricher
from .wikidata_enricher import WikidataEnricher
from .offline_pubmed_enricher import OfflinePubmedEnricher

from . import pub_common


class MetaEnricherException(Exception):
    def __str__(self) -> "str":
        message, excpairs = self.args

        for enricher, trace in excpairs:
            message += "\n\nEnricher {}. Stack trace:\n{}".format(enricher, trace)

        return str(message)


def _multiprocess_target(
    qr: "multiprocessing.Queue[Tuple[str, Tuple[Any,...]]]",
    qs: "multiprocessing.Queue[Tuple[Any, Optional[str]]]",
    enricher_class: "Type[ImplementedEnricher]",
    *args: "Any",
) -> "None":
    # We are saving either the result or any fired exception
    enricher: "Optional[ImplementedEnricher]" = None
    try:
        enricher = enricher_class(*args)  # type: ignore[abstract]
        qs.put((True, None))
    except BaseException:
        enricher = None
        qs.put((None, traceback.format_exc()))

    while enricher is not None:
        command, params = qr.get()
        retv: "Any" = None
        retv_exc: "Optional[str]" = None
        try:
            method: "Optional[Union[Callable[[Sequence[QueryId]], Sequence[IdMapping]], Callable[[Sequence[OpebEntryPub] | Sequence[IdMapping] | Sequence[BrokenWinner | QualifiedWinner], float, int], Sequence[GatheredCitRefs] | Sequence[GatheredCitRefStats]], Callable[[], SkeletonPubEnricher], Callable[[type[BaseException] | None, BaseException | None, TracebackType | None], Literal[True] | Any | None]]]"
            if command == "cachedQueryPubIds":
                method = enricher.cachedQueryPubIds
            elif command == "listReconcileCitRefMetricsBatch":
                method = enricher.listReconcileCitRefMetricsBatch
            elif command == "enter":
                method = enricher.__enter__
            elif command == "exit":
                method = enricher.__exit__
            elif command == "end":
                method = None
                enricher = None
            else:
                raise NotImplementedError(
                    "command {} is unsupported/unimplemented".format(command)
                )

            if method is not None:
                retv = method(*params)
                if retv is enricher:
                    retv = None
            else:
                retv = True
        except BaseException:
            # It seems it is not possible to pickle exceptions
            retv_exc = traceback.format_exc()
        finally:
            qs.put((retv, retv_exc))


def _multiprocess_wrapper(
    enricher_class: "Type[ImplementedEnricher]", *args: "Any"
) -> "Tuple[multiprocessing.Process, multiprocessing.Queue[Tuple[str, Tuple[Any,...]]], multiprocessing.Queue[Tuple[Any, Optional[str]]]]":
    eqr: "multiprocessing.Queue[Tuple[Any, Optional[str]]]" = multiprocessing.Queue()
    eqs: "multiprocessing.Queue[Tuple[str, Tuple[Any,...]]]" = multiprocessing.Queue()

    ep = multiprocessing.Process(
        daemon=True,
        name=enricher_class.__name__,
        target=_multiprocess_target,
        args=(eqs, eqr, enricher_class, *args),
    )
    ep.start()

    initialization_state = eqr.get()
    # If it could not be initialized, kick out!
    if isinstance(initialization_state, str):
        raise MetaEnricherException(
            "enricher initialization", [(enricher_class.__name__, initialization_state)]
        )

    return (ep, eqs, eqr)


class MetaEnricher(SkeletonPubEnricher):
    RECOGNIZED_BACKENDS: "Final[Sequence[Type[ImplementedEnricher]]]" = [  # type: ignore[valid-type]
        EuropePMCEnricher,
        PubmedEnricher,
        WikidataEnricher,
        OfflinePubmedEnricher,
    ]
    RECOGNIZED_BACKENDS_HASH: "Final[OrderedDict[str, Type[ImplementedEnricher]]]" = (  # type: ignore[valid-type]
        OrderedDict(((backend.Name(), backend) for backend in RECOGNIZED_BACKENDS))  # type: ignore[attr-defined]
    )
    ATTR_BANSET = {
        "id",
        "source",
        "enricher",
        "curie_ids",
        "base_pubs",
        "broken_curie_ids",
        "citation_count",
        "citation_refs",
        "citation_stats",
        "citations",
        "reason",
        "reference_count",
        "reference_refs",
        "reference_stats",
        "references",
    }

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

        if isinstance(cache, str):
            cache_dir = cache
        else:
            cache_dir = cache.cache_dir

        if isinstance(cache, PubDBCache):
            # Try using same checker instance everywhere
            doi_checker = cache.doi_checker
        elif doi_checker is None:
            doi_checker = DOIChecker(cache_dir)

        # The section name is the symbolic name given to this class
        section_name = self.Name()

        # Create the instances needed by this meta enricher
        if config is not None:
            use_enrichers_str = config.get(section_name, "use_enrichers", fallback=None)
        else:
            use_enrichers_str = None

        use_enrichers = (
            use_enrichers_str.split(",")
            if use_enrichers_str
            else self.RECOGNIZED_BACKENDS_HASH.keys()
        )

        enrichers_pool: "OrderedDict[str, Tuple[multiprocessing.Process, multiprocessing.Queue[Tuple[str, Tuple[Any,...]]], multiprocessing.Queue[Tuple[Any, Optional[str]]], str]]" = OrderedDict()
        for enricher_name in use_enrichers:
            enricher_class = self.RECOGNIZED_BACKENDS_HASH.get(enricher_name)
            if enricher_class:
                # Each value is an instance of AbstractPubEnricher
                # enrichers[enricher_name] = enricher_class(cache,prefix,config)
                ep, eqs, eqr = _multiprocess_wrapper(
                    enricher_class,
                    cache_dir,
                    prefix,
                    config,
                    doi_checker,
                    is_db_synchronous,
                )

                enrichers_pool[enricher_name] = (ep, eqs, eqr, enricher_name)

        # And last, the meta-enricher itself
        meta_prefix = prefix + "_" + section_name if prefix else section_name
        meta_prefix += "_" + "-".join(enrichers_pool.keys()) + "_"

        # And the meta-cache
        if isinstance(cache, str):
            pubC = PubDBCache(
                section_name,
                cache_dir=cache_dir,
                prefix=meta_prefix,
                doi_checker=doi_checker,
                is_db_synchronous=is_db_synchronous,
            )
        else:
            pubC = cache

        super().__init__(
            pubC,
            prefix=meta_prefix,
            config=config,
            doi_checker=doi_checker,
            is_db_synchronous=is_db_synchronous,
        )

        self.enrichers_pool = enrichers_pool
        self.entered_enrichers_pool: "OrderedDict[str, Tuple[multiprocessing.Process, multiprocessing.Queue[Tuple[str, Tuple[Any,...]]], multiprocessing.Queue[Tuple[Any, Optional[str]]], str]]" = OrderedDict()

    def __del__(self) -> "None":
        # Try terminating subordinated processes
        if hasattr(self, "enrichers_pool"):
            for eptuple in self.enrichers_pool.values():
                eptuple[0].terminate()

        self.enrichers_pool = OrderedDict()
        self.entered_enrichers_pool = OrderedDict()

    def __enter__(self) -> "MetaEnricher":
        if len(self.entered_enrichers_pool) > 0:
            raise MetaEnricherException("Tried to __enter__ more than once!!!!", [])

        super().__enter__()
        params = ()
        for eptuple in self.enrichers_pool.values():
            eptuple[1].put(("enter", params))

        exc: "MutableSequence[Tuple[str, Any]]" = []
        for enricher_name, eptuple in self.enrichers_pool.items():
            retval, retval_exc = eptuple[2].get()

            if isinstance(retval_exc, str):
                exc.append((eptuple[3], retval_exc))
            else:
                self.entered_enrichers_pool[enricher_name] = eptuple

        if len(exc) > 0:
            self.__del__()
            raise MetaEnricherException("__enter__ nested exception", exc)

        if len(self.entered_enrichers_pool) == 0:
            raise MetaEnricherException(
                f"Unable to __enter__ on any of the nested enrichers: {', '.join(self.enrichers_pool.keys())}",
                exc,
            )

        return self

    def __exit__(
        self,
        exc_type: "Optional[Type[BaseException]]",
        exc_val: "Optional[BaseException]",
        exc_tb: "Optional[TracebackType]",
    ) -> "Union[Literal[True], Any, None]":
        if len(self.entered_enrichers_pool) == 0:
            raise MetaEnricherException("Tried to __exit__ more than once!!!!", [])

        super().__exit__(exc_type, exc_val, exc_tb)
        params = (exc_type, exc_val, exc_tb)
        for eptuple in self.entered_enrichers_pool.values():
            eptuple[1].put(("exit", params))

        for enricher_name, eptuple in self.entered_enrichers_pool.items():
            # Ignore exceptions, as it should happen in __exit__ handlers
            retval, retval_exc = eptuple[2].get()
            if retval_exc is not None:
                self.logger.warning(
                    f"__exit__ nested exception from enricher {enricher_name}: {retval_exc}"
                )

        return self

    # Do not change this constant!!!
    META_SOURCE: "Final[EnricherId]" = cast("EnricherId", "meta")

    @classmethod
    def Name(cls) -> "EnricherId":
        return cls.META_SOURCE

    @classmethod
    def DefaultSource(cls) -> "SourceId":
        return cast("SourceId", "meta")

    # Specific methods
    def _mergeFoundPubs(self, found_pubs: "Sequence[IdMapping]") -> "IdMapping":
        """
        This method takes an array of fetched entries, which have to be merged
        and it returns a merged entry
        """

        merged_pub: "Optional[MutableMapping[str, Any]]" = None
        if found_pubs:
            base_pubs: "MutableSequence[IdMapping]" = []
            base_pubs_set: "Set[Tuple[str, str, str]]" = set()
            initial_curie_ids: "MutableSequence[CURIE]" = []
            putative_ids: "MutableSequence[str]" = []
            # Step 1: initialize features
            merged_pub = {
                "source": self.META_SOURCE,
                "base_pubs": base_pubs,
                "curie_ids": initial_curie_ids,
            }
            for i_found_pub, base_found_pub in enumerate(found_pubs):
                if base_found_pub["id"] is not None:
                    base_found_elem = (
                        base_found_pub["enricher"],
                        base_found_pub["id"],
                        base_found_pub["source"],
                    )
                    if base_found_elem not in base_pubs_set:
                        base_pubs_set.add(base_found_elem)
                        base_pubs.append(
                            {
                                "id": base_found_pub["id"],
                                "source": base_found_pub["source"],
                                "enricher": base_found_pub["enricher"],
                            }
                        )
                    for key, val in base_found_pub.items():
                        # Should we skip specific fields?
                        # This gives a chance to initialize an unknown field
                        if (key in self.ATTR_BANSET) or (val is None):
                            continue

                        # TODO: conflict detection, when a source missets an identifier
                        # Only interesting
                        merged_pub.setdefault(key, val)

            pubmed_id = merged_pub.get("pmid")
            if pubmed_id is not None:
                curie_id = pub_common.pmid2curie(pubmed_id)
                initial_curie_ids.append(curie_id)
                putative_ids.append(pubmed_id)

            doi_id = merged_pub.get("doi")
            if doi_id is not None:
                curie_id = DOIChecker.doi2curie(doi_id)
                initial_curie_ids.append(curie_id)
                doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                putative_ids.append(doi_id_norm)

            pmc_id = merged_pub.get("pmcid")
            if pmc_id is not None:
                curie_id = pub_common.pmcid2curie(pmc_id)
                initial_curie_ids.append(curie_id)
                pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                putative_ids.append(pmc_id_norm)

            # Now use the first putative id
            merged_pub["id"] = putative_ids[0]

            # print("-dbegin-",file=sys.stderr)
            # import json
            # print(json.dumps(merged_pub,indent=4,sort_keys=True),file=sys.stderr)
            # print("-dend-",file=sys.stderr)

        return cast("IdMapping", merged_pub)

    def _mergeFoundPubsList(
        self, merging_list: "Sequence[IdMapping]", keep_empty: "bool" = False
    ) -> "Sequence[IdMapping]":
        """
        This method takes an array of fetched entries, which could be merged
        in several ways, it groups the results, and returns the list of
        merged entries from those groups
        """
        merged_results = []
        if merging_list:
            i2r: "MutableMapping[str, str]" = {}
            i2e: "MutableMapping[str, MutableSequence[IdMapping]]" = {}
            # Cluster by ids
            for merging_elem in merging_list:
                eId = None
                notSet = True

                pubmed_id_v = merging_elem.get("pmid")
                if pubmed_id_v is not None:
                    pubmed_id = str(pubmed_id_v)
                    if pubmed_id in i2r:
                        eId = i2r[pubmed_id]
                    else:
                        eId = pubmed_id
                        i2r[pubmed_id] = eId

                    i2e.setdefault(eId, []).append(merging_elem)
                    notSet = False

                pmc_id = merging_elem.get("pmcid")
                if pmc_id is not None:
                    pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                    if eId is None:
                        if pmc_id_norm in i2r:
                            eId = i2r[pmc_id_norm]
                        else:
                            eId = pmc_id_norm

                    if pmc_id_norm not in i2r:
                        i2r[pmc_id_norm] = eId

                    # Avoid duplications
                    if notSet:
                        i2e.setdefault(eId, []).append(merging_elem)
                        notSet = False

                doi_id = merging_elem.get("doi")
                if doi_id is not None:
                    doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                    if eId is None:
                        if doi_id_norm in i2r:
                            eId = i2r[doi_id_norm]
                        else:
                            eId = doi_id_norm

                    if doi_id_norm not in i2r:
                        i2r[doi_id_norm] = eId

                    # Avoid duplications
                    if notSet:
                        i2e.setdefault(eId, []).append(merging_elem)
                        notSet = False

                # Detecting empty entries to be saved
                # as we know nothing about them (but they exist in some way on source)
                if keep_empty and merging_elem.get("id") is None:
                    merged_results.append(merging_elem)

            # After clustering the results, it is time to merge them
            # Duplicates should have been avoided (if possible)
            for found_pubs in i2e.values():
                merged_pub = self._mergeFoundPubs(found_pubs)
                merged_results.append(merged_pub)

        return merged_results

    def queryPubIdsBatch(self, query_ids: "Sequence[QueryId]") -> "Sequence[IdMapping]":
        if len(self.entered_enrichers_pool) == 0:
            raise MetaEnricherException(
                "Use a 'with' context in order to use 'queryPubIdsBatch' method", []
            )

        # Spawning the work among the jobs
        params = (query_ids,)
        for ep, eqs, eqr, enricher_name in self.entered_enrichers_pool.values():
            # We need the queue to request the results
            eqs.put(("cachedQueryPubIds", params))

        # Now, we gather the work of all threaded enrichers
        merging_list: "MutableSequence[IdMapping]" = []
        exc: "MutableSequence[Tuple[str, str]]" = []
        for ep, eqs, eqr, enricher_name in self.entered_enrichers_pool.values():
            # wait for it
            # The result (or the exception) is in the queue
            gathered_pairs: "Optional[Sequence[IdMapping]]"
            gathered_pairs_exc: "Optional[str]"
            gathered_pairs, gathered_pairs_exc = eqr.get()

            # Kicking up the exception, so it is managed elsewhere
            if isinstance(gathered_pairs_exc, str):
                exc.append((enricher_name, gathered_pairs_exc))
                continue
            else:
                assert gathered_pairs is not None
                # Labelling the results, so we know the enricher
                for gathered_pair in gathered_pairs:
                    gathered_pair["enricher"] = enricher_name

                merging_list.extend(gathered_pairs)

        if len(exc) > 0:
            self.__del__()
            raise MetaEnricherException("queryPubIdsBatch nested exception", exc)

        # and we process it
        merged_results = self._mergeFoundPubsList(merging_list)

        return merged_results

    def populatePubIdsBatch(
        self, partial_mappings: "MutableSequence[IdMapping]"
    ) -> None:
        if partial_mappings:
            raise Exception("FATAL ERROR: Cache miss. Should not happen")

        # Now, time to check and update cache
        partialToBeSearched = []
        i2e: "MutableMapping[str, MutableSequence[int]]" = {}
        for iPartial, partial_mapping in enumerate(partial_mappings):
            cached_mappings = self.pubC.getRawCachedMappingsFromPartial(
                cast("PartialIdMappingMinimal", partial_mapping)
            )

            if cached_mappings:
                # Right now, we are not going to deal with conflicts at this level
                partial_mapping.update(cached_mappings[0])
            else:
                partialToBeSearched.append(partial_mapping)

                # Tracking where to place the results later
                pubmed_id_v = partial_mapping.get("pmid")
                if pubmed_id_v is not None:
                    pubmed_id = str(pubmed_id_v)
                    i2e.setdefault(pubmed_id, []).append(iPartial)

                pmc_id = partial_mapping.get("pmcid")
                if pmc_id is not None:
                    pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                    i2e.setdefault(pmc_id_norm, []).append(iPartial)

                doi_id = partial_mapping.get("doi")
                if doi_id is not None:
                    doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                    i2e.setdefault(doi_id_norm, []).append(iPartial)

        # Update cache with a new search
        if partialToBeSearched:
            rescuedPartials = self.cachedQueryPubIds(
                cast("Sequence[QueryId]", partialToBeSearched)
            )
            for rescuedPartial in rescuedPartials:
                destPlaces: "MutableSequence[int]" = []

                pubmed_id_v = rescuedPartial.get("pmid")
                if pubmed_id_v is not None:
                    destPlaces.extend(i2e.get(str(pubmed_id_v), []))

                pmc_id = rescuedPartial.get("pmcid")
                if pmc_id is not None:
                    pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
                    destPlaces.extend(i2e.get(pmc_id_norm, []))

                doi_id = rescuedPartial.get("doi")
                if doi_id is not None:
                    doi_id_norm = self.doi_checker.normalize_doi(doi_id)
                    destPlaces.extend(i2e.get(doi_id_norm, []))

                # Only the distinct ones
                if destPlaces:
                    destPlaces = list(set(destPlaces))
                    for destPlace in destPlaces:
                        partial_mappings[destPlace] = rescuedPartial

    def _mergeCitRefs(
        self, toBeMergedCitRefs: "Sequence[IdMapping]"
    ) -> "Sequence[IdMapping]":
        # Any reference?
        merged_citRefs: "MutableSequence[IdMapping]" = []
        if toBeMergedCitRefs:
            # Are there empty entries?
            merged_citRefs.extend(
                filter(lambda citRef: citRef.get("id") is None, toBeMergedCitRefs)
            )

            # Are there entries with info?
            if len(merged_citRefs) < len(toBeMergedCitRefs):
                merged_temp_citRefs = self._mergeFoundPubsList(
                    list(
                        filter(
                            lambda citRef: citRef.get("id") is not None,
                            toBeMergedCitRefs,
                        )
                    ),
                    keep_empty=True,
                )

                # Now, time to check and update cache
                citRefsToBeSearched: "MutableSequence[IdMapping]" = []
                citRefsBaseHash: "MutableMapping[str, IdMapping]" = {}
                for citRef in merged_temp_citRefs:
                    # Is it an empty entry?
                    # (should be a redundant mechanism)
                    if citRef.get("id") is None:
                        merged_citRefs.append(citRef)
                        continue

                    cached_citRefs = self.pubC.getRawCachedMappingsFromPartial(
                        cast("PartialIdMappingMinimal", citRef)
                    )

                    if cached_citRefs:
                        # Right now, we are not going to deal with conflicts at this level
                        cached_citRef = cached_citRefs[0]

                        # This is needed to track down the supporting references
                        baseSet = set(
                            (base_pub["enricher"], base_pub["source"], base_pub["id"])
                            for base_pub in citRef["base_pubs"]
                        )

                        # Labelling the seeds of the reference
                        for base_pub in cached_citRef["base_pubs"]:
                            base_pub["had"] = (
                                base_pub["enricher"],
                                base_pub["source"],
                                base_pub["id"],
                            ) in baseSet

                        merged_citRefs.append(cached_citRef)
                    else:
                        citRefsBaseHash[citRef["id"]] = citRef
                        citRefsToBeSearched.append(citRef)

                # Update cache with a new search
                if citRefsToBeSearched:
                    rescuedCitRefs = self.cachedQueryPubIds(
                        cast("Sequence[QueryId]", citRefsToBeSearched)
                    )

                    if rescuedCitRefs:
                        # This is needed to track down the supporting references
                        # There could be some false positive, as we do not track down
                        # fine
                        # print("-dbegin-",file=sys.stderr)
                        # for citRef in citRefsToBeSearched:
                        # if citRef.get('base_pubs') is None:
                        # print("-BLAME-",file=sys.stderr)
                        # print(json.dumps(citRef,indent=4),file=sys.stderr)
                        # print("-/BLAME-",file=sys.stderr)
                        # print(json.dumps(citRefsToBeSearched,indent=4,sort_keys=True),file=sys.stderr)
                        # print("-dwhat-",file=sys.stderr)
                        # print(json.dumps(rescuedCitRefs,indent=4,sort_keys=True),file=sys.stderr)
                        # print("-dend-",file=sys.stderr)
                        baseSet = set(
                            (base_pub["enricher"], base_pub["source"], base_pub["id"])
                            for citRef in citRefsToBeSearched
                            for base_pub in citRef["base_pubs"]
                        )

                        # Now, label those which were tracked
                        for merged_citRef in rescuedCitRefs:
                            for base_pub in merged_citRef["base_pubs"]:
                                base_pub["had"] = (
                                    base_pub["enricher"],
                                    base_pub["source"],
                                    base_pub["id"],
                                ) in baseSet

                        merged_citRefs.extend(rescuedCitRefs)

        return merged_citRefs

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

        This method takes in batches of found publications and retrieves
        citations and / or references from ids
        hitCount: number of times cited
                for each citation it retives
                        id: id of the paper it was cited in
                        source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
                        pubYear: year of publication
                        journalAbbreviation: Journal Abbriviations
        """
        if len(self.entered_enrichers_pool) == 0:
            raise MetaEnricherException(
                "Use a 'with' context in order to use 'queryCitRefsBatch' method", []
            )

        # The publications are clustered by their original enricher
        clustered_pubs: "MutableMapping[str, MutableSequence[Tuple[Union[GatheredCitations, GatheredReferences, GatheredCitRefs], int, int]]]" = {}
        linear_pubs: "MutableSequence[Union[GatheredCitations, GatheredReferences, GatheredCitRefs]]" = []
        linear_id = -1
        for query_pub in query_citations_data:
            # This can happen when no result was found
            if "base_pubs" in query_pub:
                pub = cast(
                    "Union[GatheredCitations, GatheredReferences, GatheredCitRefs]",
                    copy.deepcopy(query_pub),
                )
                linear_pubs.append(pub)
                linear_id += 1
                for i_base_pub, base_pub in enumerate(pub["base_pubs"]):
                    if (
                        base_pub.get("id") is not None
                        and base_pub.get("source") is not None
                    ):
                        clustered_pubs.setdefault(base_pub["enricher"], []).append(
                            (
                                cast(
                                    "Union[GatheredCitations, GatheredReferences, GatheredCitRefs]",
                                    base_pub,
                                ),
                                linear_id,
                                i_base_pub,
                            )
                        )
                    elif base_pub.get("id") is None and base_pub.get("source") is None:
                        # This one should not exist, skip it
                        pass
                    else:
                        self.logger.warning(f"FIXME\n{base_pub}")

        # After clustering, issue the batch calls to each enricher in parallel
        eptuples = []
        for enricher_name, c_base_pubs in clustered_pubs.items():
            eptuple = self.entered_enrichers_pool[enricher_name]

            # Use the verbosity level we need: 1.5
            eptuple[1].put(
                (
                    "listReconcileCitRefMetricsBatch",
                    (list(map(lambda bp: bp[0], c_base_pubs)), 1.5, mode),
                )
            )
            eptuples.append(eptuple)

        # Joining all the threads
        exc = []
        for ep, eqs, eqr, enricher_name in eptuples:
            # The result (or the exception) is in the queue
            possible_result, possible_exception = eqr.get()

            # Kicking up the exception, so it is managed elsewhere
            if isinstance(possible_exception, str):
                exc.append((enricher_name, possible_exception))
                continue

            # As the method enriches the results in place
            # reconcile
            c_base_pubs = clustered_pubs[enricher_name]
            for new_base_pub, bp_ids in zip(
                possible_result, map(lambda bp: bp[1:], c_base_pubs)
            ):
                linear_id = bp_ids[0]
                i_base_pub = bp_ids[1]

                linear_pubs[linear_id]["base_pubs"][i_base_pub] = new_base_pub

        if len(exc) > 0:
            self.__del__()
            raise MetaEnricherException("queryCitRefsBatch nested exception", exc)

        # At last, reconcile!!!!!
        for merged_pub in linear_pubs:
            toBeMergedRefs: "MutableSequence[CitRefBaseHad]" = []
            toBeMergedCits: "MutableSequence[CitRefBaseHad]" = []
            base_pubs = merged_pub["base_pubs"]

            for base_pub in base_pubs:
                enricher_name = base_pub["enricher"]
                # Labelling the citations
                if (mode & 2) != 0:
                    cits_v = base_pub.get("citations")
                    if cits_v:
                        cits = cast("Sequence[CitRefBaseHad]", cits_v)
                        for cit in cits:
                            cit["enricher"] = enricher_name
                            cit["had"] = True
                        toBeMergedCits.extend(cits)

                if (mode & 1) != 0:
                    refs_v = base_pub.get("references")
                    if refs_v:
                        refs = cast("Sequence[CitRefBaseHad]", refs_v)
                        for ref in refs:
                            ref["enricher"] = enricher_name
                            ref["had"] = True
                        toBeMergedRefs.extend(refs)

            # Any reference?
            merged_pub_r = cast("GatheredReferences", merged_pub)
            merged_pub_r["references"] = cast(
                "Sequence[Reference]",
                self._mergeCitRefs(cast("Sequence[IdMapping]", toBeMergedRefs)),
            )
            merged_pub_r["reference_count"] = len(merged_pub_r["references"])
            # Any citation?
            merged_pub_c = cast("GatheredCitations", merged_pub)
            merged_pub_c["citations"] = cast(
                "Sequence[Citation]",
                self._mergeCitRefs(cast("Sequence[IdMapping]", toBeMergedCits)),
            )
            merged_pub_c["citation_count"] = len(merged_pub_c["citations"])

            # After merge, cleanup
            for base_pub in base_pubs:
                for key in *REFERENCES_KEYS, *CITATIONS_KEYS:
                    base_pub.pop(key, None)  # type: ignore[misc]

            # And yield the result
            yield merged_pub


# This is needed for the program itself
DEFAULT_BACKEND = EuropePMCEnricher
RECOGNIZED_BACKENDS_HASH: "OrderedDict[str, Type[ImplementedEnricher]]" = OrderedDict(  # type: ignore[unbound, valid-type]
    MetaEnricher.RECOGNIZED_BACKENDS_HASH
)
RECOGNIZED_BACKENDS_HASH[MetaEnricher.Name()] = MetaEnricher
