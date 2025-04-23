#!/usr/bin/python

import sys
import json
import re
import configparser

from collections import OrderedDict
import copy

import multiprocessing
import traceback

from typing import overload, Tuple, List, Dict, Any, Iterator

from .pub_cache import PubDBCache
from .doi_cache import DOIChecker
from .skeleton_pub_enricher import SkeletonPubEnricher
from .europepmc_enricher import EuropePMCEnricher
from .pubmed_enricher import PubmedEnricher
from .wikidata_enricher import WikidataEnricher

from . import pub_common


class MetaEnricherException(Exception):
	def __str__(self):
		message, excpairs = self.args
		
		for enricher , trace  in excpairs:
			message += "\n\nEnricher {}. Stack trace:\n{}".format(enricher,trace)
		
		return message

def _multiprocess_target(qr, qs, enricher_class, *args):
	# We are saving either the result or any fired exception
	enricher = None
	try:
		enricher = enricher_class(*args)
		qs.put(True)
	except BaseException as e:
		enricher = None
		qs.put(traceback.format_exc())
	
	while enricher is not None:
		command, params = qr.get()
		retv = None
		try:
			if command == 'cachedQueryPubIds':
				method = enricher.cachedQueryPubIds
			elif command == 'listReconcileCitRefMetricsBatch':
				method = enricher.listReconcileCitRefMetricsBatch
			elif command == 'enter':
				method = enricher.__enter__
			elif command == 'exit':
				method = enricher.__exit__
			elif command == 'end':
				method = None
				enricher = None
			else:
				raise NotImplementedError("command {} is unsupported/unimplemented".format(command))
			
			if method is not None:
				retv = method(*params)
				if retv is enricher:
					retv = None
			else:
				retv = True
		except BaseException as e:
			# It seems it is not possible to pickle exceptions
			retv = traceback.format_exc()
		finally:
			qs.put(retv)
	
def _multiprocess_wrapper(enricher_class,*args):
	eqs = multiprocessing.Queue()
	eqr = multiprocessing.Queue()
	
	ep = multiprocessing.Process(
		daemon=True,
		name=enricher_class.__name__,
		target=_multiprocess_target,
		args=(eqs,eqr,enricher_class,*args)
	)
	ep.start()
	
	initialization_state = eqr.get()
	# If it could not be initialized, kick out!
	if isinstance(initialization_state,str):
		raise MetaEnricherException('enricher initialization',[(enricher_class.__name__,initialization_state)])
	
	return (ep,eqs,eqr)

class MetaEnricher(SkeletonPubEnricher):
	RECOGNIZED_BACKENDS = [ EuropePMCEnricher, PubmedEnricher, WikidataEnricher ]
	RECOGNIZED_BACKENDS_HASH = OrderedDict( ( (backend.Name(),backend) for backend in RECOGNIZED_BACKENDS ) )
	ATTR_BANSET = {
		'id',
		'source',
		'enricher',
		'curie_ids',
		'base_pubs',
		'broken_curie_ids',
		'citation_count',
		'citation_refs',
		'citation_stats',
		'citations',
		'reason',
		'reference_count',
		'reference_refs',
		'reference_stats',
		'references',
	}
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		...
	
	@overload
	def __init__(self,cache:PubDBCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		if type(cache) is str:
			cache_dir = cache
		else:
			cache_dir = cache.cache_dir
		
		if isinstance(cache,PubDBCache):
			# Try using same checker instance everywhere
			doi_checker = cache.doi_checker
		elif doi_checker is None:
			doi_checker = DOIChecker(cache_dir)
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		# Create the instances needed by this meta enricher
		use_enrichers_str = config.get(section_name,'use_enrichers',fallback=None)
		
		use_enrichers = use_enrichers_str.split(',')  if use_enrichers_str else self.RECOGNIZED_BACKENDS_HASH.keys()
		
		enrichers_pool = OrderedDict()
		for enricher_name in use_enrichers:
			enricher_class = self.RECOGNIZED_BACKENDS_HASH.get(enricher_name)
			if enricher_class:
				# Each value is an instance of AbstractPubEnricher
				#enrichers[enricher_name] = enricher_class(cache,prefix,config,debug)
				ep, eqs, eqr = _multiprocess_wrapper(enricher_class,cache_dir,prefix,config,debug,doi_checker)
				
				enrichers_pool[enricher_name] = (ep,eqs,eqr,enricher_name)
		
		# And last, the meta-enricher itself
		meta_prefix = prefix + '_' + section_name  if prefix else section_name
		meta_prefix += '_' + '-'.join(enrichers_pool.keys()) + '_'
		
		# And the meta-cache
		if type(cache) is str:
			pubC = PubDBCache(section_name,cache_dir = cache_dir,prefix=meta_prefix,doi_checker=doi_checker)
		else:
			pubC = cache
		
		super().__init__(pubC,meta_prefix,config,debug,doi_checker)
		
		self.enrichers_pool = enrichers_pool
	
	def __del__(self):
		# Try terminating subordinated processes
		if hasattr(self,'enrichers_pool'):
			for eptuple in self.enrichers_pool.values():
				eptuple[0].terminate()
		
		self.enrichers_pool = {}
	
	def __enter__(self):
		super().__enter__()
		params = []
		for eptuple in self.enrichers_pool.values():
			eptuple[1].put(('enter',params))
		
		exc = []
		for eptuple in self.enrichers_pool.values():
			retval = eptuple[2].get()
			
			if isinstance(retval,str):
				exc.append((eptuple[3],retval))
		
		if len(exc) > 0:
			self.__del__()
			raise MetaEnricherException("__enter__ nested exception",exc)
		
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		super().__exit__(exc_type, exc_val, exc_tb)
		params = [exc_type, exc_val, exc_tb]
		for eptuple in self.enrichers_pool.values():
			eptuple[1].put(('exit',params))
		
		for eptuple in self.enrichers_pool.values():
			# Ignore exceptions, as it should happen in __exit__ handlers
			retval = eptuple[2].get()
		
		return self
	
	# Do not change this constant!!!
	META_SOURCE='meta'
	
	@classmethod
	def Name(cls) -> str:
		return cls.META_SOURCE
	
	# Specific methods
	def _mergeFoundPubs(self,found_pubs:List[Dict[str,Any]]) -> Dict[str,Any]:
		"""
			This method takes an array of fetched entries, which have to be merged
			and it returns a merged entry
		"""
		
		merged_pub = None
		if found_pubs:
			base_pubs = []
			base_pubs_set = set()
			initial_curie_ids = []
			putative_ids = []
			# Step 1: initialize features
			merged_pub = {
				'source': self.META_SOURCE,
				'base_pubs': base_pubs,
				'curie_ids': initial_curie_ids,
			}
			for i_found_pub,base_found_pub in enumerate(found_pubs):
				if base_found_pub['id'] is not None:
					base_found_elem = (base_found_pub['enricher'], base_found_pub['id'], base_found_pub['source'])
					if base_found_elem not in base_pubs_set:
						base_pubs_set.add(base_found_elem)
						base_pubs.append({'id': base_found_pub['id'],'source': base_found_pub['source'],'enricher': base_found_pub['enricher']})
					for key,val in base_found_pub.items():
						# Should we skip specific fields?
						# This gives a chance to initialize an unknown field
						if (key in self.ATTR_BANSET) or (val is None):
							continue
						
						# TODO: conflict detection, when a source missets an identifier
						# Only interesting
						merged_pub.setdefault(key,val)
			
			pubmed_id = merged_pub.get('pmid')
			if pubmed_id is not None:
				curie_id = pub_common.pmid2curie(pubmed_id)
				initial_curie_ids.append(curie_id)
				putative_ids.append(pubmed_id)
			
			doi_id = merged_pub.get('doi')
			if doi_id is not None:
				curie_id = DOIChecker.doi2curie(doi_id)
				initial_curie_ids.append(curie_id)
				doi_id_norm = self.doi_checker.normalize_doi(doi_id)
				putative_ids.append(doi_id_norm)
			
			pmc_id = merged_pub.get('pmcid')
			if pmc_id is not None:
				curie_id = pub_common.pmcid2curie(pmc_id)
				initial_curie_ids.append(curie_id)
				pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
				putative_ids.append(pmc_id_norm)
			
			# Now use the first putative id
			merged_pub['id'] = putative_ids[0]
			
			#print("-dbegin-",file=sys.stderr)
			#import json
			#print(json.dumps(merged_pub,indent=4,sort_keys=True),file=sys.stderr)
			#print("-dend-",file=sys.stderr)
			
		return merged_pub
	
	def _mergeFoundPubsList(self,merging_list:List[Dict[str,Any]],keep_empty:bool=False) -> List[Dict[str,Any]]:
		"""
			This method takes an array of fetched entries, which could be merged
			in several ways, it groups the results, and returns the list of
			merged entries from those groups
		"""
		merged_results = []
		if merging_list:
			i2r = {}
			i2e = {}
			# Cluster by ids
			for merging_elem in merging_list:
				eId = None
				notSet = True
				
				pubmed_id = merging_elem.get('pmid')
				if pubmed_id is not None:
					if pubmed_id in i2r:
						eId = i2r[pubmed_id]
					else:
						eId = pubmed_id
						i2r[pubmed_id] = eId
					
					i2e.setdefault(eId,[]).append(merging_elem)
					notSet = False
				
				pmc_id = merging_elem.get('pmcid')
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
						i2e.setdefault(eId,[]).append(merging_elem)
						notSet = False
				
				doi_id = merging_elem.get('doi')
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
						i2e.setdefault(eId,[]).append(merging_elem)
						notSet = False
				
				# Detecting empty entries to be saved
				# as we know nothing about them (but they exist in some way on source)
				if keep_empty and merging_elem.get('id') is None:
					merged_results.append(merging_elem)
				
			# After clustering the results, it is time to merge them
			# Duplicates should have been avoided (if possible)
			for found_pubs in i2e.values():
				merged_pub = self._mergeFoundPubs(found_pubs)
				merged_results.append(merged_pub)
		
		return merged_results
	
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		# Spawning the work among the jobs
		params = [query_ids]
		for ep, eqs, eqr, enricher_name in self.enrichers_pool.values():
			# We need the queue to request the results
			eqs.put(('cachedQueryPubIds',params))
		
		# Now, we gather the work of all threaded enrichers
		merging_list = []
		exc = []
		for ep, eqs, eqr, enricher_name  in self.enrichers_pool.values():
			# wait for it
			# The result (or the exception) is in the queue
			gathered_pairs = eqr.get()
			
			# Kicking up the exception, so it is managed elsewhere
			if isinstance(gathered_pairs, str):
				exc.append((enricher_name,gathered_pairs))
				continue
			
			# Labelling the results, so we know the enricher
			for gathered_pair in gathered_pairs:
				gathered_pair['enricher'] = enricher_name
			
			merging_list.extend(gathered_pairs)
		
		if len(exc) > 0:
			self.__del__()
			raise MetaEnricherException('queryPubIdsBatch nested exception',exc)
		
		# and we process it
		merged_results = self._mergeFoundPubsList(merging_list)
		
		return merged_results
	
	def populatePubIdsBatch(self,partial_mappings:List[Dict[str,Any]]) -> None:
		if partial_mappings:
			raise Exception('FATAL ERROR: Cache miss. Should not happen')
	
		# Now, time to check and update cache
		partialToBeSearched = []
		i2e = {}
		for iPartial, partial_mapping in enumerate(partial_mappings):
			cached_mappings = self.pubC.getRawCachedMappingsFromPartial(partial_mapping)
			
			if cached_mappings:
				# Right now, we are not going to deal with conflicts at this level
				partial_mapping.update(cached_mappings[0])
			else:
				partialToBeSearched.append(partial_mapping)
				
				# Tracking where to place the results later
				pubmed_id = partial_mapping.get('pmid')
				if pubmed_id is not None:
					i2e.setdefault(pubmed_id,[]).append(iPartial)
				
				pmc_id = partial_mapping.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					i2e.setdefault(pmc_id_norm,[]).append(iPartial)
				
				doi_id = partial_mapping.get('doi')
				if doi_id is not None:
					doi_id_norm = self.doi_checker.normalize_doi(doi_id)
					i2e.setdefault(doi_id_norm,[]).append(iPartial)
		
		# Update cache with a new search
		if partialToBeSearched:
			rescuedPartials = self.cachedQueryPubIds(partialToBeSearched)
			for rescuedPartial in rescuedPartials:
				destPlaces = []
				
				pubmed_id = rescuedPartial.get('pmid')
				if pubmed_id is not None:
					destPlaces.extend(i2e.get(pubmed_id,[]))
				
				pmc_id = rescuedPartial.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					destPlaces.extend(i2e.get(pmc_id_norm,[]))
				
				doi_id = rescuedPartial.get('doi')
				if doi_id is not None:
					doi_id_norm = self.doi_checker.normalize_doi(doi_id)
					destPlaces.extend(i2e.get(doi_id_norm,[]))
				
				# Only the distinct ones
				if destPlaces:
					destPlaces = list(set(destPlaces))
					for destPlace in destPlaces:
						partial_mappings[destPlace] = rescuedPartial
	
	def _mergeCitRefs(self,toBeMergedCitRefs:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		# Any reference?
		merged_citRefs = []
		if toBeMergedCitRefs:
			# Are there empty entries?
			merged_citRefs.extend(filter(lambda citRef: citRef.get('id') is None, toBeMergedCitRefs))
			
			# Are there entries with info?
			if len(merged_citRefs) < len(toBeMergedCitRefs):
				merged_temp_citRefs = self._mergeFoundPubsList(list(filter(lambda citRef: citRef.get('id') is not None, toBeMergedCitRefs)),keep_empty=True)
				
				# Now, time to check and update cache
				citRefsToBeSearched = []
				citRefsBaseHash = {}
				for citRef in merged_temp_citRefs:
					# Is it an empty entry?
					# (should be a redundant mechanism)
					if citRef.get('id') is None:
						merged_citRefs.append(citRef)
						continue
					
					cached_citRefs = self.pubC.getRawCachedMappingsFromPartial(citRef)
					
					if cached_citRefs:
						# Right now, we are not going to deal with conflicts at this level
						cached_citRef = cached_citRefs[0]
						
						# This is needed to track down the supporting references
						baseSet = set((base_pub['enricher'],base_pub['source'],base_pub['id'])  for base_pub in citRef['base_pubs'])
						
						# Labelling the seeds of the reference
						for base_pub in cached_citRef['base_pubs']:
							base_pub['had'] = (base_pub['enricher'],base_pub['source'],base_pub['id']) in baseSet
						
						merged_citRefs.append(cached_citRef)
					else:
						citRefsBaseHash[citRef['id']] = citRef
						citRefsToBeSearched.append(citRef)
				
				# Update cache with a new search
				if citRefsToBeSearched:
					rescuedCitRefs = self.cachedQueryPubIds(citRefsToBeSearched)
					
					if rescuedCitRefs:
						# This is needed to track down the supporting references
						# There could be some false positive, as we do not track down
						# fine
						#print("-dbegin-",file=sys.stderr)
						#for citRef in citRefsToBeSearched:
						#	if citRef.get('base_pubs') is None:
						#		print("-BLAME-",file=sys.stderr)
						#		print(json.dumps(citRef,indent=4),file=sys.stderr)
						#		print("-/BLAME-",file=sys.stderr)
						#print(json.dumps(citRefsToBeSearched,indent=4,sort_keys=True),file=sys.stderr)
						#print("-dwhat-",file=sys.stderr)
						#print(json.dumps(rescuedCitRefs,indent=4,sort_keys=True),file=sys.stderr)
						#print("-dend-",file=sys.stderr)
						baseSet = set((base_pub['enricher'],base_pub['source'],base_pub['id'])  for citRef in citRefsToBeSearched  for base_pub in citRef['base_pubs'])
						
						# Now, label those which were tracked
						for merged_citRef in rescuedCitRefs:
							for base_pub in merged_citRef['base_pubs']:
								base_pub['had'] = (base_pub['enricher'],base_pub['source'],base_pub['id']) in baseSet
						
						merged_citRefs.extend(rescuedCitRefs)
		
		return merged_citRefs
		
	
	def queryCitRefsBatch(self,query_citations_data:Iterator[Dict[str,Any]],minimal:bool=False,mode:int=3) -> Iterator[Dict[str,Any]]:
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
		
		# The publications are clustered by their original enricher
		clustered_pubs = {}
		linear_pubs = []
		linear_id = -1
		for query_pub in query_citations_data:
			# This can happen when no result was found
			if 'base_pubs' in query_pub:
				pub = copy.deepcopy(query_pub)
				linear_pubs.append(pub)
				linear_id += 1
				for i_base_pub, base_pub in enumerate(pub['base_pubs']):
					if base_pub.get('id') is not None and base_pub.get('source') is not None:
						clustered_pubs.setdefault(base_pub['enricher'],[]).append((base_pub,linear_id,i_base_pub))
					elif base_pub.get('id') is None and base_pub.get('source') is None:
						# This one should not exist, skip it
						pass
					else:
						print('FIXME',file=sys.stderr)
						print(base_pub,file=sys.stderr)
						sys.stderr.flush()
		
		# After clustering, issue the batch calls to each enricher in parallel
		eptuples = []
		for enricher_name, c_base_pubs in clustered_pubs.items():
			eptuple = self.enrichers_pool[enricher_name]

			# Use the verbosity level we need: 1.5
			eptuple[1].put(('listReconcileCitRefMetricsBatch',[list(map(lambda bp: bp[0], c_base_pubs)),1.5,mode]))
			eptuples.append(eptuple)
		
		# Joining all the threads
		exc = []
		for ep, eqs, eqr, enricher_name in eptuples:
			# The result (or the exception) is in the queue
			possible_exception = eqr.get()
			
			# Kicking up the exception, so it is managed elsewhere
			if isinstance(possible_exception, str):
				exc.append((enricher_name,possible_exception))
				continue
			
			# As the method enriches the results in place
			# reconcile
			c_base_pubs = clustered_pubs[enricher_name]
			for new_base_pub, bp_ids in zip(possible_exception,map(lambda bp: bp[1:],c_base_pubs)):
				linear_id = bp_ids[0]
				i_base_pub = bp_ids[1]
				
				linear_pubs[linear_id]['base_pubs'][i_base_pub] = new_base_pub
		
		if len(exc) > 0:
			self.__del__()
			raise MetaEnricherException('queryCitRefsBatch nested exception',exc)
		
		# At last, reconcile!!!!!
		for merged_pub in linear_pubs:
			toBeMergedRefs = []
			toBeMergedCits = []
			base_pubs = merged_pub['base_pubs']
			
			for base_pub in base_pubs:
				enricher_name = base_pub['enricher']
				# Labelling the citations
				if (mode & 2) != 0:
					cits = base_pub.get('citations')
					if cits:
						for cit in cits:
							cit['enricher'] = enricher_name
							cit['had'] = True
						toBeMergedCits.extend(cits)

				if (mode & 1) != 0:
					refs = base_pub.get('references')
					if refs:
						for ref in refs:
							ref['enricher'] = enricher_name
							ref['had'] = True
						toBeMergedRefs.extend(refs)
			
			# Any reference?
			merged_pub['references'] = self._mergeCitRefs(toBeMergedRefs)
			merged_pub['reference_count'] = len(merged_pub['references'])
			# Any citation?
			merged_pub['citations'] = self._mergeCitRefs(toBeMergedCits)
			merged_pub['citation_count'] = len(merged_pub['citations'])
			
			# After merge, cleanup
			for base_pub in base_pubs:
				for key in 'references','reference_count','citations','citation_count':
					base_pub.pop(key,None)
			
			# And yield the result
			yield merged_pub

# This is needed for the program itself
DEFAULT_BACKEND = EuropePMCEnricher
RECOGNIZED_BACKENDS_HASH = OrderedDict( MetaEnricher.RECOGNIZED_BACKENDS_HASH )
RECOGNIZED_BACKENDS_HASH[ MetaEnricher.Name() ] = MetaEnricher
