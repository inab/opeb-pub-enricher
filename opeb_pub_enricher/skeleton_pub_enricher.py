#!/usr/bin/python

import sys
import os
import json
import configparser
import copy

from urllib import request
from urllib.error import *
import http,socket

import datetime
import time

from abc import ABC, abstractmethod

from typing import overload, Tuple, List, Dict, Any, Iterator

from .pub_cache import PubDBCache
from .doi_cache import DOIChecker

from . import pub_common

#import threading
#import gc
#
#GC_THRESHOLD = 180
#def periodic_gc():
#	while True:
#		time.sleep(GC_THRESHOLD)
#		gc.collect()
#		print('----GC----',file=sys.stderr)
#
#gc_thread = threading.Thread(target=periodic_gc, name='Pub-GC', daemon=True)
#gc_thread.start()

#def get_size(obj, seen=None):
#    """Recursively finds size of objects"""
#    size = sys.getsizeof(obj)
#    if seen is None:
#        seen = set()
#    obj_id = id(obj)
#    if obj_id in seen:
#        return 0
#    # Important mark as seen *before* entering recursion to gracefully handle
#    # self-referential objects
#    seen.add(obj_id)
#    if isinstance(obj, dict):
#        size += sum([get_size(v, seen) for v in obj.values()])
#        size += sum([get_size(k, seen) for k in obj.keys()])
#    elif hasattr(obj, '__dict__'):
#        size += get_size(obj.__dict__, seen)
#    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
#        size += sum([get_size(i, seen) for i in obj])
#    return size

class SkeletonPubEnricher(ABC):
	DEFAULT_STEP_SIZE = 50
	DEFAULT_NUM_FILES_PER_DIR = 1000
	DEFAULT_MAX_RETRIES = 5
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		...
	
	@overload
	def __init__(self,cache:PubDBCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		if isinstance(cache,PubDBCache):
			# Try using same checker instance everywhere
			self.cache_dir = cache.cache_dir
			doi_checker = cache.doi_checker
		elif doi_checker is not None:
			self.cache_dir = doi_checker.cache_dir
		else:
			self.cache_dir = cache
			doi_checker = DOIChecker(self.cache_dir)
		
		self.doi_checker = doi_checker
		
		if type(cache) is str:
			cache_prefix = prefix + '_' + section_name  if prefix else section_name
			cache_prefix += '_'
			
			self.pubC = PubDBCache(section_name,cache_dir = self.cache_dir,prefix=cache_prefix,doi_checker=doi_checker)
		else:
			self.pubC = cache
		
		# Load at least a config parser
		self.config = config if config else configparser.ConfigParser()
		
		# Adding empty sections, in order to avoid the NoSectionError exception
		if not self.config.has_section(section_name):
			self.config.add_section(section_name)
		
		self.step_size = self.config.getint(section_name,'step_size',fallback=self.DEFAULT_STEP_SIZE)
		self.num_files_per_dir = self.config.getint(section_name,'num_files_per_dir',fallback=self.DEFAULT_NUM_FILES_PER_DIR)
		
		# Maximum number of retries
		self.max_retries = self.config.getint(section_name,'retries',fallback=self.DEFAULT_MAX_RETRIES)
		
		# Debug flag
		self._debug = debug
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		# The json encoder and decoder instances
		self.je = json.JSONEncoder(indent=4,sort_keys=True)
		self.jd = json.JSONDecoder()
		
		super().__init__()
	
	def __enter__(self):
		self.pubC.__enter__()
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.pubC.__exit__(exc_type, exc_val, exc_tb)
	
	@classmethod
	@abstractmethod
	def Name(cls) -> str:
		return 'skel'
	
	@abstractmethod
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		pass
	
	def cachedQueryPubIds(self,query_list:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		"""
			Caching version of queryPubIdsBatch.
			Order is not guaranteed
		"""
		# First, gather all the ids on one list, prepared for the query
		# MED: prefix has been removed because there are some problems
		# on the server side
		
		q2e = set()
		r2e = set()
		result_array = []
		
		def _prefetchCaches(publish_id:str) -> bool:
			# This one tells us the result was already got
			if publish_id in q2e:
				return True
			
			# This one signals the result is not in the cache
			internal_ids = self.pubC.getSourceIds(publish_id)
			if internal_ids is None:
				return False
			
			# Let's dig in the cached results
			validMappings = False
			mappings = [ ]
			results = [ ]
			
			for source_id_pair in internal_ids:
				# If the result is correct, skip
				if source_id_pair in r2e:
					validMappings = True
					continue
				
				mapping = self.pubC.getCachedMapping(*source_id_pair)
				
				# If some of the mappings has been invalidated,
				# complain
				if mapping is None:
					return False
				
				# If none of the mapping elements expired, register all!
				validMappings = True
				mappings.append(mapping)
				results.append(source_id_pair)
			
			# Now all the cached elements have passed the filter
			# let's register all the values
			if validMappings:
				q2e.add(publish_id)
				if len(results) > 0:
					r2e.union(*results)
					result_array.extend(mappings)
			
			return validMappings
		
		# Preparing the query ids
		query_ids = []
		# This set allows avoiding to issue duplicate queries
		set_query_ids = set()
		for query in query_list:
			query_id = {}
			
			# This loop avoid resolving twice
			pubmed_id = query.get('pmid')
			if pubmed_id is not None and not _prefetchCaches(pubmed_id):
				pubmed_set_id = (pubmed_id,'pmid')
				set_query_ids.add(pubmed_set_id)
				query_id['pmid'] = pubmed_id
			
			doi_id = query.get('doi')
			if doi_id is not None:
				doi_id_norm = self.doi_checker.normalize_doi(doi_id)
				if not _prefetchCaches(doi_id_norm):
					doi_set_id = (doi_id_norm,'doi')
					set_query_ids.add(doi_set_id)
					query_id['doi'] = doi_id_norm
			
			pmc_id = query.get('pmcid')
			if pmc_id is not None:
				pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
				if not _prefetchCaches(pmc_id_norm):
					pmc_set_id = (pmc_id_norm,'pmcid')
					set_query_ids.add(pmc_set_id)
					query_id['pmcid'] = pmc_id_norm
			
			# Add it when there is something to query about
			if len(query_id) > 0:
				query_ids.append(query_id)
		
		# Now, with the unknown ones, let's ask the server
		if len(query_ids) > 0:
			try:
				# Needed to not overwhelm the underlying implementation
				for start in range(0,len(query_ids),self.step_size):
					stop = start+self.step_size
					query_ids_slice = query_ids[start:stop]
					
					gathered_pubmed_pairs = self.queryPubIdsBatch(query_ids_slice)
					
					if gathered_pubmed_pairs:
						for mapping in gathered_pubmed_pairs:
							# Cache management
							self.pubC.setCachedMapping(mapping)
							
						# Result management
						result_array.extend(gathered_pubmed_pairs)
			except Exception as anyEx:
				print("Something unexpected happened in cachedQueryPubIds",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				import traceback
				traceback.print_exc(file=sys.stderr)
				sys.stderr.flush()
				raise anyEx
		
		return result_array
		
		
	def reconcilePubIdsBatch(self,entries:List[Any]) -> None:
		# First, gather all the ids on one list, prepared for the query
		# MED: prefix has been removed because there are some problems
		# on the server side
		
		p2e = {}
		pmc2e = {}
		d2e = {}
		pubmed_pairs = []
		
		def _updateCaches(publish_id:str) -> bool:
			internal_ids = self.pubC.getSourceIds(publish_id)
			if internal_ids is not None:
				validMappings = 0
				for source_id,_id in internal_ids:
					mapping = self.pubC.getCachedMapping(source_id,_id)
					# If the mapping did not expire, register it!
					if mapping is not None:
						validMappings += 1
						pubmed_pairs.append(mapping)
						
						pubmed_id = mapping.get('pmid')
						if pubmed_id is not None:
							p2e.setdefault(pubmed_id,{})[source_id] = mapping
						
						doi_id = mapping.get('doi')
						if doi_id is not None:
							doi_id_norm = self.doi_checker.normalize_doi(doi_id)
							d2e.setdefault(doi_id_norm,{})[source_id] = mapping
						
						pmc_id = mapping.get('pmcid')
						if pmc_id is not None:
							pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
							pmc2e.setdefault(pmc_id_norm,{})[source_id] = mapping
				
				return validMappings > 0
			else:
				return False
		
		# Preparing the query ids
		query_ids = []
		# This set allows avoiding to issue duplicate queries
		set_query_ids = set()
		for entry_pubs in map(lambda entry: entry['entry_pubs'],entries):
			for entry_pub in entry_pubs:
				query_id = {}
				# This loop avoid resolving twice
				pubmed_id = entry_pub.get('pmid')
				pubmed_set_id = (pubmed_id,'pmid')
				if pubmed_id is not None and pubmed_set_id not in set_query_ids and pubmed_id not in p2e:
					if not _updateCaches(pubmed_id):
						set_query_ids.add(pubmed_set_id)
						query_id['pmid'] = pubmed_id
						
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					doi_id_norm = self.doi_checker.normalize_doi(doi_id)
					doi_set_id = (doi_id_norm,'doi')
					if doi_set_id not in set_query_ids and doi_id_norm not in d2e and not _updateCaches(doi_id_norm):
						set_query_ids.add(doi_set_id)
						query_id['doi'] = doi_id_norm
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					pmc_set_id = (pmc_id_norm,'pmcid')
					if pmc_set_id not in set_query_ids and pmc_id_norm not in pmc2e and not _updateCaches(pmc_id_norm):
						set_query_ids.add(pmc_set_id)
						query_id['pmcid'] = pmc_id_norm
				
				# Add it when there is something to query about
				if len(query_id) > 0:
					query_ids.append(query_id)
		
		# Now, with the unknown ones, let's ask the server
		if len(query_ids) > 0:
			try:
				gathered_pubmed_pairs = self.queryPubIdsBatch(query_ids)
				
				# Cache management
				for mapping in gathered_pubmed_pairs:
					_id = mapping['id']
					source_id = mapping['source']
					self.pubC.setCachedMapping(mapping)
					
					pubmed_id = mapping.get('pmid')
					if pubmed_id is not None:
						p2e.setdefault(pubmed_id,{})[source_id] = mapping
					
					pmc_id = mapping.get('pmcid')
					if pmc_id is not None:
						pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
						pmc2e.setdefault(pmc_id_norm,{})[source_id] = mapping
					
					doi_id = mapping.get('doi')
					if doi_id is not None:
						doi_id_norm = self.doi_checker.normalize_doi(doi_id)
						d2e.setdefault(doi_id_norm,{})[source_id] = mapping
					
					pubmed_pairs.append(mapping)

					# print(json.dumps(entries,indent=4))
				# sys.exit(1)
			except Exception as anyEx:
				print("Something unexpected happened in reconcilePubIdsBatch",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				import traceback
				traceback.print_exc(file=sys.stderr)
				sys.stderr.flush()
				raise anyEx
		
		# Reconciliation and checking missing ones
		for entry in entries:
			for entry_pub in entry['entry_pubs']:
				broken_curie_ids = []
				initial_curie_ids = []
				
				results = []
				pubmed_id = entry_pub.get('pmid')
				if pubmed_id is not None:
					curie_id = pub_common.pmid2curie(pubmed_id)
					initial_curie_ids.append(curie_id)
					if pubmed_id in p2e:
						results.append(p2e[pubmed_id])
					else:
						broken_curie_ids.append(curie_id)
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					curie_id = DOIChecker.doi2curie(doi_id)
					initial_curie_ids.append(curie_id)
					doi_id_norm = self.doi_checker.normalize_doi(doi_id)
					if doi_id_norm in d2e:
						results.append(d2e[doi_id_norm])
					else:
						broken_curie_ids.append(curie_id)
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None:
					curie_id = pub_common.pmcid2curie(pmc_id)
					initial_curie_ids.append(curie_id)
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					if pmc_id_norm in pmc2e:
						results.append(pmc2e[pmc_id_norm])
					else:
						broken_curie_ids.append(curie_id)
				
				# Checking all the entries at once
				winner_set = None
				notFound = len(results) == 0
				for result in results:
					if winner_set is None:
						winner_set = result
					elif winner_set != result:
						winner = None
						break
				
				winners = []
				if winner_set is not None:
					for winner in iter(winner_set.values()):
						# Duplicating in order to augment it
						new_winner = copy.deepcopy(winner)
						
						curie_ids = []
						
						pubmed_id = new_winner.get('pmid')
						if pubmed_id is not None:
							curie_id = pub_common.pmid2curie(pubmed_id)
							curie_ids.append(curie_id)
						
						doi_id = new_winner.get('doi')
						if doi_id is not None:
							curie_id = DOIChecker.doi2curie(doi_id)
							curie_ids.append(curie_id)
						
						pmc_id = new_winner.get('pmcid')
						if pmc_id is not None:
							curie_id = pub_common.pmcid2curie(pmc_id)
							curie_ids.append(curie_id)
						
						new_winner['curie_ids'] = curie_ids
						new_winner['broken_curie_ids'] = broken_curie_ids
						winners.append(new_winner)
				else:
					broken_winner = {
						'id': None,
						'source': None,
						'curie_ids': initial_curie_ids,
						'broken_curie_ids': broken_curie_ids,
						'pmid': pubmed_id,
						'doi': doi_id,
						'pmcid': pmc_id
					}
					# No possible result
					if notFound:
						broken_winner['reason'] = 'notFound' if len(initial_curie_ids) > 0  else 'noReference'
					# There were mismatches
					else:
						broken_winner['reason'] = 'mismatch'
					
					winners.append(broken_winner)
				
				entry_pub['found_pubs'].extend(winners)
	
	
	@abstractmethod
	def queryCitRefsBatch(self,query_citations_data:Iterator[Dict[str,Any]],minimal:bool=False,mode:int=3) -> Iterator[Dict[str,Any]]:
		"""
		query_citations_data: An iterator of dictionaries with at least two keys: source and id
		minimal: Whether the list of citations and references is "minimal" (minimizing the number of queries) or not
		mode: 1 means only references, 2 means only citations, and 3 means both
		
		The results from the returned iterator do not maintain the same order as the queries from the input one
		"""
		pass
	
	def clusteredSearchCitRefsBatch(self,query_citations_data:List[Dict[str,Any]],query_hash,minimal:bool=False,mode:int=3) -> None:
		# Update the cache with the new data
		if len(query_citations_data) > 0:
			try:
				new_citrefs = self.queryCitRefsBatch(query_citations_data,minimal,mode)
			except Exception as anyEx:
				print("ERROR: Something went wrong",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				raise anyEx
			
			for new_citref in new_citrefs:
				source_id = new_citref['source']
				_id = new_citref['id']
				
				if (mode & 2) != 0:
					if 'citations' in new_citref:
						citations = new_citref['citations']
						citation_count = new_citref['citation_count']
						# There are cases where no citation could be fetched
						# but it should also be cached
						self.pubC.setCitationsAndCount(source_id,_id,citations,citation_count)
						for pub_field in query_hash[(_id,source_id)]:
							pub_field['citation_count'] = citation_count
							pub_field['citations'] = citations
				
				if (mode & 1) != 0:
					if 'references' in new_citref:
						references = new_citref['references']
						reference_count = new_citref['reference_count']
						# There are cases where no reference could be fetched
						# but it should also be cached
						self.pubC.setReferencesAndCount(source_id,_id,references,reference_count)
						for pub_field in query_hash[(_id,source_id)]:
							pub_field['reference_count'] = reference_count
							pub_field['references'] = references
	
	def listReconcileCitRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0,mode:int=3) -> List[Dict[str,Any]]:
		"""
			This method takes in batches of found publications and it retrieves citations from ids
			hitCount: number of times cited
				for each citation it retrieves
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbreviations
		"""
		
		query_citations_data = []
		query_hash = {}
		for pub_field in pub_list:
			_id = pub_field.get('id') #11932250
			if _id is not None:
				source_id = pub_field['source']
				
				if ( mode & 2 ) != 0:
					citations, citation_count = self.pubC.getCitationsAndCount(source_id,_id)
					if citation_count is not None:
						# Save now
						pub_field['citation_count'] = citation_count
						pub_field['citations'] = citations

				if ( mode & 1 ) != 0:
					references, reference_count = self.pubC.getReferencesAndCount(source_id,_id)
					if reference_count is not None:
						# Save now
						pub_field['reference_count'] = reference_count
						pub_field['references'] = references
				
				# Query later, without repetitions
				if ((mode & 2) != 0 and (citation_count is None)) or ((mode & 1) != 0 and (reference_count is None)):
					query_list = query_hash.setdefault((_id,source_id),[])
					if len(query_list) == 0:
						query_citations_data.append(pub_field)
					query_list.append(pub_field)
		
		minimal = verbosityLevel == -1
		# Update the cache with the new data
		self.clusteredSearchCitRefsBatch(query_citations_data,query_hash,minimal,mode)
		
		# If we have to return the digested stats, compute them here
		if verbosityLevel > -1 and verbosityLevel<=0:
			for pub_field in pub_list:
				if (mode & 2) != 0:
					citations = pub_field.pop('citations',None)
					# Computing the stats
					pub_field['citation_stats'] = None  if citations is None else self._citrefStats(citations)
				
				if (mode & 1) != 0:
					references = pub_field.pop('references',None)
					# Computing the stats
					pub_field['reference_stats'] = None  if references is None else self._citrefStats(references)
		elif verbosityLevel > 1:
			populables = []
			nextLevelPop = []
			for pub_field in pub_list:
				if (mode & 2) != 0:
					citations = pub_field.get('citations')
					if citations is not None:
						populables.extend(citations)
						if verbosityLevel >= 2:
							nextLevelPop.extend(citations)
					
				if (mode & 1) != 0:
					references = pub_field.get('references')
					if references is not None:
						populables.extend(references)
			#for pub_field in pub_list:
			#	if (mode & 2) != 0:
			#		citations = pub_field.get('citations')
			#		if citations is not None:
			#			self.populatePubIds(citations)
			#		
			#	if (mode & 1) != 0:
			#		references = pub_field.get('references')
			#		if references is not None:
			#			self.populatePubIds(references)
			
			if populables:
				self.populatePubIds(populables)
			
			if nextLevelPop:
				self.listReconcileCitRefMetricsBatch(nextLevelPop,verbosityLevel-1,mode)
		
		# This is needed for multiprocess approaches
		return pub_list
	
	def listReconcileRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0) -> List[Dict[str,Any]]:
		"""
			This method takes in batches of found publications and retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		return self.listReconcileCitRefMetricsBatch(pub_list,verbosityLevel,1)
	
	def listReconcileCitMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0) -> List[Dict[str,Any]]:
		"""
			This method takes in batches of found publications and retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		return self.listReconcileCitRefMetricsBatch(pub_list,verbosityLevel,2)

	# This method does the different reads and retries
	# in case of partial contents
	def retriable_full_http_read(self,theRequest:request.Request,timeout:int=300,debug_url=None) -> bytes:
		if self._debug and (debug_url is not None):
			print("[{}] {}".format(datetime.datetime.now().isoformat(),debug_url),file=sys.stderr)
			sys.stderr.flush()
		retries = 0
		while retries <= self.max_retries:
			retryexc = None
			retrymsg = None
			try:
				# The original bytes
				response = b''
				with request.urlopen(theRequest,timeout=timeout) as req:
					while True:
						try:
							# Try getting it
							responsePart = req.read()
						except http.client.IncompleteRead as icread:
							# Getting at least the partial content
							response += icread.partial
							continue
						else:
							# In this case, saving all
							response += responsePart
						break
				
				return response
			except HTTPError as e:
				retryexc = e
				if e.code >= 500:
					retrymsg = "code {}".format(e.code)
			except URLError as e:
				retryexc = e
				retryreason = str(e.reason)
				if 'handshake operation timed out' in retryreason:
					retrymsg = "handshake timeout"
				elif 'is unreachable' in retryreason:
					retrymsg = "network is unreachable"
				elif 'failure in name resolution' in retryreason:
					retrymsg = "failure in name resolution"
				
			except http.client.RemoteDisconnected as e:
				retrymsg = "remote disconnect"
				retryexc = e
			except socket.timeout as e:
				retrymsg = "socket timeout"
				retryexc = e
			
			retries += 1
			if (retrymsg is not None) and (retries <= self.max_retries):
				if self._debug:
					print("\tRetry {0}, due {1}".format(retries,retrymsg),file=sys.stderr)
					sys.stderr.flush()
				
				# Using a backoff time of 2 seconds when some recoverable error happens
				time.sleep(2**retries)
			else:
				if retryexc is None:
					retryexc = Exception("Untraced ERROR")
				
				if debug_url is not None:
					print("URL with ERROR: "+debug_url+"\n",file=sys.stderr)
					sys.stderr.flush()
				
				raise retryexc
			
	
	def _citrefStats(self,citrefs:Iterator[Dict[str,Any]]) -> List[Dict[str,Any]]:
		# Computing the stats
		citref_stats = {}
		for citref in citrefs:
			year = citref.get('year',-1)
			if year is None:
				year = -1
			if year in citref_stats:
				citref_stats[year] += 1
			else:
				citref_stats[year] = 1
		
		return [ {'year':year,'count':citref_stats[year]} for year in sorted(citref_stats.keys()) ]
	
	def flattenPubs(self,opeb_entries:List[Dict[str,Any]]) -> None:
		"""
			This method takes in batches of entries and retrives citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		
		linear_pubs = []
		for entry_pubs in map(lambda opeb_entry: opeb_entry['entry_pubs'],opeb_entries):
			for entry_pub in entry_pubs:
				linear_pubs.extend(entry_pub['found_pubs'])
		
		return linear_pubs
	
	def reconcileCitRefMetricsBatch(self,opeb_entries:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of entries and retrives citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		
		linear_pubs = []
		for entry_pubs in map(lambda opeb_entry: opeb_entry['entry_pubs'],opeb_entries):
			for entry_pub in entry_pubs:
				linear_pubs.extend(entry_pub['found_pubs'])
		
		self.listReconcileCitRefMetricsBatch(linear_pubs,verbosityLevel)
	
	def _getUniqueNewPubs(self,query_pubs:List[Dict[str,Any]],query_refs:List[Dict[str,Any]],saved_pubs:Dict[str,str],saved_comb:Dict[str,str]):
		# The list of new citations to populate later
		if len(query_pubs) > 0:
			new_pubs = list(filter(lambda pub: (pub.get('source') is not None) and ((pub.get('source','') + ':' + pub.get('id','')) not in saved_pubs),query_pubs))
		else:
			new_pubs = []
		
		unique_pubs = {}
		for new_pub in new_pubs:
			new_key = new_pub.get('source','') + ':' + new_pub.get('id','')
			if new_key not in unique_pubs:
				unique_pubs[new_key] = new_pub
		
			
		#import pprint
		#pp = pprint.PrettyPrinter(indent=4)
		#pp.pprint(query_pubs)
		
		if len(query_refs) > 0:
			#pp.pprint(query_refs)
			new_ref_pubs = list(filter(lambda pub: (pub.get('source') is not None) and ((pub.get('source','') + ':' + pub.get('id','')) not in saved_comb),query_refs))
		else:
			new_ref_pubs = []
		
		unique_ref_pubs = {}
		for new_ref_pub in new_ref_pubs:
			new_ref_key = new_ref_pub.get('source','') + ':' + new_ref_pub.get('id','')
			if (new_ref_key not in unique_pubs) and (new_ref_key not in unique_ref_pubs):
				unique_ref_pubs[new_ref_key] = new_ref_pub
		
		if len(unique_pubs) == 0 and len(unique_ref_pubs) == 0:
			return None, None
		
		# The list to obtain the basic publication data
		# and the list of new citations to dig in later (as soft as possible)
		
		return list(unique_ref_pubs.values()),list(unique_pubs.values())
	
	
	@classmethod
	def populateMapping(cls,base_mapping:Dict[str,Any],dest_mapping:Dict[str,Any],onlyYear:bool=False) -> None:
		if onlyYear:
			dest_mapping['year'] = base_mapping.get('year')
		else:
			dest_mapping.update(base_mapping)
	
	@abstractmethod
	def populatePubIdsBatch(self,partial_mappings:List[Dict[str,Any]]) -> None:
		pass
	
	def populatePubIds(self,partial_mappings:List[Dict[str,Any]],onlyYear:bool=False) -> None:
		populable_mappings = []
		
		for partial_mapping in partial_mappings:
			# We are interested only in the year facet
			# as it is a kind of indicator
			pubYear = partial_mapping.get('year')
			_id = partial_mapping.get('id')
			# There can be corrupted or incomplete entries
			# in the source
			source_id = partial_mapping.get('source')
			if _id is not None and source_id is not None and (pubYear is None or not onlyYear):
				mapping = self.pubC.getCachedMapping(source_id,_id)
				
				# Not found or expired mapping?
				if mapping is None:
					populable_mappings.append(partial_mapping)
				else:
					self.populateMapping(mapping,partial_mapping,onlyYear)
		
		if len(populable_mappings) > 0:
			for start in range(0,len(populable_mappings),self.step_size):
				stop = start+self.step_size
				populable_mappings_slice = populable_mappings[start:stop]
				populable_mappings_clone_slice = list(map(lambda p_m: { 'id': p_m['id'], 'source': p_m['source'] } , populable_mappings_slice))
				self.populatePubIdsBatch(populable_mappings_clone_slice)
			
				for p_m,p_m_c in zip(populable_mappings_slice,populable_mappings_clone_slice):
					# It is a kind of indicator the 'year' flag
					if p_m_c.get('year') is not None:
						self.pubC.setCachedMapping(p_m_c)
						self.populateMapping(p_m_c,p_m,onlyYear)
	
	KEEP_REFS_KEYS=('source', 'id', 'base_pubs')
	def _tidyCitRefRefs(self,citrefs:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		retval = []
		if citrefs is not None:
			for citref in citrefs:
				ret = {}
				for key in filter(lambda key: key in self.KEEP_REFS_KEYS,citref.keys()):
					ret[key] = citref[key]
				
				retval.append(ret)
		
		return retval
	
	def reconcilePubIdsFlatFormat(self,entries:List[Dict[str,Any]],results_path:str=None,verbosityLevel:float=0) -> List[Any]:
		# This unlinks the input from the output
		copied_entries = copy.deepcopy(entries)
		
		# The tools subdirectory
		tools_subpath = 'tools'
		os.makedirs(os.path.abspath(os.path.join(results_path,tools_subpath)),exist_ok=True)
		
		saved_tools = []
		# Now, gather the tool publication entries
		filename_prefix = 'pub_tool_'
		for start in range(0,len(copied_entries),self.step_size):
			stop = start+self.step_size
			entries_slice = copied_entries[start:stop]
			self.reconcilePubIdsBatch(entries_slice)
			
			copied_entries_slice = copy.deepcopy(entries_slice)
			for idx, entry in enumerate(copied_entries_slice):
				part_dest_file = os.path.join(tools_subpath,filename_prefix+str(start+idx)+'.json')
				dest_file = os.path.join(results_path,part_dest_file)
				saved_tools.append({
					'@id': entry['@id'],
					'file': part_dest_file
				})
				with open(dest_file,mode="w",encoding="utf-8") as outentry:
					outentry.write(self.je.encode(entry))
			del copied_entries_slice
		
		# Recording what we have already fetched (and saved)
		saved_pubs = {}
		saved_comb = {}
		saved_comb_arr = []
		
		# The counter for the files being generated
		pub_counter = 0
		pubs_subpath = 'pubs'
		query_refs = []
		query_pubs = self.flattenPubs(copied_entries)
		
		depth = 0
		
		def _save_population_slice(population_slice,not_last=True):
			nonlocal pub_counter
			nonlocal pubs_subpath
			nonlocal saved_pubs
			nonlocal saved_comb
			nonlocal saved_comb_arr
			nonlocal query_pubs
			nonlocal query_refs
			
			for new_pub in population_slice:
				# Getting the name of the file
				new_key = new_pub.get('source','') + ':' + new_pub.get('id','')
				
				assert new_key not in saved_pubs
				if new_key in saved_comb:
					new_pub_file = saved_comb[new_key]
				else:
					if pub_counter % self.num_files_per_dir == 0:
						pubs_subpath = 'pubs_'+str(pub_counter)
						os.makedirs(os.path.abspath(os.path.join(results_path,pubs_subpath)),exist_ok=True)
					part_new_pub_file = os.path.join(pubs_subpath,'pub_'+str(pub_counter)+'.json')
					saved_comb_arr.append({
						'_id': new_key,
						'file': part_new_pub_file
					})
					new_pub_file = os.path.join(results_path,part_new_pub_file)
					saved_comb[new_key] = new_pub_file
					pub_counter += 1
				
				reconciled = False
				if 'references' in new_pub:
					reconciled = True
					# Fixing the output
					new_pub['reference_refs'] = self._tidyCitRefRefs(new_pub.pop('references'))
					if not_last and (new_pub['reference_refs'] is not None):
						query_refs.extend(new_pub['reference_refs'])
				if not_last and ('citations' in new_pub):
					reconciled = True
					# Fixing the output
					new_pub['citation_refs'] = self._tidyCitRefRefs(new_pub.pop('citations'))
					if new_pub['citation_refs'] is not None:
						query_pubs.extend(new_pub['citation_refs'])
				
				with open(new_pub_file,mode="w",encoding="utf-8") as outentry:
					outentry.write(self.je.encode(new_pub))
				
				if not_last and reconciled:
					saved_pubs[new_key] = new_pub_file
		
		while (len(query_pubs) + len(query_refs)) > 0:
			unique_to_ref_populate , unique_to_reconcile = self._getUniqueNewPubs(query_pubs,query_refs,saved_pubs,saved_comb)
			
			query_pubs.clear()
			query_refs.clear()
			if unique_to_ref_populate is None:
				break
			
			not_last = depth < verbosityLevel
			if not_last:
				print("DEBUG: Level {} Pop {} Rec {}".format(depth,len(unique_to_ref_populate)+len(unique_to_reconcile),len(unique_to_reconcile)),file=sys.stderr)
				sys.stderr.flush()
				
				# The ones to get both citations and references
				for start in range(0,len(unique_to_reconcile),self.step_size):
					stop = start+self.step_size
					# This unlinks the input from the output
					unique_to_reconcile_slice = copy.deepcopy(unique_to_reconcile[start:stop])
					
					# Obtaining the publication data
					self.populatePubIds(unique_to_reconcile_slice)
					
					# The list of new citations AND references to dig in later (as soft as possible)
					self.listReconcileCitRefMetricsBatch(unique_to_reconcile_slice,-1)
					
					# Saving (it works because all the elements in unique_to_reconcile are in unique_to_populate)
					# and getting the next batch from those with references and/or citations
					_save_population_slice(unique_to_reconcile_slice)
					del unique_to_reconcile_slice
			else:
				unique_to_ref_populate.extend(unique_to_reconcile)
				if self._debug:
					print("DEBUG: Last Pop {}".format(len(unique_to_ref_populate)),file=sys.stderr)
					sys.stderr.flush()
			
			# The ones to get only references
			for start in range(0,len(unique_to_ref_populate),self.step_size):
				stop = start+self.step_size
				# This unlinks the input from the output
				unique_to_ref_populate_slice = copy.deepcopy(unique_to_ref_populate[start:stop])
				
				# Obtaining the publication data
				self.populatePubIds(unique_to_ref_populate_slice)
				
				# The list of ONLY references to dig in later (as soft as possible)
				self.listReconcileRefMetricsBatch(unique_to_ref_populate_slice,-1)
				
				# Saving (it works because all the elements in unique_to_reconcile are in unique_to_populate)
				# and getting the next batch from those with references and/or citations
				_save_population_slice(unique_to_ref_populate_slice,not_last)
				del unique_to_ref_populate_slice
			
			if not_last:
				depth += 1
			else:
				break
		
		if self._debug:
			print("DEBUG: Saved {} publications".format(pub_counter),file=sys.stderr)
			sys.stderr.flush()
		
#			print("DEBUG: Residuals {} {} {} {} {}".format(get_size(saved_pubs),get_size(saved_comb),get_size(saved_comb_arr),get_size(query_refs),get_size(query_pubs)),file=sys.stderr)
		
		# Last, save the manifest file
		manifest_file = os.path.join(results_path,'manifest.json')
		with open(manifest_file,mode="w",encoding="utf-8") as manifile:
			manifile.write(self.je.encode({'@timestamp': datetime.datetime.now().isoformat(), 'tools': saved_tools, 'publications': saved_comb_arr}))
		
#		# Add to leaky code within python_script_being_profiled.py
#		from pympler import muppy, summary
#		all_objects = muppy.get_objects()
#		sum1 = summary.summarize(all_objects)
#
#		# Prints out a summary of the large objects
#		summary.print_(sum1)
#
#		## Get references to certain types of objects such as dataframe
#		#dataframes = [ao for ao in all_objects if isinstance(ao, pd.DataFrame)]
#		#
#		#for d in dataframes:
#		#  print d.columns.values
#		#  print len(d)
		
	def reconcilePubIds(self,entries:List[Dict[str,Any]],results_path:str=None,results_format:str=None,verbosityLevel:float=0) -> List[Any]:
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		
		# As flat format is so different from the previous ones, use a separate codepath
		if results_format == "flat":
			return self.reconcilePubIdsFlatFormat(entries,results_path,verbosityLevel)
		else:
			#print(len(fetchedEntries))
			#print(json.dumps(fetchedEntries,indent=4))
			if results_format == "single":
				jsonOutput = open(results_path,mode="w",encoding="utf-8")
				print('[',file=jsonOutput)
				printComma = False
			else:
				jsonOutput = None
			
			saved_results = []
			for start in range(0,len(entries),self.step_size):
				stop = start+self.step_size
				# This unlinks the input from the output
				entries_slice = copy.deepcopy(entries[start:stop])
				self.reconcilePubIdsBatch(entries_slice)
				self.reconcileCitRefMetricsBatch(entries_slice,verbosityLevel)
				self.pubC.sync()
				if jsonOutput is not None:
					for entry in entries_slice:
						if printComma:
							print(',',file=jsonOutput)
						else:
							printComma=True
						jsonOutput.write(self.je.encode(entry))
				elif results_format == "multiple":
					filename_prefix = 'entry_' if verbosityLevel == 0  else 'fullentry_'
					for idx, entry in enumerate(entries_slice):
						rel_dest_file = filename_prefix+str(start+idx)+'.json'
						saved_results.append({
							'@id': entry['@id'],
							'file': rel_dest_file,
						})
						dest_file = os.path.join(results_path,rel_dest_file)
						with open(dest_file,mode="w",encoding="utf-8") as outentry:
							outentry.write(self.je.encode(entry))
			
			if jsonOutput is not None:
				print(']',file=jsonOutput)
				jsonOutput.close()
			else:
				# Last, save the manifest file
				manifest_file = os.path.join(results_path,'manifest.json')
				with open(manifest_file,mode="w",encoding="utf-8") as manifile:
					manifile.write(self.je.encode({'@timestamp': datetime.datetime.now().isoformat(), 'results': saved_results}))
		
		return entries
