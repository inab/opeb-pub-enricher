#!/usr/bin/python

import sys
import json
import time
import re
import configparser

from urllib import request
from urllib import parse
from urllib.error import *

from typing import overload, Tuple, List, Dict, Any, Iterator

from .abstract_pub_enricher import AbstractPubEnricher

from .pub_cache import PubDBCache
from .doi_cache import DOIChecker

from . import pub_common

class PubmedEnricher(AbstractPubEnricher):
	# Due restrictions in the service usage
	# there cannot be more than 3 queries per second in
	# unregistered mode, and no more than 10 queries per second
	# in regisitered mode.
	UNREGISTERED_MIN_DELAY = 0.34
	REGISTERED_MIN_DELAY = 0.1
	
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
		
		super().__init__(cache,prefix,config,debug,doi_checker)
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		self.api_key = self.config.get(section_name,'api_key')
		self.elink_step_size = self.config.getint(section_name,'elink_step_size',fallback=self.step_size)
		# Due restrictions in the service usage
		# there cannot be more than 3 queries per second in
		# unregistered mode, and no more than 10 queries per second
		# in regisitered mode.
		min_request_delay = self.REGISTERED_MIN_DELAY if self.api_key else self.UNREGISTERED_MIN_DELAY
		if self.request_delay < min_request_delay:
			self.request_delay = min_request_delay
	
	# Do not change this constant!!!
	PUBMED_SOURCE='pubmed'
	
	@classmethod
	def Name(cls) -> str:
		return cls.PUBMED_SOURCE
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESummary_
	PUB_ID_SUMMARY_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
	def populatePubIdsBatch(self,mappings:List[Dict[str,Any]]) -> None:
		if len(mappings) > 0:
			internal_ids = [ mapping['id']  for mapping in mappings ]
			theQuery = {
				'db': 'pubmed',
				'id': ' '.join(internal_ids),
				'retmode': 'json',
				'retmax': 100000,
				'rettype': 'abstract'
			}
			
			if self.api_key:
				theQuery['api_key'] = self.api_key
			
			summary_url_data = parse.urlencode(theQuery)
			debug_summary_url = self.PUB_ID_SUMMARY_URL+'?'+summary_url_data  if self._debug else None
			
			# Queries with retries
			entriesReq = request.Request(self.PUB_ID_SUMMARY_URL,data=summary_url_data.encode('utf-8'))
			retries = 0
			while retries <= self.max_retries:
				raw_pubmed_mappings = self.retriable_full_http_read(entriesReq,debug_url=debug_summary_url)
				
				# Avoiding to hit the server too fast
				time.sleep(self.request_delay)
				
				try:
					pubmed_mappings = self.jd.decode(raw_pubmed_mappings.decode('utf-8'))
					break
				except json.decoder.JSONDecodeError as jde:
					retries += 1
					retrymsg = 'PubMed mappings JSON decoding error'
					if self._debug:
						print("\tRetry {0}, due {1}. Dump:\n{2}".format(retries,retrymsg,raw_pubmed_mappings),file=sys.stderr)
						sys.stderr.flush()
			
			results = pubmed_mappings.get('result')
			if results is not None:
				uids = results.get('uids',[])
				internal_ids_dict = { mapping['id']: mapping  for mapping in mappings }
				for uid in uids:
					_id = str(uid)
					result = results[_id]
					mapping = internal_ids_dict.get(_id)
					#mapping = {
					#	'id': _id,
					##	'title': result['title'],
					##	'journal': result.get('journalTitle'),
					#	'source': self.PUBMED_SOURCE,
					#	'query': query_str,
					##	'year': int(result['pubYear']),
					##	'pmid': pubmed_id,
					##	'doi': doi_id,
					##	'pmcid': pmc_id
					#}
					# mapping['source'] = self.PUBMED_SOURCE
					mapping['title'] = result.get('title')
					mapping['journal'] = result.get('fulljournalname')
					
					# Computing the publication year
					pubdate = result.get('sortpubdate')
					pubyear = None
					
					if pubdate is not None:
						pubyear = int(pubdate.split('/')[0])
						
					mapping['year'] = pubyear
					
					mapping['authors'] = [ author.get('name')  for author in result.get('authors',[]) ]
					
					# Rescuing the identifiers
					pubmed_id = None
					doi_id = None
					pmc_id = None
					articleids = result.get('articleids')
					if articleids is not None:
						for articleid in result['articleids']:
							idtype = articleid.get('idtype')
							if idtype is not None:
								if idtype == 'pubmed':
									pubmed_id = articleid.get('value')
									# Let's sanitize the id
									if pubmed_id is not None:
										pubmed_id = pubmed_id.strip()
								elif idtype == 'doi':
									# Let's sanitize the id
									if doi_id is not None:
										doi_id = doi_id.strip()
									doi_id = articleid.get('value')
								elif idtype == 'pmc':
									pmc_id = articleid.get('value')
									# Let's sanitize the id
									if pmc_id is not None:
										pmc_id = pmc_id.strip()
					
					mapping['pmid'] = pubmed_id
					mapping['doi'] = doi_id
					mapping['pmcid'] = pmc_id

				#print(json.dumps(pubmed_mappings,indent=4))
				# sys.exit(1)
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESearch_
	# Documentation: https://www.nlm.nih.gov/bsd/mms/medlineelements.html
	PUB_ID_CONVERTER_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		# Preparing the query ids
		raw_query_ids = []
		
		for query_id in query_ids:
			pubmed_id = query_id.get('pmid')
			if pubmed_id is not None:
				queryStr = pubmed_id+'[pmid]'
				raw_query_ids.append(queryStr)
			
			doi_id_norm = query_id.get('doi')
			if doi_id_norm is not None:
				queryStr = '"'+doi_id_norm+'"[doi]'
				raw_query_ids.append(queryStr)
			
			pmc_id = query_id.get('pmcid')
			if pmc_id is not None:
				queryStr = pmc_id+'[pmc]'
				raw_query_ids.append(queryStr)
		
		# Now, with the unknown ones, let's ask the server
		mappings = []
		if len(raw_query_ids) > 0:
			# Step one: get the internal identifiers corresponding to the input queries
			theIdQuery = {
				'db': 'pubmed',
				'term': ' OR '.join(raw_query_ids),
				'format': 'json'
			}
			
			if self.api_key:
				theIdQuery['api_key'] = self.api_key
			
			converter_url_data = parse.urlencode(theIdQuery)
			debug_converter_url = self.PUB_ID_CONVERTER_URL + '?' + converter_url_data  if self._debug else None
			
			# Queries with retries
			converterReq = request.Request(self.PUB_ID_CONVERTER_URL,data=converter_url_data.encode('utf-8'))
			
			retries = 0
			while retries <= self.max_retries:
				raw_id_mappings = self.retriable_full_http_read(converterReq,debug_url = debug_converter_url)
				
				# Avoiding to hit the server too fast
				time.sleep(self.request_delay)
				
				try:
					id_mappings = self.jd.decode(raw_id_mappings.decode('utf-8'))
					break
				except json.decoder.JSONDecodeError as jde:
					retries += 1
					retrymsg = 'PubMed raw id mappings JSON decoding error'
					if self._debug:
						print("\tRetry {0}, due {1}. Dump:\n{2}".format(retries,retrymsg,raw_id_mappings),file=sys.stderr)
						sys.stderr.flush()
			
			
			# We record the unpaired DOIs
			eresult = id_mappings.get('esearchresult')
			if eresult is not None:
				idlist = eresult.get('idlist')
				translationstack = eresult.get('translationstack')
				
				# First, record the 
				if idlist is not None and translationstack is not None:
					for _id , query_str in zip(idlist,translationstack):
						# This is a very minimal mapping
						# needed to enrich and relate
						mapping = {
							'id': str(_id),
							'source': self.PUBMED_SOURCE
							#'query': query_str,
						}
						mappings.append(mapping)
			
			# print(json.dumps(entries,indent=4))
			
			# Step two: get all the information of these input queries
			self.populatePubIds(mappings)
		
		return mappings
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ELink_
	# Documented at: https://eutils.ncbi.nlm.nih.gov/entrez/query/static/entrezlinks.html
	# The drawback of this service is that it we are not careful enough,
	# it merges the answers from several queries
	ELINKS_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi'
	ELINK_QUERY_MAPPINGS = {
		'pubmed_pubmed_citedin': pub_common.CITATIONS_KEYS,
		'pubmed_pubmed_refs': pub_common.REFERENCES_KEYS
	}
	
	LINKNAME_MODE_MAP = {
		1: 'pubmed_pubmed_refs',
		2: 'pubmed_pubmed_citedin',
		3: 'pubmed_pubmed_citedin,pubmed_pubmed_refs'
	}
	
	def queryCitRefsBatch(self,query_citations_data:Iterator[Dict[str,Any]],minimal:bool=False,mode:int=3) -> Iterator[Dict[str,Any]]:
		# First, saving the queries to issue
		raw_ids = []
		query_hash = {}
		
		search_linkname = self.LINKNAME_MODE_MAP.get(mode,self.LINKNAME_MODE_MAP[3])
		search_linknames = search_linkname.split(r',')
		
		for query in query_citations_data:
			raw_ids.append(query['id'])
			query_hash[query['id']] = query
		
		# Second, query by batches
		for start in range(0,len(raw_ids),self.elink_step_size):
			stop = start+self.elink_step_size
			raw_ids_slice = raw_ids[start:stop]
			
			theLinksQuery = {
				'dbfrom': 'pubmed',
				'linkname': search_linkname,
				'id': raw_ids_slice,
				'db': 'pubmed',
				'retmode': 'json'
			}
			
			if self.api_key:
				theLinksQuery['api_key'] = self.api_key
			
			elink_url_data = parse.urlencode(theLinksQuery,doseq=True)
			debug_elink_url = self.ELINKS_URL+'?'+elink_url_data  if self._debug else None
			
			# Queries with retries
			elinksReq = request.Request(self.ELINKS_URL,data=elink_url_data.encode('utf-8'))
			retries = 0
			while retries <= self.max_retries:
				raw_json_citation_refs = self.retriable_full_http_read(elinksReq,debug_url=debug_elink_url)
				
				# Avoiding to hit the server too fast
				time.sleep(self.request_delay)
			
				try:
					raw_json_citations = self.jd.decode(raw_json_citation_refs.decode('utf-8'))
					break
				except json.decoder.JSONDecodeError as jde:
					retries += 1
					retrymsg = 'PubMed citations JSON decoding error'
					if self._debug:
						print("\tRetry {0}, due {1}. Dump:\n{2}".format(retries,retrymsg,raw_json_citation_refs),file=sys.stderr)
						sys.stderr.flush()
			
			linksets = raw_json_citations.get('linksets')
			if linksets is not None:
				cite_res_arr = []
				citrefsG = []
				for linkset in linksets:
					ids = linkset.get('ids',[])
					if len(ids) > 0:
						_id = str(ids[0])
						linksetdbs = linkset.get('linksetdbs',[])
						
						query = query_hash[_id]
						source_id = query['source']
						cite_res = {
							'id': _id,
							'source': source_id
						}
						
						left_linknames = search_linknames.copy()
						
						# The fetched results
						if len(linksetdbs) > 0:
							for linksetdb in linksetdbs:
								linkname = linksetdb['linkname']
								left_linknames.remove(linkname)
								
								citrefs_key,citrefs_count_key = self.ELINK_QUERY_MAPPINGS.get(linkname,(None,None))
								if citrefs_key and citrefs_key not in query:
									#import sys
									#print(query_hash,file=sys.stderr)
									links = linksetdb.get('links',[])
									
									citrefs = list(map(lambda uid: {
										'id': str(uid),	# _id
										'source': source_id
									},links))
									
									cite_res[citrefs_key] = citrefs
									cite_res[citrefs_count_key] = len(citrefs)
									# To the batch of queries
									if not minimal:
										citrefsG.extend(citrefs)
						
						# the unfetched ones with no error code
						if len(left_linknames) > 0:
							for linkname in left_linknames:
								citrefs_key,citrefs_count_key = self.ELINK_QUERY_MAPPINGS.get(linkname,(None,None))
								
								cite_res[citrefs_key] = None
								cite_res[citrefs_count_key] = 0
						
						# Now, issue the batch query
						if not minimal and (len(citrefsG) > 0):
							self.populatePubIds(citrefsG,onlyYear=True)
						
						# Saving it for later processing
						cite_res_arr.append(cite_res)
				
				if citrefsG:
					# Now, issue the last batch query
					self.populatePubIds(citrefsG,onlyYear=True)
					
				# And propagate the batch of results!
				for cite_res in cite_res_arr:		
					yield cite_res
