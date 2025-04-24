#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import datetime
from typing import Tuple, List, Dict, Any, NewType, Iterator

from . import pub_common
from .pub_common import Timestamps
from .doi_cache import DOIChecker

# Alias types declaration
Citation = NewType('Citation',Dict[str,Any])
CitationCount = NewType('CitationCount',int)
Reference = NewType('Reference',Dict[str,Any])
ReferenceCount = NewType('ReferenceCount',int)
Mapping = NewType('Mapping',Dict[str,Any])
UnqualifiedId = NewType('UnqualifiedId',str)
SourceId = NewType('SourceId',str)
QualifiedId = NewType('QualifiedId',Tuple[SourceId,UnqualifiedId])
PublishId = NewType('PublishId',str)
EnricherId = NewType('EnricherId',str)
MetaQualifiedId = NewType('MetaQualifiedId',Tuple[EnricherId,SourceId,UnqualifiedId])

import json
import sqlite3
import zlib

CACHE_DAYS = 28
	
class PubDBCache(object):
	"""
		The publications cache management code
		Currently, it stores the correspondence among PMIDs,
		PMC ids, DOIs and the internal identifier in the
		original source.
		Also, it stores the title, etc..
		Also, it stores the citations fetched from the original source
	"""
	DEFAULT_CACHE_DB_FILE="pubEnricher_CACHE.db"
	
	OLDEST_CACHE = datetime.timedelta(days=CACHE_DAYS)

	def __init__(self,enricher_name:str, cache_dir:str=".", prefix:str=None,doi_checker:DOIChecker=None):
		# The enricher name, used as default for all the queries
		self.enricher_name = enricher_name
		self.cache_dir = cache_dir
		
		if doi_checker is None:
			doi_checker = DOIChecker(cache_dir)
		
		self.doi_checker = doi_checker
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		# Should we set a prefix for the shelves?
		if prefix is None:
			cache_db_file = self.DEFAULT_CACHE_DB_FILE
		else:
			cache_db_file = prefix + self.DEFAULT_CACHE_DB_FILE
		
		self.cache_db_file = os.path.join(cache_dir,cache_db_file)
		self.jd = json.JSONDecoder()
		self.je = json.JSONEncoder()
	
	def __enter__(self):
		existsCache = os.path.exists(self.cache_db_file) and (os.path.getsize(self.cache_db_file) > 0)
		initializeCache = not existsCache
		
		# Opening / creating the database, with normal locking
		# and date parsing
		self.conn = sqlite3.connect(self.cache_db_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES, check_same_thread = False)
		self.conn.execute("""PRAGMA locking_mode = NORMAL""")
		self.conn.execute("""PRAGMA journal_mode = WAL""")
		
		# Database structures
		with self.conn:
			cur = self.conn.cursor()
			updateDatabase = initializeCache
			if initializeCache:
				# Publication table
				cur.execute("""
CREATE TABLE pub (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	payload BLOB NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	PRIMARY KEY (enricher,id,source)
)
""")
				# IDMap
				# pub_id_type VARCHAR(32) NOT NULL,
				# PRIMARY KEY (pub_id,pub_id_type),
				cur.execute("""
CREATE TABLE idmap (
	pub_id VARCHAR(4096) NOT NULL,
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	PRIMARY KEY (pub_id,id,enricher,source),
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source)
)
""")
				# Denormalized citations and references
				# so we can register empty answers,
				# and get the whole list with a single query
				cur.execute("""
CREATE TABLE citref (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	is_cit BOOLEAN NOT NULL,
	payload BLOB,
	last_fetched TIMESTAMP NOT NULL,
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source)
)
""")
				# Index on the enricher, id and source
				cur.execute("""
CREATE INDEX citref_e_i_s ON citref(enricher,id,source)
""")
				# Lower Mappings
				cur.execute("""
CREATE TABLE lower_map (
	enricher VARCHAR(32) NOT NULL,
	id VARCHAR(4096) NOT NULL,
	source VARCHAR(32) NOT NULL,
	lower_enricher VARCHAR(32) NOT NULL,
	lower_id VARCHAR(4096) NOT NULL,
	lower_source VARCHAR(32) NOT NULL,
	last_fetched TIMESTAMP NOT NULL,
	FOREIGN KEY (enricher,id,source) REFERENCES pub(enricher,id,source),
	FOREIGN KEY (lower_enricher,lower_id,lower_source) REFERENCES pub(enricher,id,source)
)
""")
				# Index on the lower mapping
				cur.execute("""
CREATE INDEX lower_map_e_i_s ON lower_map(lower_enricher,lower_id,lower_source)
""")
			cur.close()
			
			
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.conn.close()
	
	
	def sync(self) -> None:
		# This method has become a no-op
		pass
	
	def getCitRefs(self,qual_list:Iterator[QualifiedId],is_cit:bool) -> Iterator[Tuple]:
		with self.conn:
			cur = self.conn.cursor()
			for source_id, _id in qual_list:
				# The DATETIME expression helps invalidating stale results
				cur.execute("""
SELECT payload
FROM citref
WHERE
DATETIME('NOW','-{} DAYS') <= last_fetched
AND
enricher = :enricher
AND
id = :id
AND
source = :source
AND
is_cit = :is_cit
""".format(CACHE_DAYS),{'enricher': self.enricher_name,'id': _id,'source': source_id,'is_cit': is_cit})
				res = cur.fetchone()
				if res:
					yield self.jd.decode(zlib.decompress(res[0]).decode("utf-8"))  if res[0] is not None  else []
				else:
					yield None
	
	def setCitRefs(self,citref_list:Iterator[Tuple[QualifiedId,List[Tuple],bool]],timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		with self.conn:
			cur = self.conn.cursor()
			for qual_id,citrefs,is_cit in citref_list:
				params = {
					'enricher': self.enricher_name,
					'source': qual_id[0],
					'id': qual_id[1],
					'is_cit': is_cit,
					'payload': zlib.compress(self.je.encode(citrefs).encode("utf-8"),zlib.Z_BEST_COMPRESSION)  if citrefs is not None  else  None,
					'last_fetched': timestamp
				}
				
				# First, remove
				cur.execute("""
DELETE FROM citref
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND is_cit = :is_cit
""",params)
				
				# Then, insert
				cur.execute("""
INSERT INTO citref(enricher,id,source,is_cit,payload,last_fetched) VALUES(:enricher,:id,:source,:is_cit,:payload,:last_fetched)
""",params)
	
	def getCitationsAndCount(self, source_id:SourceId, _id:UnqualifiedId) -> Tuple[List[Citation],CitationCount]:
		for citations in self.getCitRefs([(source_id,_id)], True):
			if citations is not None:
				return citations,len(citations)
			else:
				break
		
		return None, None
	
	def setCitationsAndCount(self,source_id:SourceId,_id:UnqualifiedId,citations:List[Citation],citation_count:CitationCount,timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		self.setCitRefs([((source_id,_id),citations,True)],timestamp)
	
	def getReferencesAndCount(self, source_id:SourceId, _id:UnqualifiedId) -> Tuple[List[Citation],CitationCount]:
		for references in self.getCitRefs([(source_id,_id)], False):
			if references is not None:
				return references,len(references)
			else:
				break
		
		return None, None
	
	def setReferencesAndCount(self,source_id:SourceId,_id:UnqualifiedId,references:List[Reference],reference_count:ReferenceCount,timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		self.setCitRefs([((source_id,_id),references,False)],timestamp)
	
	def getRawCachedMappings_TL(self,qual_list:Iterator[QualifiedId]) -> Iterator[Tuple[datetime.datetime,Mapping]]:
		"""
			This method does not invalidate the cache
		"""
		
		cur = self.conn.cursor()
		for source_id, _id in qual_list:
			cur.execute("""
SELECT last_fetched, payload
FROM pub
WHERE
enricher = :enricher
AND
id = :id
AND
source = :source
""",{'enricher': self.enricher_name,'id': _id,'source': source_id})
			res = cur.fetchone()
			if res:
				yield Timestamps.UTCTimestamp(res[0]), self.jd.decode(zlib.decompress(res[1]).decode("utf-8"))
			else:
				yield None, None
	
	def getRawCachedMappings(self,qual_list:Iterator[QualifiedId]) -> Iterator[Tuple[datetime.datetime,Mapping]]:
		"""
			This method does not invalidate the cache
		"""
		
		with self.conn:
			for mapping_timestamp, mapping in self.getRawCachedMappings_TL(qual_list):
				yield mapping_timestamp, mapping
	
	def getRawCachedMapping_TL(self,source_id:SourceId,_id:UnqualifiedId) -> Tuple[datetime.datetime,Mapping]:
		for mapping_timestamp, mapping in self.getRawCachedMappings_TL([(source_id,_id)]):
			return mapping_timestamp, mapping
	
	def getRawCachedMapping(self,source_id:SourceId,_id:UnqualifiedId) -> Tuple[datetime.datetime,Mapping]:
		for mapping_timestamp, mapping in self.getRawCachedMappings([(source_id,_id)]):
			return mapping_timestamp, mapping
	
	def getCachedMapping(self,source_id:SourceId,_id:UnqualifiedId) -> Mapping:
		mapping_timestamp , mapping = self.getRawCachedMapping(source_id,_id)
		
		# Invalidate cache
		if mapping_timestamp is not None and (Timestamps.UTCTimestamp() - mapping_timestamp) > self.OLDEST_CACHE:
			mapping = None
		
		return mapping
	
	def getRawSourceIds_TL(self,publish_id_iter:Iterator[PublishId]) -> Iterator[List[Tuple[datetime.datetime,QualifiedId]]]:
		"""
			This method does not invalidate the cache
		"""
		cur = self.conn.cursor()
		params = {
			'enricher': self.enricher_name
		}
		for publish_id in publish_id_iter:
			params['pub_id'] = publish_id
			retval = []
			for res in cur.execute("""
SELECT last_fetched, source, id
FROM idmap
WHERE
enricher = :enricher
AND
pub_id = :pub_id
""",params):
				retval.append((Timestamps.UTCTimestamp(res[0]),(res[1],res[2])))
			yield retval
	
	def getRawSourceIds(self,publish_id:PublishId) -> List[Tuple[datetime.datetime,QualifiedId]]:
		"""
			This method does not invalidate the cache
		"""
		with self.conn:
			for listRes in self.getRawSourceIds_TL([publish_id]):
				return listRes
	
	def getSourceIds(self,publish_id:PublishId) -> List[QualifiedId]:
		internal_ids = []
		
		# Invalidate cache
		for timestamp_internal_id , internal_id in self.getRawSourceIds(publish_id):
			if timestamp_internal_id is not None and (Timestamps.UTCTimestamp() - timestamp_internal_id) <= self.OLDEST_CACHE:
				internal_ids.append(internal_id)
		
		return internal_ids
	
	def appendSourceIds_TL(self,publish_id_iter:Iterator[PublishId],source_id:SourceId,_id:UnqualifiedId,timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		cur = self.conn.cursor()
		
		params = {
			'enricher': self.enricher_name,
			'id': _id,
			'source': source_id,
			'last_fetched': timestamp
		}
		
		# In case of stale cache, remove all
		cur.execute("""
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),params)
		
		# Now, try storing specifically these
		for publish_id in publish_id_iter:
			params['pub_id'] = publish_id
			
			cur.execute("""
INSERT INTO idmap(pub_id,enricher,id,source,last_fetched) VALUES(:pub_id,:enricher,:id,:source,:last_fetched)
""",params)
	
	def removeSourceIds_TL(self,publish_id_iter:Iterator[PublishId],source_id:SourceId,_id:UnqualifiedId) -> None:
		cur = self.conn.cursor()
		
		params = {
			'enricher': self.enricher_name,
			'id': _id,
			'source': source_id
		}
		
		# In case of stale cache, remove all
		cur.execute("""
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),params)
		
		# Now, try removing specifically these
		for publish_id in publish_id_iter:
			params['pub_id'] = publish_id
			
			cur.execute("""
DELETE FROM idmap
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND pub_id = :pub_id
""",params)
		
	
	def getRawCachedMappingsFromPartial(self,partial_mapping:Mapping) -> List[Mapping]:
		"""
			This method returns one or more cached mappings, based on the partial
			mapping provided. First attempt is using the id, then it tries through
			base_pubs information. Last, it derives	on the pmid, pmcid and doi to
			rescue, if available.
			
			This method does not invalidate caches
		"""
		mappings = []
		mapping_ids = []
		with self.conn:
			if partial_mapping.get('id'):
				_ , mapping = self.getRawCachedMapping_TL(partial_mapping.get('source'),partial_mapping.get('id'))
				if mapping:
					mappings.append(mapping)
			
			# Now, trying with the identifiers of the mapped publications (if it is the case)
			if not mappings:
				base_pubs = partial_mapping.get('base_pubs',[])
				if base_pubs:
					for base_pub in base_pubs:
						for internal_ids in self.getRawMetaSourceIds_TL([(base_pub.get('enricher'),base_pub.get('source'),base_pub.get('id'))]):
							if internal_ids:
								mapping_ids.extend(map(lambda internal_id: internal_id[1], internal_ids))
			
			# Last resort
			if not mappings and (partial_mapping.get('pmid') or partial_mapping.get('pmcid') or partial_mapping.get('doi')):
				for field_name in ('pmid','pmcid','doi'):
					_theId = partial_mapping.get(field_name)
					if _theId:
						for internal_ids in self.getRawSourceIds_TL([_theId]):
							# Only return when internal_ids is a
							if internal_ids:
								mapping_ids.extend(map(lambda internal_id: internal_id[1], internal_ids))
			
			if mapping_ids:
				# Trying to avoid duplicates
				mapping_ids = list(set(mapping_ids))
				mappings.extend(filter(lambda r: r is not None, map(lambda _iId: self.getRawCachedMapping_TL(*_iId)[1], mapping_ids)))
			
		return mappings
	
	def getRawMetaSourceIds_TL(self,lower_iter:Iterator[MetaQualifiedId]) -> Iterator[List[Tuple[datetime.datetime,QualifiedId]]]:
		"""
			This method does not invalidate caches
		"""
		cur = self.conn.cursor()
		params = {
			'enricher': self.enricher_name
		}
		for lower_enricher, lower_source, lower_id in lower_iter:
			params['lower_enricher'] = lower_enricher
			params['lower_source'] = lower_source
			params['lower_id'] = lower_id
			retval = []
			for res in cur.execute("""
SELECT last_fetched, source, id
FROM lower_map
WHERE
enricher = :enricher
AND
lower_enricher = :lower_enricher
AND
lower_source = :lower_source
AND
lower_id = :lower_id
""",params):
				retval.append((Timestamps.UTCTimestamp(res[0]),(res[1],res[2])))
			yield retval
	
	def getRawMetaSourceIds(self,lower:MetaQualifiedId) -> List[Tuple[datetime.datetime,QualifiedId]]:
		"""
			This method does not invalidate caches
		"""
		with self.conn:
			for retval in self.getRawMetaSourceIds_TL([lower]):
				return retval
	
	def getMetaSourceIds(self,lower:MetaQualifiedId) -> List[QualifiedId]:
		meta_ids = []
		for timestamp_meta_id , meta_id in self.getRawMetaSourceIds(lower):
			# Invalidate cache
			if timestamp_meta_id is not None and (Timestamps.UTCTimestamp() - timestamp_meta_id) <= self.OLDEST_CACHE:
				meta_ids.append(meta_id)
		
		return meta_ids
	
	def appendMetaSourceIds_TL(self,lower_iter:Iterator[MetaQualifiedId],source_id:SourceId,_id:UnqualifiedId,timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		cur = self.conn.cursor()
		
		params = {
			'enricher': self.enricher_name,
			'id': _id,
			'source': source_id,
			'last_fetched': timestamp
		}
		
		# In case of stale cache, remove all
		cur.execute("""
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),params)
		
		# Now, try storing specifically these
		for lower_enricher,lower_source,lower_id in lower_iter:
			params['lower_enricher'] = lower_enricher
			params['lower_source'] = lower_source
			params['lower_id'] = lower_id
			
			cur.execute("""
INSERT INTO lower_map(enricher,id,source,lower_enricher,lower_id,lower_source,last_fetched) VALUES(:enricher,:id,:source,:lower_enricher,:lower_id,:lower_source,:last_fetched)
""",params)
	
	def removeMetaSourceIds_TL(self,lower_iter:Iterator[MetaQualifiedId],source_id:SourceId,_id:UnqualifiedId) -> None:
		cur = self.conn.cursor()
		
		params = {
			'enricher': self.enricher_name,
			'id': _id,
			'source': source_id
		}
		
		# In case of stale cache, remove all
		cur.execute("""
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND DATETIME('NOW','-{} DAYS') > last_fetched
""".format(CACHE_DAYS),params)
		
		# Now, try removing specifically these
		for lower_enricher,lower_source,lower_id in lower_iter:
			params['lower_enricher'] = lower_enricher
			params['lower_source'] = lower_source
			params['lower_id'] = lower_id
			
			cur.execute("""
DELETE FROM lower_map
WHERE enricher = :enricher
AND id = :id
AND source = :source
AND lower_enricher = :lower_enricher
AND lower_id = :lower_id
AND lower_source = :lower_source
""",params)
	
	def setCachedMappings(self,mapping_iter:Iterator[Mapping],mapping_timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		for mapping in mapping_iter:
			# Before anything, get the previous mapping before updating it
			_id = mapping['id']
			source_id = mapping['source']

			with self.conn:
				old_mapping_timestamp , old_mapping = self.getRawCachedMapping_TL(source_id,_id)
			
				cur = self.conn.cursor()
				params = {
					'enricher': mapping.get('enricher',self.enricher_name),
					'source': mapping['source'],
					'id': mapping['id'],
					'payload': zlib.compress(self.je.encode(mapping).encode("utf-8"),zlib.Z_BEST_COMPRESSION),
					'last_fetched': mapping_timestamp
				}
				
				# First, remove all the data from the previous mappings
				cur.execute("""
DELETE FROM pub
WHERE enricher = :enricher
AND id = :id
AND source = :source
""",params)
				
				# Then, insert
				cur.execute("""
INSERT INTO pub(enricher,id,source,payload,last_fetched) VALUES(:enricher,:id,:source,:payload,:last_fetched)
""",params)
				
				# Then, cleanup of sourceIds cache
				pubmed_id = mapping.get('pmid')
				pmc_id = mapping.get('pmcid')
				pmc_id_norm = pub_common.normalize_pmcid(pmc_id)  if pmc_id else None
				doi_id = mapping.get('doi')
				doi_id_norm = self.doi_checker.normalize_doi(doi_id)  if doi_id else None
				
				if old_mapping_timestamp is not None:
					old_pubmed_id = old_mapping.get('pmid')
					old_doi_id = old_mapping.get('doi')
					old_pmc_id = old_mapping.get('pmcid')
				else:
					old_pubmed_id = None
					old_doi_id = None
					old_pmc_id = None
				old_doi_id_norm = self.doi_checker.normalize_doi(old_doi_id)  if old_doi_id else None
				old_pmc_id_norm = pub_common.normalize_pmcid(old_pmc_id)  if old_pmc_id else None
				
				removable_ids = []
				appendable_ids = []
				for old_id, new_id in [(old_pubmed_id,pubmed_id),(old_doi_id_norm,doi_id_norm),(old_pmc_id_norm,pmc_id)]:
					# Code needed for mismatches
					if old_id is not None and old_id != new_id:
						removable_ids.append(old_id)
					
					if new_id is not None and old_id != new_id:
						appendable_ids.append(new_id)
				
				if removable_ids:
					self.removeSourceIds_TL(removable_ids,source_id,_id)
				if appendable_ids:
					self.appendSourceIds_TL(appendable_ids,source_id,_id,timestamp=mapping_timestamp)
				
				# Let's manage also the lower mappings, from base_pubs
				
				# Creating the sets
				oldLowerSet = set()
				if old_mapping:
					old_base_pubs = old_mapping.get('base_pubs',[])
					for old_lower in old_base_pubs:
						if old_lower.get('id'):
							oldLowerSet.add((old_lower['enricher'],old_lower['source'],old_lower['id']))
				
				newLowerSet = set()
				new_base_pubs = mapping.get('base_pubs',[])
				for new_lower in new_base_pubs:
					if new_lower.get('id'):
						newLowerSet.add((new_lower['enricher'],new_lower['source'],new_lower['id']))
				
				# This set has the entries to be removed
				toRemoveSet = oldLowerSet - newLowerSet
				self.removeMetaSourceIds_TL(toRemoveSet,source_id,_id)
				
				# This set has the entries to be added
				toAddSet = newLowerSet - oldLowerSet
				self.appendMetaSourceIds_TL(toAddSet,source_id,_id,mapping_timestamp)
	
	def setCachedMapping(self,mapping:Mapping,mapping_timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		self.setCachedMappings([mapping],mapping_timestamp)
	
