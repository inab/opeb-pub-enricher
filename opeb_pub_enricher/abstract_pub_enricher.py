#!/usr/bin/python

import configparser

from abc import abstractmethod

from typing import overload, Tuple, List, Dict, Any, Iterator

from .skeleton_pub_enricher import SkeletonPubEnricher
from .pub_cache import PubDBCache
from .doi_cache import DOIChecker

class AbstractPubEnricher(SkeletonPubEnricher):
	DEFAULT_REQUEST_DELAY = 0.25
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		...
	
	@overload
	def __init__(self,cache:PubDBCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False,doi_checker:DOIChecker=None):
		super().__init__(cache,prefix,config,debug,doi_checker)
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		request_delay = self.config.getfloat(section_name,'request_delay',fallback=self.DEFAULT_REQUEST_DELAY)
		self.request_delay = request_delay
		
		useragent = self.config.get(section_name,'useragent',fallback='Mozilla/5.0 (X11; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0')
		self.useragent = useragent
	
	@classmethod
	@abstractmethod
	def Name(cls) -> str:
		return 'abstract'
