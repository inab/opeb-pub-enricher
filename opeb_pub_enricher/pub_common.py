#!/usr/bin/python

import datetime, time
from urllib import parse

class Timestamps(object):
	@staticmethod
	def LocalTimestamp(theDate:datetime=datetime.datetime.now()) -> datetime:
		utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
		return theDate.replace(tzinfo=datetime.timezone(offset=datetime.timedelta(seconds=-utc_offset_sec)))
	
	@staticmethod
	def UTCTimestamp(theUTCDate:datetime=datetime.datetime.utcnow()) -> datetime:
		return theUTCDate.replace(tzinfo=datetime.timezone.utc)

import sys

def print_debug_url(module,sourceURL):
	print(' '.join((module,str(Timestamps.LocalTimestamp()),sourceURL)),file=sys.stderr)


import re

def pmid2curie(pubmed_id):
	return 'pmid:'+str(pubmed_id)

def normalize_pmcid(pmc_id):
	"""
		Normalize PMC ids, in case they lack the PMC prefix
	"""
	pmc_id_norm = str(pmc_id)
	if pmc_id_norm.isdigit():
		pmc_id_norm = 'PMC'+pmc_id_norm
	
	return pmc_id_norm

PMC_PATTERN = re.compile('^PMC(.*)',re.I)

def denormalize_pmcid(pmc_id):
	found_pat = PMC_PATTERN.search(pmc_id)
	if found_pat:
		# It was normalized
		pmc_id = found_pat.group(1)
	
	return pmc_id

def pmcid2curie(pmc_id):
	pmc_id_norm = normalize_pmcid(pmc_id)
	return 'pmc:'+pmc_id_norm

CITATIONS_KEYS = ('citations','citation_count')
REFERENCES_KEYS =  ('references','reference_count')


import http.client

# This method does the different reads and retries
# in case of partial contents
def full_http_read(request) -> bytes:
	# The original bytes
	response = b''
	while True:
		try:
			# Try getting it
			responsePart = request.read()
		except http.client.IncompleteRead as icread:
			# Getting at least the partial content
			response += icread.partial
			continue
		else:
			# In this case, saving all
			response += responsePart
		break

	return response


import warnings
import functools
def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func
