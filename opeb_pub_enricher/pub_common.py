#!/usr/bin/python

import datetime
import functools
import http.client
import os
import re
import time
import warnings

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import (
        Any,
        Callable,
        Final,
        Iterator,
        NewType,
        Optional,
        Pattern,
        Sequence,
        Tuple,
        TypeAlias,
        TypedDict,
        TypeVar,
        Union,
    )

    # Alias types declaration
    PubmedId: TypeAlias = Union[int, str]
    NormalizedPMCId: TypeAlias = str
    PMCId: TypeAlias = Union[int, NormalizedPMCId]
    DOIId: TypeAlias = str
    PublishId: TypeAlias = Union[DOIId, PMCId, PubmedId]
    CURIE: TypeAlias = str
    RT = TypeVar("RT")

    UnqualifiedId = NewType("UnqualifiedId", str)
    SourceId = NewType("SourceId", str)
    EnricherId = NewType("EnricherId", str)

    QualifiedId: TypeAlias = Tuple[SourceId, UnqualifiedId]

    MetaQualifiedId: TypeAlias = Tuple[EnricherId, SourceId, UnqualifiedId]

    class AbstractWinner(TypedDict):
        curie_ids: Sequence[CURIE]
        broken_curie_ids: Sequence[CURIE]
        pmid: Optional[PubmedId]
        doi: Optional[DOIId]
        pmcid: Optional[PMCId]

    class QualifiedWinner(AbstractWinner):
        id: UnqualifiedId
        source: SourceId

    class BrokenWinner(AbstractWinner):
        id: None
        source: None
        reason: str


class Timestamps(object):
    @staticmethod
    def LocalTimestamp(
        theDate: "datetime.datetime" = datetime.datetime.now(),
    ) -> "datetime.datetime":
        utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
        return theDate.replace(
            tzinfo=datetime.timezone(offset=datetime.timedelta(seconds=-utc_offset_sec))
        )

    @staticmethod
    def UTCTimestamp(
        theUTCDate: "datetime.datetime" = datetime.datetime.utcnow(),
    ) -> "datetime.datetime":
        return theUTCDate.replace(tzinfo=datetime.timezone.utc)

    @functools.cache
    @staticmethod
    def BiggestTimestamp() -> "datetime.datetime":
        return Timestamps.UTCTimestamp(datetime.datetime.max)


def pmid2curie(pubmed_id: "PubmedId") -> "CURIE":
    return "pmid:" + str(pubmed_id)


def normalize_pmcid(pmc_id: "PMCId") -> "NormalizedPMCId":
    """
    Normalize PMC ids, in case they lack the PMC prefix
    """
    pmc_id_norm = str(pmc_id)
    if pmc_id_norm.isdigit():
        pmc_id_norm = "PMC" + pmc_id_norm

    return pmc_id_norm


PMC_PATTERN: "Final[Pattern[str]]" = re.compile(r"^PMC(.*)", re.I)


def denormalize_pmcid(pmc_id: "PMCId") -> "PMCId":
    found_pat = PMC_PATTERN.search(pmc_id if isinstance(pmc_id, str) else str(pmc_id))
    if found_pat:
        # It was normalized
        pmc_id = found_pat.group(1)

    return pmc_id


def pmcid2curie(pmc_id: "PMCId") -> "CURIE":
    pmc_id_norm = normalize_pmcid(pmc_id)
    return "pmc:" + pmc_id_norm


CITATIONS_KEYS = ("citations", "citation_count")
REFERENCES_KEYS = ("references", "reference_count")


# This method does the different reads and retries
# in case of partial contents
def full_http_read(resp: "http.client.HTTPResponse") -> "bytes":
    # The original bytes
    response = b""
    while True:
        try:
            # Try getting it
            responsePart = resp.read()
        except http.client.IncompleteRead as icread:
            # Getting at least the partial content
            response += icread.partial
            continue
        else:
            # In this case, saving all
            response += responsePart
        break

    return response


def deprecated(func: "Callable[..., RT]") -> "Callable[..., RT]":
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    @functools.wraps(func)
    def new_func(*args: "Any", **kwargs: "Any") -> "RT":
        warnings.simplefilter("always", DeprecationWarning)  # turn off filter
        warnings.warn(
            "Call to deprecated function {}.".format(func.__name__),
            category=DeprecationWarning,
            stacklevel=2,
        )
        warnings.simplefilter("default", DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    return new_func


# Next method has been borrowed from FlowMaps
def scantree(path: "Union[str, os.PathLike[str]]") -> "Iterator[os.DirEntry[str]]":
    """Recursively yield DirEntry objects for given directory."""

    hasDirs = False
    for entry in os.scandir(path):
        # We are avoiding to enter in loops around '.' and '..'
        if entry.is_dir(follow_symlinks=False):
            if entry.name[0] != ".":
                hasDirs = True
        else:
            yield entry

    # We are leaving the dirs to the end
    if hasDirs:
        for entry in os.scandir(path):
            # We are avoiding to enter in loops around '.' and '..'
            if entry.is_dir(follow_symlinks=False) and entry.name[0] != ".":
                yield entry
                yield from scantree(entry.path)
