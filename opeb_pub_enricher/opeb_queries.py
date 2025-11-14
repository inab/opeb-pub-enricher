#!/usr/bin/python

import inspect
import json
import logging
import lzma
from urllib import request
from urllib.error import (
    URLError,
)

from . import pub_common

from typing import (
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Final,
        Mapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        TypeAlias,
        TypedDict,
        Union,
    )

    from .pub_common import (
        BrokenWinner,
        PubmedId,
        PMCId,
        QualifiedWinner,
    )

    RawOpebEntry: TypeAlias = Mapping[str, Any]

    class OpebEntryPubMin(TypedDict):
        found_pubs: "Union[MutableSequence[OpebEntryPub], MutableSequence[Union[QualifiedWinner, BrokenWinner]]]"  # To be improved

    class OpebEntryPub(OpebEntryPubMin, total=False):
        pmid: PubmedId
        doi: str
        pmcid: PMCId

    ParsedOpebEntry = TypedDict(
        "ParsedOpebEntry",
        {
            "@id": str,
            # "id": str,
            "entry_pubs": Sequence[OpebEntryPub],
        },
    )


class OpenEBenchQueries(object):
    OPENEBENCH_SOURCE: "Final[str]" = (
        "https://openebench.bsc.es/monitor/rest/search?projection=publications"
    )
    OPEB_PUB_FIELDS: "Final[Tuple[str, ...]]" = ("pmid", "doi", "pmcid")

    def __init__(
        self,
        load_opeb_filename: "Optional[str]" = None,
        save_opeb_filename: "Optional[str]" = None,
    ):
        # Getting a logger focused on specific classes
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        self.load_opeb_filename = load_opeb_filename
        self.save_opeb_filename = save_opeb_filename

    def parseOpenEBench(
        self, entries: "Sequence[RawOpebEntry]"
    ) -> "Sequence[ParsedOpebEntry]":
        """
        This method takes as input a list of entries fetched from OpenEBench,
        and it returns a a list of dictionaries, whose keys are
        - id (entry id)
        - entry_pubs
        """
        trimmedEntries: "MutableSequence[ParsedOpebEntry]" = []
        for entry in entries:
            entry_pubs: "MutableSequence[OpebEntryPub]" = []
            for pub in entry.get("publications", []):
                if pub is not None:
                    filtered_pub = {
                        field: pub[field].strip()
                        if isinstance(pub[field], str)
                        else pub[field]
                        for field in filter(
                            lambda field: field in pub, self.OPEB_PUB_FIELDS
                        )
                    }
                    filtered_pub["found_pubs"] = []
                    if len(filtered_pub) > 0:
                        entry_pubs.append(cast("OpebEntryPub", filtered_pub))

            if len(entry_pubs) > 0:
                trimmedEntries.append(
                    {
                        "@id": entry["@id"],
                        "entry_pubs": entry_pubs,
                    }
                )

        return trimmedEntries

    def fetchPubIds(
        self, sourceURL: "str" = OPENEBENCH_SOURCE
    ) -> "Sequence[ParsedOpebEntry]":
        """
        This method fetches from OpenEBench the list of publications for each
        entry, and it returns a list of dictionaries, whose keys are
        - id (entry id)
        - pubmed_idsmay get suffix added by low-le
        - doi_ids
        The reconciliation is done later
        """
        try:
            if self.load_opeb_filename:
                if self.load_opeb_filename.endswith(".xz"):
                    with lzma.open(self.load_opeb_filename, mode="rb") as resp:
                        raw_opeb = resp.read()
                else:
                    with open(self.load_opeb_filename, mode="rb") as resp:
                        raw_opeb = resp.read()
            else:
                req = request.Request(sourceURL)
                # This fixes an issue, as the API answers in several flavours
                req.add_header("Accept", "application/json")
                with request.urlopen(req) as resp:
                    raw_opeb = pub_common.full_http_read(resp)

            if self.save_opeb_filename:
                with open(self.save_opeb_filename, mode="wb") as savefile:
                    savefile.write(raw_opeb)

            retval = json.loads(raw_opeb.decode("utf-8"))

            return self.parseOpenEBench(retval)

        except URLError as ue:
            self.logger.exception("could not fetch {0}".format(sourceURL))
            raise ue
        except json.JSONDecodeError as jde:
            self.logger.error("Bad-formed JSON: " + jde.msg)
            raise jde
        except Exception as anyEx:
            self.logger.exception("Something unexpected happened in fetchPubIds")
            raise anyEx
