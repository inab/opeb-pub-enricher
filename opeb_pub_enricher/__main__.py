#!/usr/bin/python

import argparse
import configparser
import datetime
import os
import sys


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import (
        Optional,
    )

# Next method is used to label methods as deprecated


# from .pub_common import *
from .opeb_queries import OpenEBenchQueries
from .meta_pub_enricher import DEFAULT_BACKEND, RECOGNIZED_BACKENDS_HASH

#############
# Main code #
#############


def main() -> "None":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-F",
        "--full",
        help="Return the full gathered citation results, not the citation stats by year",
        action="count",
        dest="verbosity_level",
        default=0,
    )
    parser.add_argument(
        "--fully-annotated",
        help="Return the reference and citation results fully annotated, not only the year",
        action="store_true",
        dest="do_annotate_citations",
        default=False,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Show the URL statements",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-b",
        "--backend",
        help="Choose the enrichment backend",
        choices=RECOGNIZED_BACKENDS_HASH,
        default="europepmc",
    )
    parser.add_argument(
        "-C",
        "--config",
        help="Config file to pass setup parameters to the different enrichers",
        nargs=1,
        dest="config_filename",
    )
    parser.add_argument(
        "--save-opeb",
        help="Save the OpenEBench content to a file",
        nargs=1,
        dest="save_opeb_filename",
    )
    parser.add_argument(
        "--use-opeb",
        help="Use the OpenEBench content from a file instead of network",
        nargs=1,
        dest="load_opeb_filename",
    )

    dof_group = parser.add_mutually_exclusive_group(required=True)
    dof_group.add_argument(
        "-D",
        "--directory",
        help="Store each separated result in the given directory",
        nargs=1,
        dest="results_dir",
    )
    dof_group.add_argument(
        "-f",
        "--file",
        help="The results file, in JSON format",
        nargs=1,
        dest="results_file",
    )
    dof_group.add_argument(
        "-p",
        "--path",
        help="The path to the results. Depending on the format, it may be a file or a directory",
        nargs=1,
        dest="results_path",
    )

    parser.add_argument(
        "--format",
        help="The output format to be used",
        nargs=1,
        choices=["single", "multiple", "flat"],
        default=["flat"],
        dest="results_format",
    )

    parser.add_argument(
        "cacheDir",
        help="The optional cache directory, to be reused",
        nargs="?",
        default=os.path.join(os.getcwd(), "cacheDir"),
    )
    args = parser.parse_args()

    # Now, let's work!
    verbosity_level = args.verbosity_level

    results_path: "Optional[str]" = None
    results_format: "Optional[str]" = None
    if args.results_path is not None:
        results_path = args.results_path[0]
        results_format = args.results_format[0]

    output_file = args.results_file[0] if args.results_file is not None else None
    if output_file is not None:
        results_path = output_file
        results_format = "single"

    results_dir = args.results_dir[0] if args.results_dir is not None else None
    if results_dir is not None:
        results_path = results_dir
        results_format = "multiple"

    assert results_path is not None
    assert results_format in ("single", "multiple", "flat")

    config_filename = (
        args.config_filename[0] if args.config_filename is not None else None
    )
    save_opeb_filename = (
        args.save_opeb_filename[0] if args.save_opeb_filename is not None else None
    )
    load_opeb_filename = (
        args.load_opeb_filename[0] if args.load_opeb_filename is not None else None
    )
    cache_dir = args.cacheDir
    debug = args.debug

    # Setting the internal verbosity level
    if args.do_annotate_citations:
        # If the flag is set, the verbosity level is raised to 1
        # when the verbosity flag was not set
        if verbosity_level == 0:
            verbosity_level = 1

        # This half-verbosity increment tells to populate the citations
        # with metadata, but not the references or citations
        verbosity_level += 0.5

    # Parsing the config file
    if config_filename is None:
        config = None
    else:
        print("* Reading config file {}".format(config_filename))
        config = configparser.ConfigParser()
        config.read(config_filename)

    # Creating the cache directory, in case it does not exist
    os.makedirs(os.path.abspath(cache_dir), exist_ok=True)
    if results_format != "single":
        os.makedirs(os.path.abspath(results_path), exist_ok=True)

    ChosenEnricher = RECOGNIZED_BACKENDS_HASH.get(args.backend, DEFAULT_BACKEND)
    with ChosenEnricher(cache_dir, config=config, debug=debug) as pub:  # type: ignore[misc]
        # Step 1: fetch the entries with associated pubmed
        opeb_q = OpenEBenchQueries(load_opeb_filename, save_opeb_filename)
        fetchedEntries = opeb_q.fetchPubIds()

        # Step 2: reconcile the DOI <-> PubMed id of the entries

        try:
            print(
                "[{}] Output format: {} Path: {}".format(
                    datetime.datetime.now().isoformat(), results_format, results_path
                )
            )
            print(
                "[{}] Number of tools to query about: {}".format(
                    datetime.datetime.now().isoformat(), len(fetchedEntries)
                )
            )
            sys.stdout.flush()
            pub.reconcilePubIds(
                fetchedEntries,
                results_path=results_path,
                results_format=results_format,
                verbosityLevel=verbosity_level,
            )
            print("[{}] Finished!".format(datetime.datetime.now().isoformat()))
            sys.stdout.flush()
        except Exception as anyEx:
            print(
                "[{}] ERROR: Something went wrong".format(
                    datetime.datetime.now().isoformat()
                ),
                file=sys.stderr,
            )
            print(anyEx, file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            sys.exit(10)
