# OpenEBench publications enricher

This is the code repository of the OpenEBench publications enricher used in OpenEBench.

Depending on the chosen flags, it fetches from OpenEBench technical toolbox REST API (which follows [next JSON Schema](https://openebench.bsc.es/monitor/tool/tool.json), [source from repo here](https://github.com/inab/elixibilitas/blob/master/java/elixibilitas-rest/src/main/resources/META-INF/resources/tool.json)) the list of tools, along with their registered PubMed Id , DOI or PubMedCentral Id. Only tools with such information are considered.

The extracted publication identifiers are validated against the enabled publication repositories (currently supported PubMed, EuropePMC and WikiData), gathering for the valid identifiers additional information, like the journal, year, authors, references and citations.

## Installation requirements

This program was initially written for Python 3.5 and later. The installation procedure is in [INSTALL.md](INSTALL.md).

## Usage
Once the program is installed, and its environment activated, you can see the different options using `-h` flag:

```
usage: pubEnricher.py [-h] [-F] [--fully-annotated] [-d]
                      [-b {europepmc,pubmed,wikidata,meta}]
                      [-C CONFIG_FILENAME] [--save-opeb SAVE_OPEB_FILENAME]
                      [--use-opeb LOAD_OPEB_FILENAME]
                      (-D RESULTS_DIR | -f RESULTS_FILE | -p RESULTS_PATH)
                      [--format {single,multiple,flat}]
                      [cacheDir]

positional arguments:
  cacheDir              The optional cache directory, to be reused

optional arguments:
  -h, --help            show this help message and exit
  -F, --full            Return the full gathered citation results, not the
                        citation stats by year
  --fully-annotated     Return the reference and citation results fully
                        annotated, not only the year
  -d, --debug           Show the URL statements
  -b {europepmc,pubmed,wikidata,meta}, --backend {europepmc,pubmed,wikidata,meta}
                        Choose the enrichment backend
  -C CONFIG_FILENAME, --config CONFIG_FILENAME
                        Config file to pass setup parameters to the different
                        enrichers
  --save-opeb SAVE_OPEB_FILENAME
                        Save the OpenEBench content to a file
  --use-opeb LOAD_OPEB_FILENAME
                        Use the OpenEBench content from a file instead of
                        network
  -D RESULTS_DIR, --directory RESULTS_DIR
                        Store each separated result in the given directory
  -f RESULTS_FILE, --file RESULTS_FILE
                        The results file, in JSON format
  -p RESULTS_PATH, --path RESULTS_PATH
                        The path to the results. Depending on the format, it
                        may be a file or a directory
  --format {single,multiple,flat}
                        The output format to be used
```

The chosen output format may change the way the results are recovered and some flags implemented.

The most prominent change has been the `flat` format, which implies writing a separate file for each searched tool and found publication, avoiding duplications in the original, nested format. It also generates a `manifest.json` file, describing the generated files.

Although a config file is not needed to run the program, it is needed to customize its behaviour. A sample config file is available at [sample-config.ini](sample-config.ini), with embedded descriptions.
