# Installation and usage of OpenEBench submitter

## Preconditions

This script needs both `curl` and `jq`:

```bash
sudo apt install curl jq
```

## Usage

You have to create a configuration file. You can take as base the file [sample-submitter.ini](sample-submitter.ini), and you have to put there your credentials.

Then, after you have properly run `pubEnricher.py`, and you have the results in a directory, you have to call the submitter in the next way:

```bash
bash result_submitter.bash config-submitter.ini <pubEnricherResultsDir>
```
