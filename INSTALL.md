# OpenEBench references and citations enricher

This program is written for Python 3.5 and later, and it depends on standard libraries, plus the ones declared in [requirements.txt](requirements.txt).

* In order to install the dependencies you need `pip` and `venv` modules.
	- `pip` is available in many Linux distributions (Ubuntu package `python3-pip`, CentOS EPEL package `python-pip`), and also as [pip](https://pip.pypa.io/en/stable/) Python package.
	- `venv` is also available in many Linux distributions (Ubuntu package `python3-venv`). In some of them is integrated into the Python 3.5 (or later) installation.


* The creation of a virtual environment and installation of the dependencies in that environment is done running:
  
```bash
python3 -m venv .pyenv
source .pyenv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
  
* The program can be run using next command line:
  
```bash
source .pyenv/bin/activate
python pubEnricher.py -h
```

* If you upgrades your Python installation, or you move this folder to a different location after following this instructions, you may need to remove and reinstall the virtual environment.
