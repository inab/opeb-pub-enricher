#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import setuptools

# In this way, we are sure we are getting
# the installer's version of the library
# not the system's one
setupDir = os.path.dirname(__file__)
sys.path.insert(0, setupDir)

from opeb_pub_enricher import __version__ as ope_version
from opeb_pub_enricher import __author__ as ope_author
from opeb_pub_enricher import __license__ as ope_license

# Populating the long description
readme_path = os.path.join(setupDir, "README.md")
with open(readme_path, "r") as fh:
    long_description = fh.read()

# Populating the install requirements
requirements = []
requirements_path = os.path.join(setupDir, "requirements.txt")
if os.path.exists(requirements_path):
    with open(requirements_path, mode="r", encoding="utf-8") as f:
        egg = re.compile(r"#[^#]*egg=([^=&]+)")
        for line in f.read().splitlines():
            m = egg.search(line)
            requirements.append(line if m is None else m.group(1))

setuptools.setup(
    name="opeb-pub-enricher",
    version=ope_version,
    author=ope_author,
    author_email="jose.m.fernandez@bsc.es",
    license=ope_license,
    description="OpenEBench publications enricher",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/inab/opeb-pub-enricher",
    project_urls={"Bug Tracker": "https://github.com/inab/opeb-pub-enricher/issues"},
    packages=setuptools.find_packages(),
    package_data={},
    scripts=[
        "pubEnricher.py",
    ],
    install_requires=requirements,
    # See https://gist.github.com/nazrulworld/3800c84e28dc464b2b30cec8bc1287fc
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.5",
)
