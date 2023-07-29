# `clean_cddb`

## Description

This is a package for cleaning the CDDB (Compact Disc Database) collection of music albums and tracks data.

## Usage

See `notebooks/cleaning_cddb.ipynb` for annotated examples.

Example usage
```python
import logging

import pandas as pd
import pandera as pa

import clean_cddb

try:
    validated_df = clean_cddb.schema(source_df, lazy=True)
    logging.info("Validation success. No failure cases detected.")
except pa.errors.SchemaErrors as err:
    logging.info("Validation failure. Failure cases detected.")
    logging.debug(err)
    failure_cases_df = err.failure_cases
```

With `failure_cases_df`, we can perform analysis or query the failure cases. This is useful for high-level summaries (e.g., "how many observations fail a given validation check?") or detailed examples (e.g., "show example failure cases where the year was out of range").

## Setup

#### Option 1: Build from source

Use this if you want the package source code locally.

```bash
$ git clone https://github.com/paulzuradzki/cs513-data-cleaning-project
$ cd cs513-data-cleaning-project
$ python3 -m venv venv
$ source /venv/bin/activate
(venv) $ python -m pip install --upgrade pip
(venv) $ python -m pip install -r requirements.txt
(venv) $ python -m pip install .

# use the flag `-e` (`--editable` mode) if you plan to edit the package source inside src/
# this creates a symbolic link between your virtual environment site-packages and your local directory
# that way, you don't have to re-install as you edit
(venv) $ python -m pip install -e .
```

#### Option 2: Install with pip from GitHub

* Use this if you want to import `clean_cddb` into another project; e.g., `import clean_cddb`
* This package is not published on PyPI.org; however, the source is public on GitHub and you can install using the git repository URL like so:

```bash
$ python3 -m venv venv
$ source /venv/bin/activate
(venv) $ python -m pip install --upgrade pip
(venv) $ python -m pip install git+https://github.com/paulzuradzki/cs513-data-cleaning-project.git
```

#### Testing

```bash
python -m pytest
```