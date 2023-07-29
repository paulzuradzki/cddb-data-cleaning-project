# `clean_cddb`

## Description

This is a package for cleaning the CDDB (Compact Disc Database) collection of music albums and tracks data.

## Usage

* See `scripts/run_clean_cddb.py`
* Or see `notebooks/cleaning_cddb.ipynb` for an annotated example

Example usage
```python
import logging

import pandas as pd
import pandera as pa

import clean_cddb

filepath = "../data/input/cddb.tsv"
source_df = pd.read_csv(filepath, sep="\t", dtype="str")

try:
    validated_df = clean_cddb.schema(source_df, lazy=True)
    logging.info("Validation success.")
except pa.errors.SchemaErrors as err:
    logging.info("Validation failure.")
    logging.info(err)
    failure_cases_df = err.failure_cases
```

*Sample first 5 values from source dataframe*<br>

```python
source_df.head()
```

| artist                    | category   | genre   | title                                        | tracks                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | year   |     id | merged_values   |
|:--------------------------|:-----------|:--------|:---------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------|-------:|:----------------|
| Backstreet Boys           | blues      | Pop     | Millennium                                   | Larger Than Life \| I Want It That Way \| Show Me The Meaning Of Being Lonely \| It's Gotta Be You \| I Need You Tonight \| Don't Want You Back \| Don't  Wanna Lose You Now \| The One \| Back To Your Heart \| Spanish Eyes \| No One Else Comes Close \| The Perfect Fan \| I'll Be There For You \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|                                                                                                                                                                                                       | <NA>   |  10000 | <NA>            |
| Various                   | data       | <NA>    | Frankfurt Trance Vol. 04 cd1                 | DJ Tom Stevens VS. Fridge - Outface 2000 (Radio Mix) \| Alice Deejay - Better Off Alone (Signum Remix) \| Tillmann Uhrmacher Feat. Peter Ries - Bassfly (Original Mix) \| DJ 2 L 8 - Too Late \| Time Square - Invisible Girl (Future Breeze Remix) \| Cirillo - Across The Soundline \| DJ Leon & Jam X - Hold It \| Sean Dexter - Synthetica (Extended Mix) \| DJ BjÃ¶rn - On A Mission (Original Mix) \| 8Voice - Music Hypnotizes 2000 \| Alex Apollo - Jahr 2000 \| Headroom - Utopia (Radio Mix) \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \| | <NA>   | 100001 | <NA>            |
| NO RETURN                 | data       | Data    | Self Mutilation                              | Do or Die \| Truth and Reality \| Lost \| Soul Extractor \| Sadistic Desire \| The True Way \| Fanatic Mind \| Individualistic Ideal \| One Life \| Trail of Blood \| Sect \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|                                                                                                                                                                                                                                                                                                                         | <NA>   | 100002 | <NA>            |
| Ã¤Â¸Â­Ã¦?â€˜Ã©â€ºâ€¦Ã¤Â¿Å | data       | Pop     | Ã¦Æ’Â³Ã£?â€žÃ¥â€¡ÂºÃ£?Â®Ã£?â€¹Ã£?â€˜Ã£â€šâ€° | Ã§â€ºâ€ Ã¥Â¸Â°Ã£â€šÅ  \| Ã£?â€žÃ£?Â¤Ã£?â€¹Ã¨Â¡â€”Ã£?Â§Ã¤Â¼Å¡Ã£?Â£Ã£?Å¸Ã£?ÂªÃ£â€šâ€° \| Ã©Â¢Â¨Ã£?Â®Ã£?ÂªÃ£?â€žÃ¦â€”Â¥ \| Ã£?Å¸Ã£?Â Ã£?Å Ã¥â€°?Ã£?Å’Ã£?â€žÃ£?â€ž \| Ã£?ÂµÃ£â€šÅ’Ã£?â€šÃ£?â€ž \| Ã£?â€šÃ£â€š?Ã©?â€™Ã¦ËœÂ¥ \| Ã¤Â¿ÂºÃ£?Å¸Ã£?Â¡Ã£?Â®Ã¦â€”â€¦ \| Ã§â„¢Â½Ã£?â€žÃ¥Â¯Â«Ã§Å“Å¾Ã©Â¤Â¨ \| Ã£?â€¢Ã£?â„¢Ã£â€šâ€°Ã£?â€žÃ¦â„¢â€šÃ¤Â»Â£ \| Ã¥Â¤Å“Ã¨Â¡Å’Ã¥Ë†â€”Ã¨Â»Å  \| Ã£?â€šÃ£?ÂªÃ£?Å¸Ã£â€šâ€™Ã¦â€žâ€ºÃ£?â„¢Ã£â€šâ€¹Ã§Â§? \| Ã©?â€™Ã¦ËœÂ¥Ã¨Â²Â´Ã¦â€”? \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|                                                 | 1989   | 100003 | <NA>            |
| Emanuel                   | data       | Data    | Felicidade                                   | Felicidade quando o telefone toca \| Vem bailar o tic tic \| Quero que sejas minha e de mais ninguem \| Eu sei que me amas \| O melhor que hÃ¡ \| Minha vizinha  deixa me a bater mal \| S. JoÃ£o Ã© foliÃ£o \| SÃ³ quero o teu carinho \| tudo farei para ter a tua paixÃ£o \| serÃ¡s sempre minha \| Vem bailar o tic tic verÃ§Ã£o dance \| Mix \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|  \|                                                                                                                                                      | 1998   | 100004 | <NA>            |

<br>

*Sample first 5 values of failure cases dataframe*<br>

```python
failure_cases_df_markdown.head()
```

| schema_context   | column   | check                      | check_number   | failure_case                                                      |   index |
|:-----------------|:---------|:---------------------------|:---------------|:------------------------------------------------------------------|--------:|
| Column           | artist   | not_nullable               | <NA>           | <NA>                                                              |    9030 |
| Column           | title    | Check for invalid symbols. | 0              | L'intÃ©grale des sonates pour flÃ»te (CD2-2)                      |    8657 |
| Column           | title    | Check for invalid symbols. | 0              | MatthÃ¤us-Passion (Akademie fÃ¼r alte Musik Berlin, CD 2)         |    8666 |
| Column           | title    | Check for invalid symbols. | 0              | Georg Friedrich HÃ¤ndel - Der Messias (AuszÃ¼ge in engl. Sprache) |    8665 |
| Column           | title    | Check for invalid symbols. | 0              | String Quartets, Op. 33 (disc 1/2)                                |    8662 |


Validation check summary

| column   | check                                          |   counts | check_source_code   |
|----------|------------------------------------------------|----------|---------------------|
| artist   | Check for invalid artist values.               |      697 | def check_artist_is_valid(x):
    """Check for invalid artist values."""
    
    various_artist_pattern = r"\b(various|various artist(s)?|var)\b"
    try:
        # match on variations of "various", "various artist", "various artists", and "var"; case-insensitive
        # also match 2 or more question marks like "????" or "?? ??"
        if _match := re.search(
            various_artist_pattern, re.escape(x), re.IGNORECASE
        ):
            if x != "Various":
                return False
            
        if '??' in x:
            return False
    except:
        return False

    return True                     |
| category | Check for invalid categories.                  |       89 | def check_category_is_valid(x):
    """Check for invalid categories."""
    
    if x in ['blues', 'classical', 'country', 'folk', 'jazz', 'misc', 'newage', 'reggae', 'rock', 'soundtrack']:
        return True
    else:
        return False                     |
| genre    | Check for invalid genres.                      |        1 | def check_genre_is_valid(x):
    """Check for invalid genres."""
    try:
        if '--' in x:
            return False
    except:
        return False
    else:
        return True                     |
| id       | Check that the length of 'id' is 6 characters. |      477 | lambda x: len(x)==6 |
| title    | Check for invalid symbols.                     |     2456 | def check_col_has_valid_characters(x):
    """Check for invalid symbols."""
    
    # consider NaNs and floats to be invalid
    if not isinstance(x, str):
        return False

    invalid_symbols = set("-.\^z¤¦©¬®¯°±²³´µ¶¸¹º»¼½¾¿ÀÂÃÄÅÆÇÈÌÕÖÜàâäåçèéïð÷øùû˜ѼҸ€中俊劇四団季雅�")
    for char in x:
        if char in invalid_symbols:
            return False
        
    chinese_pattern = re.compile(r'[\u4e00-\u9fff]')  # CJK Unified Ideographs
    japanese_pattern = re.compile(r'[\u3040-\u30ff\u31f0-\u31ff\u3200-\u9faf]')  # Hiragana, Katakana, CJK Unified Ideographs Extension A
    has_chinese = bool(chinese_pattern.search(x))
    has_japanese = bool(japanese_pattern.search(x))
    
    if has_chinese or has_japanese:
        return False

    return True                     |
| tracks   | Check for tracks possibly using numeric prefix |      599 | def check_track_has_numeric_prefix(x):
    for keyword in ['disk', 'track', 'title', '01']:
        if keyword in x.lower():
            return False        
    return True                     |
| year     | Check if year is numeric.                      |       28 | def check_year_is_numeric(x):
    """Check if year is numeric."""
    
    try:
        if not str(int(x)).isnumeric():
            return False
    except:
        return False
    return True                     |
| year     | Check that year is between 1950 and 2030.      |       56 | def check_year_range_is_valid(x):
    """Check that year is between 1950 and 2030."""
    
    try:
        int(x)
    except:
        return False

    if int(x) > 1950 and int(x) < 2030:
        return True
    else:
        return False                     |
        
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