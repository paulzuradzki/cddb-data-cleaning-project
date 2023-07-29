""" cleaning_transforms.py

The idea is that each "clean_df*()" function takes a dataframe 
and returns a dataframe after applying a cleaning procedure.

"""

import re

import ftfy
import pandas as pd

from . import checks


def clean_value_standardize_various_artist(x):
    x = str(x).strip()
    various_artist_pattern = r"\b(various|various artist(s)?|var)\b"
    if _match := re.search(various_artist_pattern, re.escape(x), re.IGNORECASE):
        if x != "Various":
            return "Various"
    return x


def clean_df_standardize_various_artists(df) -> pd.DataFrame:
    return df.assign(
        artist=lambda _df: _df["artist"].apply(clean_value_standardize_various_artist)
    )


################################################################
################################################################


def clean_value_try_to_fix_encoding_errors(x):
    if not checks.check_col_has_valid_characters(x):
        text_to_try = ftfy.fix_text(x)
        if checks.check_col_has_valid_characters(text_to_try):
            return text_to_try
        else:
            return x
    return x


def clean_df_try_to_fix_encoding_errors(df, column):
    df[column] = df[column].apply(clean_value_try_to_fix_encoding_errors)
    return df


################################################################
################################################################
