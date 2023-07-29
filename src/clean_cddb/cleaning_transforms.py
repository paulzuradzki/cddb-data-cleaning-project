""" cleaning_transforms.py

The idea is that each "clean_df*()" function takes a dataframe
and returns a dataframe after applying a cleaning procedure.

"""

import re

import ftfy
from typing import Any
import pandas as pd

from . import checks


def clean_value_standardize_various_artist(x: Any) -> str:
    x_str: str = str(x).strip()
    various_artist_pattern = r"\b(various|various artist(s)?|var)\b"

    if _match := re.search(various_artist_pattern, re.escape(x_str), re.IGNORECASE):
        if x != "Various":
            return "Various"

    return x_str


def clean_df_standardize_various_artists(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        artist=lambda _df: _df["artist"].apply(clean_value_standardize_various_artist)
    )


################################################################
################################################################


def clean_value_try_to_fix_encoding_errors(x: Any) -> str:
    if not checks.check_col_has_valid_characters(str(x)):
        text_to_try: str = ftfy.fix_text(str(x))
        if checks.check_col_has_valid_characters(text_to_try):
            return text_to_try
    return str(x)


def clean_df_try_to_fix_encoding_errors(
    df: pd.DataFrame, column_name: str
) -> pd.DataFrame:
    df[column_name] = df[column_name].apply(clean_value_try_to_fix_encoding_errors)
    return df


################################################################
################################################################
