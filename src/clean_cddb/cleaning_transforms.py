""" cleaning_transforms.py

The idea is that each "clean_df*()" function takes a dataframe
and returns a dataframe after applying a cleaning procedure.

"""

import re
import typing
from typing import Any

import ftfy
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


def clean_row_invalid_symbols(row: Any) -> Any:
    if not checks.check_col_has_valid_characters(
        row.get("artist")
    ) or not checks.check_artist_is_valid(row.get("artist")):
        row = pd.Series(["REJECT_ROW - invalid artist"] * len(row), index=row.index)
    return row


@typing.no_type_check
def clean_df_invalid_symbols(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(clean_row_invalid_symbols, axis=1)


def clean_value_invalid_categories(value: Any) -> str:
    if not checks.check_category_is_valid(value):
        return "N/A"
    return str(value)


def clean_df_invalid_categories(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        category=lambda _df: _df["category"].apply(clean_value_invalid_categories)
    )

def clean_row_id_format(row: Any, ids: list) -> Any:
    if not checks.check_id_six_digit_starting_one(row['id']):
        formatted_id = str(int((row['id'])) + 100000)
        if formatted_id not in ids:
            row['id'] = formatted_id
    return row


def clean_df_id_format(df: pd.DataFrame) -> pd.DataFrame:
    list_of_ids = df['id'].tolist()
    return df.apply(clean_row_id_format, ids=list_of_ids, axis=1)


def clean_row_genre_invalid(row: Any) -> Any:
    genre_str = str(row.get("genre"))
    if not checks.check_genre_is_valid(genre_str):
        row = pd.Series(
            ["REJECT_ROW - invalid genre" for _cell in row], index=row.index
        )
    else:
        row["genre"] = (
            genre_str.replace("Data", "N/A")
            .replace("data", "N/A")
            .replace("nan", "N/A")
        )
    return row


@typing.no_type_check
def clean_df_genre_invalid(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(clean_row_genre_invalid, axis=1)


def clean_row_tracks_invalid_symbols(row: Any) -> Any:
    invalid_symbols = set("Ã¤Â")
    for s in invalid_symbols:
        if s in row["tracks"]:
            row = pd.Series(
                ["REJECT_ROW - invalid symbol in tracks" for _cell in row],
                index=row.index,
            )
        return row
    return row


@typing.no_type_check
def clean_df_tracks_invalid_symbols(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(clean_row_tracks_invalid_symbols, axis=1)


def clean_value_year(value: Any) -> Any:
    if checks.check_year_is_numeric(value) and checks.check_year_range_is_valid(value):
        return value
    else:
        return pd.NA


def clean_df_year(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        year=lambda _df: _df["year"].apply(clean_value_year).astype("Int32")
    )


def clean_df_title(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(title=lambda _df: _df["title"].fillna("N/A"))


def clean_df_genre(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(genre=lambda _df: _df["genre"].fillna(_df["category"]))
