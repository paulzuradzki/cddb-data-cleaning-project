"""
High-level steps
- Conduct checks
- Apply cleaning transformations
- Output album- and track-level data sets
- Compare before/after cleaning with summary and detailed examples
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd
import pandera as pa

import clean_cddb
from clean_cddb.utils import (
    get_check_func_descriptions,
    get_failure_cases_summary_as_formatted_table,
)


def df_to_var(df: pd.DataFrame, var_name: str) -> pd.DataFrame:
    globals()[var_name] = df
    return df


pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(process)d - %(levelname)s - %(message)s",
)

#######################
# Import data
#######################

logging.info("Reading cddb.tsv...")
filepath = "./data/input/cddb.tsv"
source_df = pd.read_csv(filepath, sep="\t", dtype="str")

#######################
# Validation: source_df
#######################

logging.info("Validating source_df...")
try:
    validated_df = clean_cddb.schema(source_df, lazy=True)
    logging.info("Validation success. No failure cases detected.")
except pa.errors.SchemaErrors as err:
    logging.info("Validation failure. Failure cases detected.")
    logging.debug(err)
    before_cleaning_failure_cases_df = err.failure_cases

logging.info("Reporting on failure cases for source_df...")
before_cleaning_failure_cases_df = before_cleaning_failure_cases_df.pipe(
    get_check_func_descriptions, clean_cddb.schema
)

before_cleaning_failure_cases_summary = (
    before_cleaning_failure_cases_df.groupby(["column", "check"], as_index=False)
    .size()
    .sort_values(by=["column", "check"])
    .rename(columns={"size": "counts"})
)

before_cleaning_failure_cases_summary_table = (
    get_failure_cases_summary_as_formatted_table(before_cleaning_failure_cases_df)
)

#######################
# Cleaning
#######################

logging.info("Applying cleaning operations...")
clean_df_before_drops = None
clean_df = (
    source_df.pipe(clean_cddb.clean_df_standardize_various_artists)
    .pipe(clean_cddb.clean_df_id_format)
    .pipe(clean_cddb.clean_df_try_to_fix_encoding_errors, "artist")
    .pipe(df_to_var, "clean_df_artist_transforms_only")
    .pipe(clean_cddb.clean_df_invalid_symbols)
    .pipe(clean_cddb.clean_df_invalid_categories)
    .pipe(clean_cddb.clean_df_genre_invalid)
    .pipe(clean_cddb.clean_df_tracks_invalid_symbols)
    .pipe(clean_cddb.clean_df_year)
    .pipe(clean_cddb.clean_df_title)
    .pipe(clean_cddb.clean_df_genre)
    # Save an intermediate dataframe prior to dropping records
    # so we can compare with source_df later
    .pipe(df_to_var, "clean_df_before_drops")
    # Drop rows with "REJECT_ROW*" prefix
    .query("~id.str.contains('REJECT_ROW')")
    .drop(columns=["merged_values"])
)

#######################
# Validation: clean_df
#######################

logging.info("Validating clean_df...")
# apply schema to clean_df
try:
    validated_df = clean_cddb.schema(clean_df, lazy=True)
    logging.info("Validation success. No failure cases detected.")
except pa.errors.SchemaErrors as err:
    logging.info("Validation failure. Failure cases detected.")
    logging.debug(err)
    after_cleaning_failure_cases_df = err.failure_cases

logging.info("Reporting on failure cases for clean_df...")
after_cleaning_failure_cases_df = after_cleaning_failure_cases_df.pipe(
    get_check_func_descriptions, clean_cddb.schema
)

after_cleaning_failure_cases_summary = (
    after_cleaning_failure_cases_df.groupby(["column", "check"], as_index=False)
    .size()
    .sort_values(by=["column", "check"])
    .rename(columns={"size": "counts"})
)

#######################
# Evaluation
#######################

logging.info("Creating evaluation summary of before-vs-after cleaning...")
evaluation_summary_df = before_cleaning_failure_cases_summary.merge(
    after_cleaning_failure_cases_summary,
    on=["column", "check"],
    how="outer",
    suffixes=["_before_cleaning", "_after_cleaning"],
).fillna("")

logging.info("Creating detailed row-level comps of before-vs-after cleaning...")
comps_df: pd.DataFrame = (
    source_df.compare(
        clean_df_before_drops,  # type: ignore [arg-type]
        result_names=("before_cleaning", "after_cleaning"),
    )
    .astype("object")  # type: ignore [arg-type]
    .fillna("")
)

columns_to_compare = ["artist", "category", "genre", "title", "tracks", "year", "id"]
comps_df_formatted = (
    comps_df.astype(str)
    .stack()
    .reset_index()
    .rename(columns={"level_0": "row_id", "level_1": "before_or_after"})
    .drop(columns=["merged_values"])
    .groupby(["row_id"], as_index=False)[columns_to_compare]
    .agg(lambda row: "  =>  ".join(row))
    .replace("^(  =>  )$", "", regex=True)
)

################################
# Transform to track-level data
################################

track_level_df = (
    # Start with original dataframe
    clean_df
    # Split 'tracks' on pipe into an array; we can "explode" it later
    .pipe(lambda _df: _df.assign(tracks=_df["tracks"].str.split("|")))
    # "explode"/expand from "tracks" array in to 1 observation per track
    # perform a self-join to CD data set; the CD-level data will repeat for each track
    .pipe(
        lambda _df: _df.merge(
            _df["tracks"].explode(), left_index=True, right_index=True
        )
    )
    .pipe(df_to_var, "df_after_explode")
    # Make new 'tracks' field; strip ' ' empty space track names to '' empty string
    .pipe(lambda _df: _df.assign(tracks=_df["tracks_y"].str.strip()))
    # Don't need these fields anymore
    .drop(columns=["tracks_x", "tracks_y"])
    # Filter out empty string track names
    .query("tracks!=''")
    .pipe(df_to_var, "df_after_empty_track_name_filter")
    .reset_index(drop=True)
    .loc[:, ["id", "tracks"]]
    .reset_index()
    .rename(
        columns={
            "id": "album_row_id",
            "index": "track_id",
            "tracks": "track_name",
        }
    )
)

#######################
# Export data
#######################

dfs = {
    "source_df": source_df,
    "before_cleaning_failure_cases_df": before_cleaning_failure_cases_df,
    "before_cleaning_failure_cases_summary": before_cleaning_failure_cases_summary,
    "clean_df": clean_df,
    "after_cleaning_failure_cases_df": after_cleaning_failure_cases_df,
    "after_cleaning_failure_cases_summary": after_cleaning_failure_cases_summary,
    "evaluation_summary_df": evaluation_summary_df,
    "comps_df": comps_df,
    "comps_df_formatted": comps_df_formatted,
    "track_level_df": track_level_df,
}

Path("./data/output/csv").mkdir(exist_ok=True)
Path("./data/output/sqlite_db").mkdir(exist_ok=True)

conn = sqlite3.connect("./data/output/sqlite_db/cddb.db")

logging.info("Exporting data sets...")
logging.info("Output directory: ./data/output/")
for df_name, df in dfs.items():
    df.to_sql(df_name, con=conn, if_exists="replace")
    df.to_csv(f"./data/output/csv/{df_name}.csv", index=False)

output_path_checks_summary_table = (
    "./data/output/before_cleaning_failure_cases_summary_table.txt"
)
with open(output_path_checks_summary_table, "w") as f:
    f.write(before_cleaning_failure_cases_summary_table)
    f.write("\n")
    logging.info(f"Wrote: {output_path_checks_summary_table}")

df_names = str(list(dfs.keys()))
logging.info(f"Created SQL tables and CSVs for to following dataframes:\n{df_names}")
