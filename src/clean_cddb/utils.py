import logging
import typing
from typing import Any, Dict, Hashable, List, Union

import pandas as pd
import pandera as pa
import tabulate


def get_check_name_descriptions(schema: pa.DataFrameSchema) -> Dict[str, str]:
    """Get the descriptions for each check function."""

    check_name_descriptions: Dict[str, str] = {}
    for _column_name, column_obj in schema.columns.items():
        for check in column_obj.checks:
            check_name_descriptions[check.name] = check.description
    return check_name_descriptions


def display_check_name_and_descriptions(
    check_name_descriptions: Dict[str, str]
) -> None:
    """Utility function to pretty-print the check names and descriptions.

    The check descriptions contain the source code of the check function.
    """

    for _check_name, description in check_name_descriptions.items():
        print(description)
        print("=" * 50)
        print()


def get_check_func_descriptions(
    failure_cases_df: pd.DataFrame, schema: pa.DataFrameSchema
) -> pd.DataFrame:
    """Adds descriptions to failure_cases_df by inspecting schema."""
    check_name_and_descriptions: Dict[str, str] = get_check_name_descriptions(schema)
    # display_check_name_and_descriptions(check_name_and_descriptions)

    failure_cases_df_with_source = failure_cases_df.assign(
        check_source_code=lambda _df: _df["check"].map(check_name_and_descriptions)
    )
    return failure_cases_df_with_source


@typing.no_type_check
def get_failure_cases_summary_as_formatted_table(
    failure_cases_df: pd.DataFrame,
) -> None:
    failure_cases_summary: pd.DataFrame = (
        failure_cases_df.groupby(
            ["column", "check", "check_source_code"], as_index=False
        )
        .size()
        .sort_values(by=["column", "check"])
        .rename(columns={"size": "counts"})
        .loc[:, ["column", "check", "counts", "check_source_code"]]
    )

    report_items: List[Dict[Hashable, Any]] = failure_cases_summary.to_dict(
        orient="records"
    )
    formatted_table: str = tabulate.tabulate(
        report_items, headers="keys", tablefmt="grid"
    )
    return formatted_table


@typing.no_type_check
def log_df_change(
    after_df: pd.DataFrame, before_df: pd.DataFrame, operation_label: str
) -> pd.DataFrame:
    comps_df: pd.DataFrame = before_df.compare(
        after_df, result_names=("before", "after")
    )

    if not comps_df.empty:
        comps_df_sample_markdown: str = (
            comps_df.sample(5, random_state=0).fillna("").to_markdown()
        )
    else:
        comps_df_sample_markdown: Union[str, None] = None

    n_rows, _ = comps_df.shape

    log_message = "Cleaning operation"
    log_message += f"\noperation_label: {operation_label}"
    log_message += f"\ncleaning operation_label: {operation_label}"
    log_message += f"\nNumber of rows affected: {n_rows}"
    log_message += (
        f"\nColumns affected: {set([col for col, _ in comps_df.columns.tolist()])}"
    )
    log_message += f"\nExamples:\n{comps_df_sample_markdown}\n"
    log_message += f"{'='*100}\n"
    log_message += f"{'='*100}\n"

    logging.info(log_message)

    # Return the same dataframe
    return after_df
