from typing import Dict

import pandera as pa
import tabulate


def get_check_name_descriptions(schema: pa.DataFrameSchema) -> Dict[str, str]:
    """Get the descriptions for each check function."""

    check_name_descriptions: Dict = {}
    for _column_name, column_obj in schema.columns.items():
        for check in column_obj.checks:
            check_name_descriptions[check.name] = check.description
    return check_name_descriptions


def display_check_name_and_descriptions(check_name_descriptions):
    """Utility function to pretty-print the check names and descriptions.

    The check descriptions contain the source code of the check function.
    """

    for _check_name, description in check_name_descriptions.items():
        print(description)
        print("=" * 50)
        print()


def get_check_func_descriptions(failure_cases_df, schema):
    """Adds descriptions to failure_cases_df by inspecting schema."""
    check_name_and_descriptions: Dict[str, str] = get_check_name_descriptions(schema)
    # display_check_name_and_descriptions(check_name_and_descriptions)

    failure_cases_df_with_source = failure_cases_df.assign(
        check_source_code=lambda _df: _df["check"].map(check_name_and_descriptions)
    )
    return failure_cases_df_with_source


def display_failure_cases_summary(failure_cases_df):
    failure_cases_summary = (
        failure_cases_df.groupby(
            ["column", "check", "check_source_code"], as_index=False
        )
        .size()
        .sort_values(by=["column", "check"])
        .rename(columns={"size": "counts"})
        .loc[:, ["column", "check", "counts", "check_source_code"]]
    )

    report_items = failure_cases_summary.to_dict(orient="records")
    formatted_table: str = tabulate.tabulate(
        report_items, headers="keys", tablefmt="grid"
    )
    print(formatted_table)
