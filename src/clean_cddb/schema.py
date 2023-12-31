"""schema.py

Pandera schema for CDDB dataset.
* A "Pandera Schema" refers to a set of validation rules
  and data quality checks defined for a pandas DataFrame.
* It allows you to specify the expected data types, formats, constraints,
  and other conditions that the data in the DataFrame should adhere to.
* Using Pandera, you can create a schema for a DataFrame,
  apply the schema to the data, and check whether the data meets the defined
  validation rules.

Notes
* `element_wise=True` applies the check to each element
   as opposed to the entire column/array.
    * Column-level checks are for checking things like "is the mean of the this
      column within X range".
    * Element-wise checks are for checking things like "is a given row value
      within a valid range".
* we use `inspect.getsource(Callable)` to get the source code for a check
  function so we can include it later in reporting
* `checks.check_*()` functions
    * These are data quality checks
    * Returning false for a given value indicates a failure case.
    * One check may be used across multiple columns.
    * Pandera has its own built-in checks for common operations
     (range check, check if value is member of valid set, etc.)
    * These are user-defined checks.
"""

import inspect

import pandera as pa

from . import checks


schema = pa.DataFrameSchema(
    {
        "artist": pa.Column(
            object,
            nullable=False,
            checks=[
                pa.Check(
                    checks.check_artist_is_valid,
                    element_wise=True,
                    name=checks.check_artist_is_valid.__doc__,
                    description=inspect.getsource(checks.check_artist_is_valid),
                ),
                pa.Check(
                    checks.check_col_has_valid_characters,
                    element_wise=True,
                    name=checks.check_col_has_valid_characters.__doc__,
                    ignore_na=False,
                    description=inspect.getsource(
                        checks.check_col_has_valid_characters
                    ),
                ),
            ],
        ),
        "category": pa.Column(
            object,
            nullable=False,
            checks=[
                pa.Check(
                    checks.check_category_is_valid,
                    element_wise=True,
                    name=checks.check_category_is_valid.__doc__,
                    description=inspect.getsource(checks.check_category_is_valid),
                )
            ],
        ),
        "genre": pa.Column(
            object,
            nullable=False,
            checks=[
                pa.Check(
                    checks.check_genre_is_valid,
                    element_wise=True,
                    name=checks.check_genre_is_valid.__doc__,
                    description=inspect.getsource(checks.check_genre_is_valid),
                )
            ],
        ),
        "title": pa.Column(
            object,
            nullable=False,
            checks=[
                pa.Check(
                    checks.check_col_has_valid_characters,
                    element_wise=True,
                    name=checks.check_col_has_valid_characters.__doc__,
                    ignore_na=False,
                    description=inspect.getsource(
                        checks.check_col_has_valid_characters
                    ),
                )
            ],
        ),
        "year": pa.Column(
            "Int32",
            nullable=True,
            checks=[
                # Implementing our own range check due to varying types
                pa.Check(
                    checks.check_year_range_is_valid,
                    element_wise=True,
                    name=checks.check_year_range_is_valid.__doc__,
                    description=inspect.getsource(checks.check_year_range_is_valid),
                ),
                # Implementing our own data type check, because it will get
                # read as an object by default
                pa.Check(
                    checks.check_year_is_numeric,
                    element_wise=True,
                    name=checks.check_year_is_numeric.__doc__,
                    description=inspect.getsource(checks.check_year_is_numeric),
                ),
            ],
        ),
        # "tracks": pa.Column(
        #     object,
        #     nullable=False,
        #     checks=[
        #         pa.Check(
        #             checks.check_track_has_numeric_prefix,
        #             element_wise=True,
        #             name=checks.check_year_is_numeric.__doc__,
        #             description=inspect.getsource(
        #                 checks.check_track_has_numeric_prefix
        #             ),
        #         )
        #     ],
        # ),
        "id": pa.Column(
            object,
            nullable=False,
            checks=[
                pa.Check(
                    lambda x: len(x) == 6,
                    element_wise=True,
                    name="Check that the length of 'id' is 6 characters.",
                    description=inspect.getsource(
                         checks.check_id_six_digit_starting_one),
                )
            ],
        ),
    }
)
