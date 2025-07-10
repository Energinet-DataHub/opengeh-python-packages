from dataclasses import dataclass
from typing import Tuple

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from geh_common.testing.dataframes.assert_schemas import assert_schema


@dataclass
class AssertDataframesConfiguration:
    show_actual_and_expected_count: bool = False
    show_actual_and_expected: bool = False
    show_columns_when_actual_and_expected_are_equal: bool = False
    ignore_extra_columns_in_actual: bool = True
    ignore_row_order: bool = True

    ignore_nullability: bool = True
    """Default true because Spark doesn't handle nullability well."""
    ignore_column_order: bool = False
    ignore_decimal_scale: bool = False
    ignore_decimal_precision: bool = False
    ignore_duplicated_rows: bool = False
    columns_to_skip: list[str] | None = None


def assert_dataframes_and_schemas(
    actual: DataFrame,
    expected: DataFrame,
    configuration: AssertDataframesConfiguration | None = None,
) -> None:
    assert actual is not None, "Actual data frame is None"
    assert expected is not None, "Expected data frame is None"
    actual_rows = actual.count()
    expected_rows = expected.count()

    if configuration is None:
        configuration = AssertDataframesConfiguration()

    if configuration.show_actual_and_expected_count:
        print("\n")  # noqa
        print(f"Number of rows in actual: {actual_rows}")  # noqa
        print(f"Number of rows in expected: {expected_rows}")  # noqa

    if configuration.columns_to_skip is not None and len(configuration.columns_to_skip) > 0:
        actual = actual.drop(*configuration.columns_to_skip)
        expected = expected.drop(*configuration.columns_to_skip)

    if configuration.ignore_extra_columns_in_actual:
        # When there are ignored columns, the actual dataframe will have
        # more columns than the expected dataframe. Therefore, in order to
        # compare the extra columns are removed from the actual dataframe.
        actual_columns = set(actual.columns)
        expected_columns = set(expected.columns)
        columns_to_drop = actual_columns - expected_columns
        actual = actual.drop(*columns_to_drop)

    try:
        assert_schema(
            actual=actual.schema,
            expected=expected.schema,
            ignore_nullability=configuration.ignore_nullability,
            ignore_column_order=configuration.ignore_column_order,
            ignore_decimal_scale=configuration.ignore_decimal_scale,
            ignore_decimal_precision=configuration.ignore_decimal_precision,
        )
    except AssertionError:
        print("SCHEMA MISMATCH:")  # noqa
        print("ACTUAL SCHEMA:")  # noqa
        actual.printSchema()
        print("EXPECTED SCHEMA:")  # noqa
        expected.printSchema()
        raise

    if configuration.show_actual_and_expected:
        print("ACTUAL:")  # noqa
        actual.show(3000, False)
        print("EXPECTED:")  # noqa
        expected.show(3000, False)

    if not configuration.ignore_duplicated_rows:
        try:
            _assert_no_duplicates(actual, actual_rows)
        except AssertionError:
            if not configuration.show_columns_when_actual_and_expected_are_equal:
                actual, expected = _drop_columns_if_the_same(actual, expected)

            print("DUPLICATED ROWS IN ACTUAL:")  # noqa
            _show_duplicates(actual).show(3000, False)
            raise

        try:
            _assert_no_duplicates(expected, expected_rows)
        except AssertionError:
            if not configuration.show_columns_when_actual_and_expected_are_equal:
                actual, expected = _drop_columns_if_the_same(actual, expected)

            print("DUPLICATED ROWS IN EXPECTED:")  # noqa
            _show_duplicates(expected).show(3000, False)
            raise

    try:
        assert_dataframes_equal(actual, expected)
    except AssertionError:
        if not configuration.show_columns_when_actual_and_expected_are_equal:
            actual, expected = _drop_columns_if_the_same(actual, expected)

        print("DATA MISMATCH:")  # noqa
        print("IN ACTUAL BUT NOT IN EXPECTED:")  # noqa
        actual.subtract(expected).show(3000, False)
        print("IN EXPECTED BUT NOT IN ACTUAL:")  # noqa
        expected.subtract(actual).show(3000, False)
        raise

    try:
        assert actual_rows == expected_rows
    except AssertionError:
        if not configuration.show_columns_when_actual_and_expected_are_equal:
            actual, expected = _drop_columns_if_the_same(actual, expected)

        print(  # noqa
            f"NUMBER OF ROWS MISMATCH: Actual: {actual_rows}, Expected: {expected_rows}"
        )
        raise

    # Check row order if configured
    if configuration.ignore_row_order is False:
        try:
            assert_row_order(actual, expected)
        except AssertionError:
            if not configuration.show_columns_when_actual_and_expected_are_equal:
                actual, expected = _drop_columns_if_the_same(actual, expected)

            print("ROW ORDER MISMATCH:")  # noqa
            print("First 10 rows of ACTUAL:")  # noqa
            actual.limit(10).show(truncate=False)
            print("First 10 rows of EXPECTED:")  # noqa
            expected.limit(10).show(truncate=False)
            raise


def assert_row_order(actual: DataFrame, expected: DataFrame) -> None:
    """Assert that rows in actual dataframe match the exact order of rows in expected dataframe.

    Args:
        actual: The actual dataframe
        expected: The expected dataframe

    Raises:
        AssertionError: If rows do not appear in the same order
    """
    # Convert both dataframes to lists of rows for direct comparison
    actual_rows = actual.collect()
    expected_rows = expected.collect()

    # Check if number of rows match
    if len(actual_rows) != len(expected_rows):
        assert False, f"Row count mismatch: actual ({len(actual_rows)}) vs expected ({len(expected_rows)})"

    # Compare row by row
    for i, (a_row, e_row) in enumerate(zip(actual_rows, expected_rows)):
        if a_row != e_row:
            assert False, f"Row mismatch at position {i}: \nActual: {a_row}\nExpected: {e_row}"


def assert_dataframes_equal(actual: DataFrame, expected: DataFrame) -> None:
    actual_excess = actual.subtract(expected)
    expected_excess = expected.subtract(actual)

    actual_excess_count = actual_excess.count()
    expected_excess_count = expected_excess.count()

    if actual_excess_count > 0:
        print("Actual excess:")  # noqa
        actual_excess.show(3000, False)

    if expected_excess_count > 0:
        print("Expected excess:")  # noqa
        expected_excess.show(3000, False)

    assert actual.count() == expected.count() and actual_excess_count == 0 and expected_excess_count == 0, (
        "Dataframes data are not equal"
    )


def _assert_no_duplicates(df: DataFrame, original_count: int) -> None:
    distinct_count = df.dropDuplicates().count()
    assert original_count == distinct_count, "The DataFrame contains duplicate rows"


def _show_duplicates(df: DataFrame) -> DataFrame:
    duplicates = df.groupby(df.columns).count().where(f.col("count") > 1).withColumnRenamed("count", "duplicate_count")
    return duplicates


def _drop_columns_if_the_same(df1: DataFrame, df2: DataFrame) -> Tuple[DataFrame, DataFrame]:
    column_names = df1.columns
    for column_name in column_names:
        df1_column = df1.select(column_name).collect()
        df2_column = df2.select(column_name).collect()

        if df1_column == df2_column:
            df1 = df1.drop(column_name)
            df2 = df2.drop(column_name)

    return df1, df2
