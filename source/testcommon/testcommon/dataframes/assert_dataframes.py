from dataclasses import dataclass
from typing import Any, Tuple

from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from testcommon.dataframes.assert_schemas import assert_schema


@dataclass
class AssertDataframesConfiguration:
    show_actual_and_expected_count: bool = False
    show_actual_and_expected: bool = False

    ignore_nullability: bool = True,
    """Default true because Spark doesn't handle nullability well."""
    ignore_column_order: bool = False,
    ignore_decimal_scale: bool = False,
    ignore_decimal_precision: bool = False,

    columns_to_skip: list[str] | None = None,


def assert_dataframes_and_schemas(
    actual: DataFrame,
    expected: DataFrame,
    configuration: AssertDataframesConfiguration | None = None,
) -> None:
    assert actual is not None, "Actual data frame is None"
    assert expected is not None, "Expected data frame is None"

    if configuration is None:
        configuration = AssertDataframesConfiguration()

    if configuration.show_actual_and_expected_count:
        print("\n")
        print(f"Number of rows in actual: {actual.count()}")
        print(f"Number of rows in expected: {expected.count()}")

    if configuration.columns_to_skip is not None and len(configuration.columns_to_skip) > 0:
        actual = actual.drop(*configuration.columns_to_skip)
        expected = expected.drop(*configuration.columns_to_skip)

    try:
        assert_schema(
            actual: actual.schema,
            expected: expected.schema,
            ignore_nullability: configuration.ignore_nullability,
            ignore_column_order: configuration.ignore_column_order,
            ignore_decimal_scale: configuration.ignore_decimal_scale,
            ignore_decimal_precision: configuration.ignore_decimal_precision
        )
    except AssertionError:
        print("SCHEMA MISMATCH:")
        print("ACTUAL SCHEMA:")
        actual.printSchema()
        print("EXPECTED SCHEMA:")
        expected.printSchema()
        raise

    if configuration.show_actual_and_expected:
        print("ACTUAL:")
        actual.show(3000, False)
        print("EXPECTED:")
        expected.show(3000, False)

    try:
        _assert_no_duplicates(actual)
    except AssertionError:

        if (
            not configuration.show_columns_when_actual_and_expected_are_equal
        ):
            actual, expected = _drop_columns_if_the_same(actual, expected)

        print("DUPLICATED ROWS IN ACTUAL:")
        _show_duplicates(actual).show(3000, False)
        raise

    try:
        _assert_no_duplicates(expected)
    except AssertionError:

        if (
            not configuration.show_columns_when_actual_and_expected_are_equal
        ):
            actual, expected = _drop_columns_if_the_same(actual, expected)

        print("DUPLICATED ROWS IN EXPECTED:")
        _show_duplicates(expected).show(3000, False)
        raise

    try:
        _assert_dataframes(actual, expected)
    except AssertionError:

        if (
            not configuration.show_columns_when_actual_and_expected_are_equal
        ):
            actual, expected = _drop_columns_if_the_same(actual, expected)

        print("DATA MISMATCH:")
        print("IN ACTUAL BUT NOT IN EXPECTED:")
        actual.subtract(expected).show(3000, False)
        print("IN EXPECTED BUT NOT IN ACTUAL:")
        expected.subtract(actual).show(3000, False)
        raise

    try:
        assert actual.count() == expected.count()
    except AssertionError:

        if (
            not configuration.show_columns_when_actual_and_expected_are_equal
        ):
            actual, expected = _drop_columns_if_the_same(actual, expected)

        print(
            f"NUMBER OF ROWS MISMATCH: Actual: {actual.count()}, Expected: {expected.count()}"
        )
        raise


def _assert_dataframes(actual: DataFrame, expected: DataFrame) -> None:
    actual_excess = actual.subtract(expected)
    expected_excess = expected.subtract(actual)

    if actual_excess.count() > 0:
        print("Actual excess:")
        actual_excess.show(3000, False)

    if expected_excess.count() > 0:
        print("Expected excess:")
        expected_excess.show(3000, False)

    assert (
        actual_excess.count() == 0 and expected_excess.count() == 0
    ), "Dataframes data are not equal"


def _assert_no_duplicates(df: DataFrame) -> None:
    original_count = df.count()
    distinct_count = df.dropDuplicates().count()
    assert original_count == distinct_count, "The DataFrame contains duplicate rows"


def _show_duplicates(df: DataFrame) -> DataFrame:
    duplicates = (
        df.groupby(df.columns)
        .count()
        .where(f.col("count") > 1)
        .withColumnRenamed("count", "duplicate_count")
    )
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
