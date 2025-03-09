from dataclasses import dataclass
from typing import Tuple

import pandas as pd
from pyspark.sql import DataFrame

from geh_common.testing.dataframes.assert_schemas import assert_schema


@dataclass
class AssertDataframesConfiguration:
    show_actual_and_expected_count: bool = False
    show_actual_and_expected: bool = False
    show_columns_when_actual_and_expected_are_equal: bool = False
    ignore_extra_columns_in_actual: bool = True

    ignore_nullability: bool = True
    """Default true because Spark doesn't handle nullability well."""
    ignore_column_order: bool = False
    ignore_decimal_scale: bool = False
    ignore_decimal_precision: bool = False
    columns_to_skip: list[str] | None = None


def assert_dataframes_and_schemas(
    actual: DataFrame,
    expected: DataFrame,
    configuration: AssertDataframesConfiguration | None = None,
) -> None:
    assert actual is not None, "Actual data frame is None"
    assert expected is not None, "Expected data frame is None"

    # Convert Spark DataFrames to Pandas DataFrames
    actual_pd = actual.toPandas()
    expected_pd = expected.toPandas()

    actual_rows = len(actual_pd)
    expected_rows = len(expected_pd)

    if configuration is None:
        configuration = AssertDataframesConfiguration()

    if configuration.show_actual_and_expected_count:
        print("\n")  # noqa
        print(f"Number of rows in actual: {actual_rows}")  # noqa
        print(f"Number of rows in expected: {expected_rows}")  # noqa

    if configuration.columns_to_skip is not None and len(configuration.columns_to_skip) > 0:
        actual_pd = actual_pd.drop(columns=configuration.columns_to_skip)
        expected_pd = expected_pd.drop(columns=configuration.columns_to_skip)

    if configuration.ignore_extra_columns_in_actual:
        actual_columns = set(actual_pd.columns)
        expected_columns = set(expected_pd.columns)
        columns_to_drop = actual_columns - expected_columns
        actual_pd = actual_pd.drop(columns=list(columns_to_drop))

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
        print(actual_pd)  # noqa
        print("EXPECTED:")  # noqa
        print(expected_pd)  # noqa

    try:
        _assert_no_duplicates(actual_pd, actual_rows)
    except AssertionError:
        if not configuration.show_columns_when_actual_and_expected_are_equal:
            actual_pd, expected_pd = _drop_columns_if_the_same(actual_pd, expected_pd)

        print("DUPLICATED ROWS IN ACTUAL:")  # noqa
        print(_show_duplicates(actual_pd))  # noqa
        raise

    try:
        _assert_no_duplicates(expected_pd, expected_rows)
    except AssertionError:
        if not configuration.show_columns_when_actual_and_expected_are_equal:
            actual_pd, expected_pd = _drop_columns_if_the_same(actual_pd, expected_pd)

        print("DUPLICATED ROWS IN EXPECTED:")  # noqa
        print(_show_duplicates(expected_pd))  # noqa
        raise

    _assert_dataframes(actual_pd, expected_pd)


def _assert_dataframes(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    actual_excess = actual[~actual.isin(expected.to_dict(orient="list")).all(axis=1)]
    expected_excess = expected[~expected.isin(actual.to_dict(orient="list")).all(axis=1)]

    actual_excess_count = len(actual_excess)
    expected_excess_count = len(expected_excess)

    if actual_excess_count > 0:
        print("Actual excess:")  # noqa
        print(actual_excess)

    if expected_excess_count > 0:
        print("Expected excess:")  # noqa
        print(expected_excess)

    assert actual_excess_count == 0 and expected_excess_count == 0, "Dataframes data are not equal"


def _assert_no_duplicates(df: pd.DataFrame, original_count: int) -> None:
    distinct_count = df.drop_duplicates().shape[0]
    assert original_count == distinct_count, "The DataFrame contains duplicate rows"


def _show_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    duplicates = df[df.duplicated(keep=False)]
    duplicates["duplicate_count"] = duplicates.groupby(list(df.columns)).transform("count")
    return duplicates.drop_duplicates()


def _drop_columns_if_the_same(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    for column_name in df1.columns:
        if df1[column_name].equals(df2[column_name]):
            df1 = df1.drop(columns=[column_name])
            df2 = df2.drop(columns=[column_name])

    return df1, df2
