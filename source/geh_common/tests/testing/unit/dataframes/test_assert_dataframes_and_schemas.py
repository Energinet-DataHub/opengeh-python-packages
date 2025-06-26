import pytest
from pyspark.sql import types as T

from geh_common.testing.dataframes import (
    AssertDataframesConfiguration,
    assert_dataframes_and_schemas,
)

actual_schema = T.StructType(
    [
        T.StructField("a", T.IntegerType(), False),
        T.StructField("b", T.StringType(), True),
        T.StructField("c", T.BooleanType(), True),
    ]
)

expected_schema = T.StructType(
    [
        T.StructField("a", T.IntegerType(), False),
        T.StructField("c", T.BooleanType(), True),
    ]
)

expected_data = [(1, True), (2, False), (3, False)]
actual_data = [(1, "x", True), (2, "y", False), (3, "z", False)]


def test_with_ignored_ignore_extra_columns_in_actual_true(spark):
    # Arrange
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    actual = spark.createDataFrame(actual_data, schema=actual_schema)

    # Act & Assert
    assert_dataframes_and_schemas(actual, expected)


def test_with_ignore_extra_columns_in_actual_false_throws_exception(spark):
    # Arrange
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    actual = spark.createDataFrame(actual_data, schema=actual_schema)

    configuration = AssertDataframesConfiguration()
    configuration.ignore_extra_columns_in_actual = False

    # Act & Assert
    with pytest.raises(Exception):
        assert_dataframes_and_schemas(actual, expected, configuration)


def test_with_duplicated_rows_in_expected_throws_exception(spark):
    # Arrange
    # Only expected dataframe has duplicates
    normal_actual_data = [(1, "x", True), (2, "y", False), (3, "z", False)]
    duplicated_expected_data = [(1, True), (2, False), (2, False), (3, False)]

    expected = spark.createDataFrame(duplicated_expected_data, schema=expected_schema)
    actual = spark.createDataFrame(normal_actual_data, schema=actual_schema)

    # Act & Assert
    with pytest.raises(AssertionError) as excinfo:
        assert_dataframes_and_schemas(actual, expected)

    assert "The DataFrame contains duplicate rows" in str(excinfo.value)


def test_with_duplicated_rows_in_actual_throws_exception(spark):
    # Arrange
    # Only actual dataframe has duplicates
    duplicated_actual_data = [(1, "x", True), (2, "y", False), (2, "y", False), (3, "z", False)]
    normal_expected_data = [(1, True), (2, False), (3, False)]

    expected = spark.createDataFrame(normal_expected_data, schema=expected_schema)
    actual = spark.createDataFrame(duplicated_actual_data, schema=actual_schema)

    # Act & Assert
    with pytest.raises(AssertionError) as excinfo:
        assert_dataframes_and_schemas(actual, expected)

    assert "The DataFrame contains duplicate rows" in str(excinfo.value)


def test__when_with_duplicated_rows_and_ignore_duplicated_rows_true__then_does_not_raise(spark):
    # Arrange
    # Both dataframes have duplicates
    duplicated_actual_data = [(1, "x", True), (2, "y", False), (2, "y", False), (3, "z", False)]
    duplicated_expected_data = [(1, True), (2, False), (2, False), (3, False)]

    expected = spark.createDataFrame(duplicated_expected_data, schema=expected_schema)
    actual = spark.createDataFrame(duplicated_actual_data, schema=actual_schema)

    configuration = AssertDataframesConfiguration()
    configuration.ignore_duplicated_rows = True

    # Act & Assert
    # Should not raise an exception when ignore_duplicated_rows is True
    assert_dataframes_and_schemas(actual, expected, configuration)
