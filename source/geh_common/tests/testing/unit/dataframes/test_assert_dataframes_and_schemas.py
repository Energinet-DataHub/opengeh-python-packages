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


def test__when_column_order_is_different__then_raises_exception(spark):
    # Arrange
    actual_schema = T.StructType(
        [
            T.StructField("aaa", T.StringType(), False),
            T.StructField("bbb", T.StringType(), True),
        ]
    )
    expected_schema = T.StructType(
        [
            T.StructField("bbb", T.StringType(), True),
            T.StructField("aaa", T.StringType(), False),
        ]
    )

    actual_data = [("a", "b"), ("c", "d"), ("e", "f")]
    expected_data = [("a", "b"), ("c", "d"), ("e", "f")]
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    actual = spark.createDataFrame(actual_data, schema=actual_schema)

    # Act & Assert
    with pytest.raises(AssertionError) as excinfo:
        assert_dataframes_and_schemas(actual, expected)

    assert "Schema mismatch. Expected column name 'bbb', but found 'aaa'" in str(excinfo.value)


def test__when_dataframe_row_order_mismatch__then_raises_exception(spark):
    # Arrange
    actual_schema = T.StructType(
        [
            T.StructField("aaa", T.StringType(), False),
            T.StructField("bbb", T.StringType(), True),
        ]
    )
    expected_schema = T.StructType(
        [
            T.StructField("aaa", T.StringType(), False),
            T.StructField("bbb", T.StringType(), True),
        ]
    )

    actual_data = [("a", "b"), ("c", "d"), ("e", "f")]
    expected_data = [("c", "d"), ("a", "b"), ("e", "f")]
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    actual = spark.createDataFrame(actual_data, schema=actual_schema)

    configuration = AssertDataframesConfiguration()
    configuration.enforce_row_order = True

    # Act & Assert
    with pytest.raises(AssertionError) as excinfo:
        assert_dataframes_and_schemas(actual, expected, configuration)

    assert "Row mismatch at" in str(excinfo.value)
