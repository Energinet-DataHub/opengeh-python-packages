import pytest
from pyspark.sql import types as T

from opengeh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas

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
