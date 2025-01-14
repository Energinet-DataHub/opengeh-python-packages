import pytest
from pyspark.sql import types as T

from testcommon.dataframes import assert_dataframes_and_schemas, AssertDataframesConfiguration

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


def test_with_ignored(spark):
    # Arrange
    expected_data = [(1, True), (2, False), (3, False)]
    expected = spark.createDataFrame(expected_data, schema=expected_schema)

    actual_data = [(1, "x", True), (2, "y", False), (3, "z", False)]
    actual = spark.createDataFrame(actual_data, schema=actual_schema)

    # Act & Assert
    assert_dataframes_and_schemas(actual, expected)


def test_with_strict_actual_assertion_with_ignored_columns(spark):
    # Arrange
    expected_data = [(1, True), (2, False), (3, False)]
    expected = spark.createDataFrame(expected_data, schema=expected_schema)

    actual_data = [(1, "x", True), (2, "y", False), (3, "z", False)]
    actual = spark.createDataFrame(actual_data, schema=actual_schema)

    configuration = AssertDataframesConfiguration()
    configuration.strict_actual_assertion = True

    # Act & Assert
    with pytest.raises(Exception):
        assert_dataframes_and_schemas(actual, expected, configuration)
