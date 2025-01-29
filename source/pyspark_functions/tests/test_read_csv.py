from pyspark.sql import types as T
from testcommon.dataframes import assert_dataframes_and_schemas, AssertDataframesConfiguration
from tests.constants import ETL_TEST_DATA
from pyspark_functions.read_csv import read_csv
import pytest

from pyspark.sql.types import (
    StructField,
    StringType,
    TimestampType,
    StructType, IntegerType, DecimalType, FloatType, BooleanType,
)

schema = T.StructType([
    T.StructField("a", T.IntegerType(), False),
    T.StructField("b", T.StringType(), True),
    T.StructField("c", T.BooleanType(), True),
])

schema_without_ignored = T.StructType([
    T.StructField("a", T.IntegerType(), False),
    T.StructField("c", T.BooleanType(), True),
])

nullability_schema = StructType(
    [
        StructField("string_type_nullable", StringType(), True),
        StructField("string_type", StringType(), False),
        StructField("integer_type_nullable", IntegerType(), True),
        StructField("integer_type", IntegerType(), False),
        StructField("timestamp_type_nullable", TimestampType(), True),
        StructField("timestamp_type", TimestampType(), False),
        StructField("decimal_type_nullable", DecimalType(), True),
        StructField("decimal_type", DecimalType(), False),
        StructField("float_type_nullable", FloatType(), True),
        StructField("float_type", FloatType(), False),
        StructField("boolean_type_nullable", BooleanType(), True),
        StructField("boolean_type", BooleanType(), False),
    ]
)

IGNORED_VALUE = "[IGNORED]"


def test_read_csv_with_ignored(spark):
    # Arrange
    expected_data = [(1, True), (2, True), (3, False)]
    columns = ["a", "c"]
    expected = spark.createDataFrame(expected_data, columns).collect()
    path = ETL_TEST_DATA / "then" / "with_ignored.csv"

    # Act
    actual = read_csv(spark, str(path), schema_without_ignored, ignored_value=IGNORED_VALUE).collect()

    # Assert
    assert actual == expected, f"Expected {expected}, got {actual}."


def test_no_array(spark):
    path = ETL_TEST_DATA / "then" / "no_array.csv"
    df = read_csv(spark, str(path), schema, sep=";", ignored_value=IGNORED_VALUE)
    assert df.schema == schema, "Schema does not match"

    test_df = spark.createDataFrame([(1, "a", True)], schema=schema)

    assert_dataframes_and_schemas(df, test_df)

    collected = df.collect()
    assert collected[0].a == 1, f"a should be 1, got {collected[0].a}"
    assert collected[0].b == "a", f"b should be a, got {collected[0].b}"
    assert collected[0].c is True, f"c should be True, got {collected[0].c}"


def test_with_array_string(spark):
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.StringType(), True),
            T.StructField("c", T.BooleanType(), True),
            T.StructField("d", T.ArrayType(T.StringType()), True),
            T.StructField(
                "e", T.ArrayType(T.StringType(), containsNull=False), True
            ),
        ]
    )

    path = ETL_TEST_DATA / "then" / "with_array_string.csv"
    df = read_csv(spark, str(path), schema, sep=";")
    assert df.schema == schema, "Schema does not match"

    test_df = spark.createDataFrame(
        [(1, "a", True, ["a", "b", None], ["a", "b", "c"])], schema=schema
    )

    assert_dataframes_and_schemas(df, test_df)

    collected = df.collect()
    assert collected[0].a == 1, f"a should be 1, got {collected[0].a}"
    assert collected[0].b == "a", f"b should be a, got {collected[0].b}"
    assert collected[0].c is True, f"c should be True, got {collected[0].c}"
    assert collected[0].d == [
        "a",
        "b",
        None,
    ], collected
    assert collected[0].e == [
        "a",
        "b",
        "c",
    ], f"e should be ['a', 'b', 'c'], got {collected[0].e}"


def test_read_csv_with_nullabilities(spark):
    # Arrange
    path = ETL_TEST_DATA / "then" / "with_nullability.csv"
    configuration = AssertDataframesConfiguration()
    configuration.ignore_nullability = False

    # Act
    actual = read_csv(spark, str(path), nullability_schema)

    # Assert
    expected = spark.createDataFrame(data=actual.rdd, schema=nullability_schema, verifySchema=True)
    assert_dataframes_and_schemas(actual, expected, configuration)


def test_read_csv_fewer_columns_should_fail(spark):
    # Arrange
    path = ETL_TEST_DATA / "then" / "fewer_columns.csv"
    # Act & Assert
    with pytest.raises(ValueError):
        read_csv(spark, str(path), schema)


def test_read_csv_more_columns(spark):
    # Arrange
    path = ETL_TEST_DATA / "then" / "more_columns.csv"
    expected_data = [(1, "a", True, 1, 1.1, False, "string")]
    # Act
    actual = read_csv(spark, str(path), schema, ignore_additional_columns=False)
    # Assert
    schema_with_extra = (
        schema
        .add(T.StructField("extra_int", T.IntegerType(), True))
        .add(T.StructField("extra_float", T.FloatType(), True))
        .add(T.StructField("extra_bool", T.BooleanType(), True))
        .add(T.StructField("extra_string", T.StringType(), True))
    )
    expected = spark.createDataFrame(expected_data, schema_with_extra)
    order = ("a", "b", "c", "extra_int", "extra_float", "extra_bool", "extra_string")
    actual = actual.select(*order)
    expected = expected.select(*order)

    assert_dataframes_and_schemas(actual, expected)
