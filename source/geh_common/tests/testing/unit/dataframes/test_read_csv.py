from pyspark.sql import types as T
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from geh_common.testing.dataframes import (
    AssertDataframesConfiguration,
    assert_dataframes_and_schemas,
    read_csv,
)
from tests.testing.unit.scenario_testing.constants import SCENARIO_TESTING_DATA

schema = T.StructType(
    [
        T.StructField("a", T.IntegerType(), False),
        T.StructField("b", T.StringType(), True),
        T.StructField("c", T.BooleanType(), True),
    ]
)

schema_without_ignored = T.StructType(
    [
        T.StructField("a", T.IntegerType(), False),
        T.StructField("c", T.BooleanType(), True),
    ]
)

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
    path = SCENARIO_TESTING_DATA / "then" / "with_ignored.csv"

    # Act
    actual = read_csv(spark, str(path), schema_without_ignored, ignored_value=IGNORED_VALUE).collect()

    # Assert
    assert actual == expected, f"Expected {expected}, got {actual}."


def test_no_array(spark):
    path = SCENARIO_TESTING_DATA / "then" / "no_array.csv"
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
            T.StructField("e", T.ArrayType(T.StringType(), containsNull=False), True),
        ]
    )

    path = SCENARIO_TESTING_DATA / "then" / "with_array_string.csv"
    df = read_csv(spark, str(path), schema, sep=";")
    assert df.schema == schema, "Schema does not match"

    test_df = spark.createDataFrame([(1, "a", True, ["a", "b", None], ["a", "b", "c"])], schema=schema)

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
    path = SCENARIO_TESTING_DATA / "then" / "with_nullability.csv"
    configuration = AssertDataframesConfiguration()
    configuration.ignore_nullability = False

    # Act
    actual = read_csv(spark, str(path), nullability_schema)

    # Assert
    expected = spark.createDataFrame(data=actual.rdd, schema=nullability_schema, verifySchema=True)
    assert_dataframes_and_schemas(actual, expected, configuration)
