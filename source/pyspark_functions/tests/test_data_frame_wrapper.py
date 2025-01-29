from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark_functions.data_frame_wrapper import DataFrameWrapper
import pytest


def test__data_frame_wrapper_initialization(spark):
    # Arrange
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, schema)

    # Act
    wrapper = DataFrameWrapper(df, schema)

    # Assert
    assert wrapper.df.schema == schema
    assert wrapper.df.collect() == df.collect()


def test__data_frame_wrapper_keeps_columns_from_expected_schema(spark):
    # Arrange
    initial_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True),
    ])
    data = [("Alice", 30, "Roskilde"), ("Bob", 25, "Taastrup")]
    df = spark.createDataFrame(data, initial_schema)
    expected_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    expected_data = [("Alice", 30), ("Bob", 25)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Act
    wrapper = DataFrameWrapper(df, expected_schema)

    # Assert
    assert wrapper.df.schema == expected_schema
    assert wrapper.df.collect() == expected_df.collect()


def test__data_frame_wrapper_missing_nullable_columns(spark):
    # Arrange
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    data = [("Alice", 30), ("Bob", None)]
    df = spark.createDataFrame(data, schema=schema)

    expected_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True),
    ])
    expected_data = [("Alice", 30, None), ("Bob", None, None)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Act
    wrapper = DataFrameWrapper(df, expected_schema)

    # Assert
    assert wrapper.df.collect() == expected_df.collect()


def test__data_frame_wrapper_unexpected_nullability(spark):
    # Arrange
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    data = [("Alice", 30), ("Bob", None)]
    df = spark.createDataFrame(data, schema=schema)
    used_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), False),
    ])

    # Act & Assert
    with pytest.raises(AssertionError, match="Expected column name 'age' to have nullable"):
        DataFrameWrapper(df, used_schema)


def test__data_frame_wrapper_cache_internal(spark):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, schema)

    wrapper = DataFrameWrapper(df, schema)
    wrapper.cache_internal()

    assert wrapper.df.is_cached
