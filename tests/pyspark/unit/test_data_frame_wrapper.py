import pyspark.sql.types as T
import pytest

from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper


def test__ctor__when_valid_input__returns_expected_schema_and_data(spark):
    # Arrange
    schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, schema)

    # Act
    wrapper = DataFrameWrapper(df, schema)

    # Assert
    assert wrapper.df.schema == schema
    assert wrapper.df.collect() == df.collect()


def test__ctor__when_different_columns__returns_expected_schema(spark):
    # Arrange
    initial_schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("city", T.StringType(), True),
        ]
    )
    data = [("Alice", 30, "Roskilde"), ("Bob", 25, "Taastrup")]
    df = spark.createDataFrame(data, initial_schema)
    expected_schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    expected_data = [("Alice", 30), ("Bob", 25)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Act
    wrapper = DataFrameWrapper(df, expected_schema)

    # Assert
    assert wrapper.df.schema == expected_schema
    assert wrapper.df.collect() == expected_df.collect()


def test__ctor__when_nullable_column__returns_column_filled_with_nulls(spark):
    # Arrange
    schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    data = [("Alice", 30), ("Bob", None)]
    df = spark.createDataFrame(data, schema=schema)

    expected_schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("city", T.StringType(), True),
        ]
    )
    expected_data = [("Alice", 30, None), ("Bob", None, None)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Act
    wrapper = DataFrameWrapper(df, expected_schema)

    # Assert
    assert wrapper.df.collect() == expected_df.collect()


def test__ctor__when_not_nullable_column_and_null_value__throws_exception(
    spark,
):
    # Arrange
    schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    data = [("Alice", 30), ("Bob", None)]
    df = spark.createDataFrame(data, schema=schema)
    used_schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), False),
        ]
    )

    # Act & Assert
    with pytest.raises(AssertionError, match="Expected column name 'age' to have nullable"):
        DataFrameWrapper(df, used_schema)


def test_cache_internal__dataframe_is_cached(spark):
    schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, schema)

    wrapper = DataFrameWrapper(df, schema)
    wrapper.cache_internal()

    assert wrapper.df.is_cached
