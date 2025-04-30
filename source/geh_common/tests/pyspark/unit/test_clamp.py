from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, TimestampType

from geh_common.pyspark.clamp import clamp_period_end, clamp_period_start
from geh_common.testing.dataframes.assert_dataframes import assert_dataframes_equal


def test_clamp_period_start(spark: SparkSession):
    # Arrange
    data = [
        (None,),
        (datetime(2024, 1, 5, tzinfo=UTC),),
        (datetime(2025, 1, 1, tzinfo=UTC),),
        (datetime(2025, 5, 1, tzinfo=UTC),),
    ]
    schema = StructType([StructField("period_start", TimestampType(), True)])
    df = spark.createDataFrame(data, schema)
    clamp_start_datetime = datetime(2025, 1, 1)

    expected_data = [
        (clamp_start_datetime,),
        (clamp_start_datetime,),
        (clamp_start_datetime,),
        (datetime(2025, 5, 1, tzinfo=UTC),),
    ]
    expected = spark.createDataFrame(expected_data, schema)

    # Act
    actual = df.withColumn("period_start", clamp_period_start(col("period_start"), clamp_start_datetime))

    # Assert
    assert_dataframes_equal(actual, expected)


def test_clamp_period_end(spark):
    # Arrange
    data = [
        (None,),
        (datetime(2024, 1, 5, tzinfo=UTC),),
        (datetime(2025, 1, 1, tzinfo=UTC),),
        (datetime(2025, 5, 1, tzinfo=UTC),),
    ]
    schema = StructType([StructField("period_end", TimestampType(), True)])
    df = spark.createDataFrame(data, schema)
    clamp_end_datetime = datetime(2025, 1, 1)

    expected_data = [
        (clamp_end_datetime,),
        (datetime(2024, 1, 5, tzinfo=UTC),),
        (clamp_end_datetime,),
        (clamp_end_datetime,),
    ]
    expected = spark.createDataFrame(expected_data, schema)

    # Act
    actual = df.withColumn("period_end", clamp_period_end(col("period_end"), clamp_end_datetime))

    # Assert
    assert_dataframes_equal(actual, expected)
