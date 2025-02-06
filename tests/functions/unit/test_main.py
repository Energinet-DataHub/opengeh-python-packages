from datetime import datetime

from pyspark.sql import functions as F

from geh_common.functions.transformations import (
    begining_of_year,
    convert_localtime_to_utc,
    convert_utc_to_localtime,
    days_in_year,
)


def test_convert_utc_to_localtime(spark):
    # Arrange
    data = [(datetime(2023, 1, 1, 11, 0),)]
    df = spark.createDataFrame(data, ["timestamp"])

    # Act
    result = convert_utc_to_localtime(df, "timestamp", "Europe/Copenhagen")

    # Assert
    assert result.collect()[0]["timestamp"].hour == 12  # UTC+1


def test_convert_localtime_to_utc(spark):
    # Arrange
    data = [(datetime(2023, 1, 1, 13, 0),)]
    df = spark.createDataFrame(data, ["timestamp"])

    # Act
    result = convert_localtime_to_utc(df, "timestamp", "Europe/Copenhagen")

    # Assert
    assert result.collect()[0]["timestamp"].hour == 12  # UTC


def test_begining_of_year(spark):
    # Arrange
    data = [(datetime(2023, 3, 15),)]
    df = spark.createDataFrame(data, ["date"])

    # Act
    result = df.select(begining_of_year(F.col("date")).alias("year_start"))

    # Assert
    assert result.collect()[0]["year_start"] == datetime(2023, 1, 1, 0, 0)


def test_beginning_of_year_with_offset(spark):
    # Arrange
    data = [(datetime(2023, 3, 15),)]
    df = spark.createDataFrame(data, ["date"])

    # Act
    result = df.select(begining_of_year(F.col("date"), 1).alias("year_start"))

    # Assert
    assert result.collect()[0]["year_start"] == datetime(2024, 1, 1, 0, 0)


def test_days_in_year(spark):
    # Arrange
    data = [
        (datetime(2023, 1, 1),),  # Non-leap year
        (datetime(2024, 1, 1),),  # Leap year
    ]
    df = spark.createDataFrame(data, ["date"])

    # Act
    result = df.select(days_in_year(F.col("date")).alias("days"))
    rows = result.collect()

    # Assert
    assert rows[0]["days"] == 365  # 2023
    assert rows[1]["days"] == 366  # 2024
