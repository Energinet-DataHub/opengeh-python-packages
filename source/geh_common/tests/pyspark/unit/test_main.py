from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import functions as F

from geh_common.pyspark.transformations import (
    begining_of_year,
    convert_localtime_to_utc,
    convert_utc_to_localtime,
    days_in_year,
)


@pytest.mark.parametrize(
    "date, expected",
    [
        (datetime(2023, 1, 1, tzinfo=timezone.utc), datetime(2023, 1, 1, 1, tzinfo=ZoneInfo("Europe/Copenhagen"))),
        (datetime(2023, 7, 1, tzinfo=timezone.utc), datetime(2023, 7, 1, 2, tzinfo=ZoneInfo("Europe/Copenhagen"))),
    ],
)
def test_convert_utc_to_localtime(spark, date, expected):
    # Arrange
    data = [(date,)]
    df = spark.createDataFrame(data, ["timestamp"])

    # Act
    result = convert_utc_to_localtime(df, "timestamp", "Europe/Copenhagen")

    # Assert
    assert result.collect()[0]["timestamp"].astimezone(timezone.utc).replace(tzinfo=None) == expected.replace(
        tzinfo=None
    )


@pytest.mark.parametrize(
    "date, expected",
    [
        (datetime(2023, 1, 1, 1, tzinfo=ZoneInfo("Europe/Copenhagen")), datetime(2023, 1, 1, tzinfo=timezone.utc)),
        (datetime(2023, 7, 1, 2, tzinfo=ZoneInfo("Europe/Copenhagen")), datetime(2023, 7, 1, tzinfo=timezone.utc)),
    ],
)
def test_convert_localtime_to_utc(spark, date, expected):
    # Arrange
    df = spark.createDataFrame([(date,)], ["timestamp"])

    # Act
    result = convert_localtime_to_utc(df, "timestamp", "Europe/Copenhagen")

    # Assert
    assert result.collect()[0]["timestamp"].astimezone(ZoneInfo("Europe/Copenhagen")).replace(
        tzinfo=None
    ) == expected.replace(tzinfo=None)  # UTC


def test_begining_of_year(spark):
    # Arrange
    data = [(datetime(2023, 3, 15),)]
    df = spark.createDataFrame(data, ["date"])

    # Act
    result = df.select(begining_of_year(F.col("date")).alias("year_start"))

    # Assert
    assert result.collect()[0]["year_start"].astimezone(timezone.utc) == datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_beginning_of_year_with_offset(spark):
    # Arrange
    data = [(datetime(2023, 3, 15),)]
    df = spark.createDataFrame(data, ["date"])

    # Act
    result = df.select(begining_of_year(F.col("date"), 1).alias("year_start"))

    # Assert
    assert result.collect()[0]["year_start"].astimezone(timezone.utc) == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "year,date, expected",
    [
        (2023, datetime(2023, 1, 1, tzinfo=timezone.utc), 365),
        (2024, datetime(2024, 1, 1, tzinfo=timezone.utc), 366),
        (2020, datetime(2020, 2, 29, tzinfo=timezone.utc), 366),  # Leap year
        (2021, datetime(2021, 2, 28, tzinfo=timezone.utc), 365),
    ],
)
def test_days_in_year_normal_year(spark, year, date, expected):
    # Arrange
    data = [
        (date,),
    ]
    df = spark.createDataFrame(data, ["date"])

    # Act
    result = df.select(days_in_year(F.col("date")).alias("days"))
    rows = result.collect()

    # Assert
    assert rows[0]["days"] == expected
