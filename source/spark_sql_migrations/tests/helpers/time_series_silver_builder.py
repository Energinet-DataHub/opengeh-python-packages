from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode

from package.schemas.silver.time_series_silver_schema import time_series_silver_schema
from tests.helpers.datetime_formatter import format_datetime
from tests.helpers.time_series_silver_defaults import TimeSeriesSilverDefaults

default_date = format_datetime("01-01-2018T23:00:00+0000")
default_startdate = format_datetime("01-02-2020T23:00:00+0000")
default_enddate = format_datetime("01-10-2020T23:00:00+0000")


def build(
    spark: SparkSession,
    valid_from_date: datetime = default_startdate,
    valid_to_date: datetime = default_enddate,
    resolution: str = TimeSeriesSilverDefaults.resolution,
    amount_of_time_series: int = 1,
    quality: str = "E01",
    quantity: float = 0.3,
    metering_point_id: str = TimeSeriesSilverDefaults.metering_point_id,
    type_of_mp: str = "E17",
    transaction_id: str = "TRE2313e067d00c1dbe5b7793a26456ef69",
    historical_flag: str = "N",
    created: datetime = datetime.now(),
    status: int = 2,
    read_reason: str = "",
) -> DataFrame:
    time_series = []

    for i in range(amount_of_time_series):
        value = TimeSeriesValue(position=i + 1, quantity=quantity, quality=quality)
        time_series.append(
            TimeSeriesSilver(
                metering_point_id=metering_point_id,
                type_of_mp=type_of_mp,
                transaction_id=transaction_id,
                valid_from_date=valid_from_date,
                valid_to_date=valid_to_date,
                transaction_insert_date=default_date,
                historical_flag=historical_flag,
                resolution=resolution,
                unit="KWH",
                status=status,
                read_reason=read_reason,
                values=[value],
                created=created,
            )
        )

    return spark.createDataFrame(time_series, schema=time_series_silver_schema)


class TimeSeriesValue:
    def __init__(self, position: int, quantity: float, quality: str) -> None:
        self.position = position
        self.quantity = Decimal(quantity)
        self.quality = quality


class TimeSeriesSilver:
    def __init__(
        self,
        metering_point_id: str,
        transaction_id: str,
        valid_from_date: datetime,
        valid_to_date: datetime,
        transaction_insert_date: datetime,
        historical_flag: str,
        type_of_mp: str,
        resolution: str,
        status: int,
        read_reason: str,
        unit: str,
        values: list[TimeSeriesValue],
        created: datetime,
    ) -> None:
        self.metering_point_id = metering_point_id
        self.type_of_mp = type_of_mp
        self.transaction_id = transaction_id
        self.valid_from_date = valid_from_date
        self.valid_to_date = valid_to_date
        self.transaction_insert_date = transaction_insert_date
        self.historical_flag = historical_flag
        self.resolution = resolution
        self.unit = unit
        self.status = status
        self.read_reason = read_reason
        self.values = values
        self.created = created


def build_with_exploded_values(
    spark: SparkSession,
    valid_from_date: datetime = default_startdate,
    resolution: str = "60",
    amount_of_time_series: int = 1,
    quality: str = "E01",
) -> DataFrame:
    time_series = build(
        spark=spark,
        valid_from_date=valid_from_date,
        resolution=resolution,
        amount_of_time_series=amount_of_time_series,
        quality=quality,
    )
    exploded_ts = time_series.select("*", explode("values")).drop("values")
    return exploded_ts
