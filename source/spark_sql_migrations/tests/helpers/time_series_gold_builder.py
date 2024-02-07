import uuid
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from datetime import datetime
from decimal import Decimal
from package.schemas.gold.time_series_gold_schema import time_series_gold_schema
from tests.helpers.datetime_formatter import format_date
from tests.helpers.time_series_bronze_defaults import TimeSeriesBronzeDefaults
import package.enums.time_series.resolution_enum as Resolution

default_date = format_date("01-01-2018")
default_to_date = format_date("01-02-2018")


def build(
    spark: SparkSession,
    metering_point_id: str = TimeSeriesBronzeDefaults.metering_point_id,
    quantity: Decimal = Decimal(1),
    quality: str = TimeSeriesBronzeDefaults.quality,
    observation_time: datetime = default_date,
    transaction_id: str = str(uuid.uuid4()),
    transaction_insert_date: datetime = default_date,
    unit: str = "KWH",
    valid_from_date: datetime = default_date,
    valid_to_date: datetime = default_to_date,
    position: int = 1,
    created: datetime = datetime.now(),
    modified: datetime = datetime.now(),
    schema: StructType = time_series_gold_schema,
    resolution: str = Resolution.Dh3ResolutionEnum.PT1H.value,
    historical_flag: str = "N",
    status: int = 2,
    read_reason: str = "",
    partition_metering_point_id: int = 0,
) -> DataFrame:
    time_series = [
        TimeSeriesGold(
            metering_point_id,
            quantity,
            quality,
            observation_time,
            transaction_id,
            transaction_insert_date,
            unit,
            valid_from_date,
            valid_to_date,
            position,
            created,
            modified,
            resolution,
            historical_flag,
            status,
            read_reason,
            partition_metering_point_id,
        )
    ]
    return spark.createDataFrame(time_series, schema=schema)


class TimeSeriesGold:
    def __init__(
        self,
        metering_point_id: str,
        quantity: Decimal,
        quality: str,
        observation_time: datetime,
        transaction_id: str,
        transaction_insert_date: datetime,
        unit: str,
        valid_from_date: datetime,
        valid_to_date: datetime,
        position: int,
        created: datetime,
        modified: datetime,
        resolution: str,
        historical_flag: str,
        status: int,
        read_reason: str,
        partition_metering_point_id: int,
    ) -> None:
        self.metering_point_id = metering_point_id
        self.quantity = quantity
        self.quality = quality
        self.observation_time = observation_time
        self.transaction_id = transaction_id
        self.transaction_insert_date = transaction_insert_date
        self.unit = unit
        self.valid_from_date = valid_from_date
        self.valid_to_date = valid_to_date
        self.position = position
        self.created = created
        self.modified = modified
        self.resolution = resolution
        self.historical_flag = historical_flag
        self.status = status
        self.read_reason = read_reason
        self.partition_metering_point_id = partition_metering_point_id
