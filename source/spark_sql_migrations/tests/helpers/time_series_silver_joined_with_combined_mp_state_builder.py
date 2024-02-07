from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal
from tests.helpers.schemas.time_series_silver_joined_with_combined_mp_state_schema import (
    time_series_with_combined_mp_data_schema,
)
from pyspark.sql.dataframe import DataFrame
from tests.helpers.time_series_silver_defaults import TimeSeriesSilverDefaults
from tests.helpers.metering_point_defaults import MeteringPointDefaults
from tests.helpers.datetime_formatter import format_datetime

default_start_date = format_datetime("01-02-2023T23:00:00+0000")
default_end_date = format_datetime("01-04-2023T22:00:00+0000")


def build(
    spark: SparkSession,
    time_series_metering_point_id: str = TimeSeriesSilverDefaults.metering_point_id,
    type_of_mp: str = TimeSeriesSilverDefaults.default_type_of_mp,
    resolution: str = TimeSeriesSilverDefaults.resolution,
    transaction_id: str = "trans_id_business_validation_rules_test",
    valid_from_date: datetime = default_start_date,
    valid_to_date: datetime = default_end_date,
    use_time_series_mp_id_as_mp_id: bool = True,
    metering_point_min_valid_from_date: datetime = default_start_date,
    metering_point_max_valid_to_date: datetime = default_end_date,
    metering_point_meter_reading_occurrences: list = [
        TimeSeriesSilverDefaults.resolution
    ],
    metering_point_physical_statuses: list = [
        MeteringPointDefaults.physical_status_of_mp
    ],
) -> DataFrame:
    # The dataframe returns only the columns relevant for testing the business validation rules
    value = TimeSeriesValue(position=1, quantity=1, quality="A04")
    data = [
        (
            time_series_metering_point_id,
            type_of_mp,
            resolution,
            transaction_id,
            valid_from_date,
            valid_to_date,
            _use_same_metering_point_id_or_none(
                use_time_series_mp_id_as_mp_id, time_series_metering_point_id
            ),
            metering_point_min_valid_from_date,
            metering_point_max_valid_to_date,
            metering_point_meter_reading_occurrences,
            metering_point_physical_statuses,
            [value],
        )
    ]

    return spark.createDataFrame(data, schema=time_series_with_combined_mp_data_schema)


def _use_same_metering_point_id_or_none(
    use_same: bool, time_series_mp_id: str
) -> str | None:
    if use_same:
        return time_series_mp_id
    else:
        return None


class TimeSeriesValue:
    def __init__(self, position: int, quantity: float, quality: str) -> None:
        self.position = position
        self.quantity = Decimal(quantity)
        self.quality = quality
