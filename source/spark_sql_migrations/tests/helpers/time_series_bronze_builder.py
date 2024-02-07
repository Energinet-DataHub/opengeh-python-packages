from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal
from pyspark.sql.dataframe import DataFrame
from tests.helpers.time_series_bronze_defaults import TimeSeriesBronzeDefaults
from tests.helpers.datetime_formatter import convert_to_datetime
from package.schemas.bronze.time_series_bronze_schema import time_series_bronze_schema
from typing import Any


default_masterdata_start_date = convert_to_datetime("2018-01-01T00:00:00.000+0000")
default_masterdata_end_date = convert_to_datetime("2018-02-01T00:00:00.000+0000")
default_transaction_insert_date = convert_to_datetime("2018-01-01T00:00:00.000+0000")
default_startdate = convert_to_datetime("2020-01-01T23:00:00.000+0000")
default_enddate = convert_to_datetime("2021-01-01T23:00:00.000+0000")


def get_masterdata(
    type_of_mp: str = TimeSeriesBronzeDefaults.default_type_of_mp,
    grid_area_id: str | None = TimeSeriesBronzeDefaults.grid_area_id,
    masterdata_start_date: datetime = default_masterdata_start_date,
    masterdata_end_date: datetime = default_masterdata_end_date
) -> list[Any]:
    return [
        {
            "grid_area": grid_area_id,
            "type_of_mp": type_of_mp,
            "masterdata_start_date": masterdata_start_date,
            "masterdata_end_date": masterdata_end_date,
        }
    ]


def get_masterdata_with_same_type_of_mp() -> list[Any]:
    return [
        {
            "grid_area": TimeSeriesBronzeDefaults.grid_area_id,
            "type_of_mp": "E17",
            "masterdata_start_date": default_masterdata_start_date,
            "masterdata_end_date": None,
        },
        {
            "grid_area": TimeSeriesBronzeDefaults.grid_area_id,
            "type_of_mp": "E17",
            "masterdata_start_date": default_masterdata_start_date,
            "masterdata_end_date": None,
        },
    ]


def get_masterdata_with_different_type_of_mp() -> list[Any]:
    return [
        {
            "grid_area": TimeSeriesBronzeDefaults.grid_area_id,
            "type_of_mp": "E17",
            "masterdata_start_date": default_masterdata_start_date,
            "masterdata_end_date": None,
        },
        {
            "grid_area": TimeSeriesBronzeDefaults.grid_area_id,
            "type_of_mp": "E18",
            "masterdata_start_date": default_masterdata_start_date,
            "masterdata_end_date": None,
        },
    ]


def get_masterdata_with_two_dicts_one_as_none() -> list[Any]:
    return [
        {
            "grid_area": TimeSeriesBronzeDefaults.grid_area_id,
            "type_of_mp": "E17",
            "masterdata_start_date": default_masterdata_start_date,
            "masterdata_end_date": None,
        },
        None,
    ]


def get_points(
    number_of_values: int = TimeSeriesBronzeDefaults.number_of_values,
    quantity_start: Decimal = TimeSeriesBronzeDefaults.quantity_start,
    quality: str = TimeSeriesBronzeDefaults.quality,
) -> list[Any]:
    values = []
    for x in range(number_of_values):
        values.append(
            {
                "position": x + 1,
                "quantity": quantity_start / 100,
                "quality": quality,
            }
        )
    return values


def get_points_where_position_is_none() -> list[Any]:
    values = []
    for x in range(10):
        values.append(
            {
                "position": None,
                "quantity": TimeSeriesBronzeDefaults.quantity_start / 100,
                "quality": TimeSeriesBronzeDefaults.quality,
            }
        )
    return values


def get_points_where_quantity_is_none() -> list[Any]:
    values = []
    for x in range(10):
        values.append(
            {
                "position": x + 1,
                "quantity": None,
                "quality": TimeSeriesBronzeDefaults.quality,
            }
        )
    return values


def get_points_where_quality_is_none() -> list[Any]:
    values = []
    for x in range(10):
        values.append(
            {
                "position": x + 1,
                "quantity": TimeSeriesBronzeDefaults.quantity_start / 100,
                "quality": None,
            }
        )
    return values


def build(
    spark: SparkSession,
    masterdata: Any = get_masterdata(),
    points: Any = get_points(),
    metering_point_id: str = TimeSeriesBronzeDefaults.metering_point_id,
    historical_flag: str = TimeSeriesBronzeDefaults.historical_flag,
    resolution: str | None = TimeSeriesBronzeDefaults.resolution,
    unit: str = TimeSeriesBronzeDefaults.unit,
    status: int = TimeSeriesBronzeDefaults.status,
    read_reason: str = TimeSeriesBronzeDefaults.read_reason,
    transaction_insert_date: datetime | None = default_transaction_insert_date,
    valid_from_date: datetime = default_startdate,
    valid_to_date: datetime = default_enddate,
    rescued_data: str | None = TimeSeriesBronzeDefaults.rescued_data,
    created: datetime = datetime.now(),
    file_path: str = "",
) -> DataFrame:
    time_series_data = [
        {
            "metering_point": {
                "metering_point_id": metering_point_id,
                "masterdata": masterdata,
            },
            "time_series": [
                {
                    "transaction_id": "TRE2313e067d00c1dbe5b7793a26456ef69",
                    "valid_from_date": valid_from_date,
                    "valid_to_date": valid_to_date,
                    "transaction_insert_date": transaction_insert_date,
                    "historical_flag": historical_flag,
                    "resolution": resolution,
                    "unit": unit,
                    "status": status,
                    "read_reason": read_reason,
                    "values": points,
                },
            ],
            "_rescued_data": rescued_data,
            "created": created,
            "file_path": file_path,
        }
    ]

    return spark.createDataFrame(time_series_data, schema=time_series_bronze_schema)
