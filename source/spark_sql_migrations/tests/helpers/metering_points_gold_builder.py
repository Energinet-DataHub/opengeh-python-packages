from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.dataframe import DataFrame
from package.schemas.gold.metering_points_gold_schema import metering_points_gold_schema
from tests.helpers.metering_point_defaults import MeteringPointDefaults
from tests.helpers.datetime_formatter import format_date

default_startdate = format_date("01-01-2020")
default_enddate = format_date("01-01-2021")


def build(
    spark: SparkSession,
    balance_supplier_id: str | None = MeteringPointDefaults.balance_supplier_id,
    metering_point_id: str = MeteringPointDefaults.metering_point_id,
    type_of_mp: str = MeteringPointDefaults.type_of_mp,
    settlement_method: str | None = MeteringPointDefaults.settlement_method,
    metering_grid_area_id: str = MeteringPointDefaults.metering_grid_area_id,
    meter_reading_occurrence: str | None = MeteringPointDefaults.meter_reading_occurrence,
    valid_from_date: datetime = default_startdate,
    valid_to_date: datetime | None = default_enddate,
    created: datetime = datetime.now(),
    modified: datetime = datetime.now(),
    metering_point_state_id: int = MeteringPointDefaults.metering_point_state_id,
    from_grid_area: str | None = MeteringPointDefaults.from_grid_area,
    to_grid_area: str | None = MeteringPointDefaults.to_grid_area,
    physical_status_of_mp: str | None = MeteringPointDefaults.physical_status_of_mp
) -> DataFrame:
    df = [
        {
            "metering_point_id": metering_point_id,
            "type_of_mp": type_of_mp,
            "settlement_method": settlement_method,
            "metering_grid_area_id": metering_grid_area_id,
            "meter_reading_occurrence": meter_reading_occurrence,
            "from_grid_area": from_grid_area,
            "to_grid_area": to_grid_area,
            "parent_metering_point_id": None,
            "physical_status_of_mp": physical_status_of_mp,
            "balance_supplier_id": balance_supplier_id,
            "balance_responsible_party_id": "5799994000343",
            "valid_from_date": valid_from_date,
            "valid_to_date": valid_to_date,
            "created": created,
            "modified": modified,
            "metering_point_state_id": metering_point_state_id,
            "partition_column": int(metering_point_id) % 1000
        }
    ]

    return spark.createDataFrame(df, schema=metering_points_gold_schema)
