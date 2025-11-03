from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_common.data_products.measurements_core.measurements_gold import (
    measurements_zorder as current_measurements_data_product,
)
from geh_common.infrastructure.model.current_measurements import CurrentMeasurements
from geh_common.infrastructure.model.current_measurements_column_names import CurrentMeasurementsColumnNames


class CurrentMeasurementsRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    table_name = current_measurements_data_product.view_name
    database_name = current_measurements_data_product.database_name

    def read_current_measurements(
        self,
        period_start_utc: datetime,
        period_end_utc: datetime,
        metering_point_ids: list[str] | None = None,
    ) -> CurrentMeasurements:
        current_measurements = self._read()
        period_start_local_time = period_start_utc.astimezone(ZoneInfo("Europe/Copenhagen"))
        period_end_local_time = period_end_utc.astimezone(ZoneInfo("Europe/Copenhagen"))

        # Filtering by z-order's partition_year and partition_month columns
        current_measurements_filtered = current_measurements.filter(
            # period_start
            (F.col(CurrentMeasurementsColumnNames.partition_year) > period_start_local_time.year)
            | (
                (F.col(CurrentMeasurementsColumnNames.partition_year) == period_start_local_time.year)
                & (F.col(CurrentMeasurementsColumnNames.partition_month) >= period_start_local_time.month)
            )
            &
            # period_end
            (F.col(CurrentMeasurementsColumnNames.partition_year) < period_end_local_time.year)
            | (
                (F.col(CurrentMeasurementsColumnNames.partition_year) == period_end_local_time.year)
                & (F.col(CurrentMeasurementsColumnNames.partition_month) <= period_end_local_time.month)
            )
        )

        # If metering_point_ids is provided, filter by metering_point_ids
        if metering_point_ids is not None:
            # Construct a set containing the last 3 digits of each metering_point_id
            metering_point_ids_last_3_digits = set(int(mp_id[-3:]) for mp_id in metering_point_ids)
            # Filter by z-order's partition_metering_point_id using the constructed set
            current_measurements_filtered = current_measurements_filtered.where(
                F.col(CurrentMeasurementsColumnNames.partition_metering_point_id).isin(metering_point_ids_last_3_digits)
            )
            current_measurements_filtered = current_measurements_filtered.where(
                F.col(CurrentMeasurementsColumnNames.metering_point_id).isin(metering_point_ids)
            )

        # Filter observation_time by period start and end
        current_measurements_filtered = current_measurements_filtered.filter(
            (F.col(CurrentMeasurementsColumnNames.observation_time) >= period_start_utc)
            & (F.col(CurrentMeasurementsColumnNames.observation_time) <= period_end_utc)
        )

        current_measurements_filtered = current_measurements_filtered.select(
            CurrentMeasurementsColumnNames.metering_point_id,
            CurrentMeasurementsColumnNames.observation_time,
            CurrentMeasurementsColumnNames.quantity,
            CurrentMeasurementsColumnNames.quality,
            CurrentMeasurementsColumnNames.metering_point_type,
            CurrentMeasurementsColumnNames.partition_metering_point_id,
            CurrentMeasurementsColumnNames.partition_year,
            CurrentMeasurementsColumnNames.partition_month,
        )

        # assert_contract(df.schema, current_measurements_data_product.schema) TODO HENRIK: find ud af om vi skal lave asserts.
        return CurrentMeasurements(current_measurements_filtered)

    def _read(self) -> DataFrame:
        """Read table or view. The function is introduced to allow mocking in tests."""
        return self._spark.read.table(f"{self._catalog_name}.{self.database_name}.{self.table_name}")
