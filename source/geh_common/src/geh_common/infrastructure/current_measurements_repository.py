from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_common.data_products.measurements_core.measurements_gold import (
    measurements_zorder as current_measurements_data_product,
)
from geh_common.infrastructure.model.current_measurements import CURRENT_MEASUREMENTS_SCHEMA, CurrentMeasurements
from geh_common.infrastructure.model.current_measurements_column_names import CurrentMeasurementsColumnNames
from geh_common.testing.dataframes import assert_contract


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

    def read(
        self,
    ) -> CurrentMeasurements:
        current_measurements = self._read()
        assert_contract(current_measurements.schema, CURRENT_MEASUREMENTS_SCHEMA)
        return CurrentMeasurements(current_measurements)

    def read_and_filter(
        self,
        period_start_utc: datetime,
        period_end_utc: datetime,
        metering_point_ids: list[str] | None = None,
    ) -> CurrentMeasurements:
        current_measurements = self._read()

        # If metering_point_ids is provided, filter by metering_point_ids
        if metering_point_ids is not None:
            current_measurements = current_measurements.where(
                F.col(CurrentMeasurementsColumnNames.metering_point_id).isin(metering_point_ids)
            )

        # Filter observation_time by period start and end
        current_measurements = current_measurements.filter(
            (F.col(CurrentMeasurementsColumnNames.observation_time) >= period_start_utc)
            & (F.col(CurrentMeasurementsColumnNames.observation_time) < period_end_utc)
        )

        return CurrentMeasurements(current_measurements)

    def _read(self) -> DataFrame:
        """Read table or view. The function is introduced to allow mocking in tests."""
        return self._spark.read.table(f"{self._catalog_name}.{self.database_name}.{self.table_name}")
