from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from .metering_point_defaults import MeteringPointDefaults
from datetime import datetime
from typing import Any
import pyspark.sql.functions as F
import tests.helpers.metering_points_bronze_builder as metering_point_bronze_builder


class MeteringPointsBronzeQuarantineBuilder(
    metering_point_bronze_builder.MeteringPointsBronzeBuilder
):
    def add_validation_columns(self, df: DataFrame) -> DataFrame:
        return df.withColumn("validate_closed_metering_points", F.lit(False))

    def build(
        self,
        spark: SparkSession,
        physical_status_of_mp: str = MeteringPointDefaults.physical_status_of_mp,
        balance_supplier_id: str = MeteringPointDefaults.balance_supplier_id,
        balance_supplier_start_date: datetime = MeteringPointDefaults.balance_supplier_start_date,
        balance_responsible_party_id: str = MeteringPointDefaults.balance_responsible_party_id,
        balance_resp_party_start_date: datetime = MeteringPointDefaults.balance_resp_party_start_date,
    ) -> DataFrame:
        df = super().build(
            spark,
            connection_register=[
                self.create_connection_register_row(
                    physical_status_of_mp=physical_status_of_mp,
                    balance_supplier_id=balance_supplier_id,
                    balance_supplier_start_date=balance_supplier_start_date,
                    balance_responsible_party_id=balance_responsible_party_id,
                    balance_resp_party_start_date=balance_resp_party_start_date,
                ),
            ],
        )

        return self.add_validation_columns(df)

    def build_multiple_connection_registers_rows(
        self, spark: SparkSession, connection_register: list[Any]
    ) -> DataFrame:
        df = super().build(spark, connection_register=connection_register)
        return self.add_validation_columns(df)

    def build_multiple_metering_points(
        self,
        spark: SparkSession,
        metering_points: list[Any],
    ) -> DataFrame:
        df = super().build(spark, metering_points=metering_points)
        return df
