from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from dependency_injector.wiring import Provide, inject
from spark_sql_migrations.models.table_version import TableVersion
from spark_sql_migrations.container import SparkSqlMigrationsContainer
from spark_sql_migrations.utility.catalog_helper import is_unity_catalog


def delta_table_exists(spark: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> bool:
    return spark.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}")


def create_schema(
    spark: SparkSession, catalog_name: str, schema_name: str, comment: str = "", location: str = ""
) -> None:

    if is_unity_catalog(spark, catalog_name):
        # Schema creation for unity catalogs is created in infrastructure
        return

    sql_command = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"

    if comment:
        sql_command += f" COMMENT '{comment}'"

    if location:
        sql_command += f" LOCATION '{location}'"
    spark.sql(sql_command)


def create_table_from_schema(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    schema: StructType,
    optimize: bool = False,
    location: str = "",
    target_file_size_in_mb: int = 0,
    enable_change_data_feed: bool = False,
    partition_columns: List[str] = [],
) -> None:
    schema_df = spark.createDataFrame([], schema=schema)
    ddl = schema_df._jdf.schema().toDDL()

    sql_command = f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} ({ddl}) USING DELTA"

    if partition_columns:
        partition_columns_str = ", ".join(partition_columns)
        sql_command += f" PARTITIONED BY ({partition_columns_str})"

    tbl_properties = []
    if optimize:
        tbl_properties.append("delta.autoOptimize.optimizeWrite = true")
        tbl_properties.append("delta.autoOptimize.autoCompact = true")
    else:
        if target_file_size_in_mb > 0:
            tbl_properties.append(f"delta.targetFileSize = '{target_file_size_in_mb}mb'")

    if enable_change_data_feed:
        tbl_properties.append("delta.enableChangeDataFeed = true")

    if len(tbl_properties) > 0:
        tbl_properties_string = ", ".join(tbl_properties)
        sql_command += f" TBLPROPERTIES ({tbl_properties_string})"

    if location:
        sql_command += f" LOCATION '{location}'"
    spark.sql(sql_command)


def restore_table(table_version: TableVersion) -> None:
    _restore_table(table_version)


@inject
def _restore_table(
    table_version: TableVersion, spark: SparkSession = Provide[SparkSqlMigrationsContainer.spark]
) -> None:
    spark.sql(f"RESTORE TABLE {table_version.table_name} TO VERSION AS OF {table_version.version}")
