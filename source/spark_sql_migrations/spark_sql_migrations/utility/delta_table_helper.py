from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def delta_table_exists(spark: SparkSession, schema_name: str, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name, schema_name)


def create_schema(
    spark: SparkSession, schema_name: str, comment: str = "", location: str = ""
) -> None:
    sql_command = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

    if comment:
        sql_command += f" COMMENT '{comment}'"

    if location:
        sql_command += f" LOCATION '{location}'"
    spark.sql(sql_command)


def create_table_from_schema(
    spark: SparkSession,
    database: str,
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

    sql_command = f"CREATE TABLE IF NOT EXISTS {database}.{table_name} ({ddl}) USING DELTA"

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
