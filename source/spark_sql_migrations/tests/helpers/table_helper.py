import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def append_to_table(dataframe: DataFrame, full_table: str) -> None:
    dataframe.write.format("delta").mode("append").saveAsTable(full_table)


def get_table_version(spark: SparkSession, schema: str, table: str) -> int:
    if not spark.catalog.tableExists(table, schema):
        return 0

    history = spark.sql(f"DESCRIBE HISTORY {schema}.{table}")
    current_version = history.orderBy(F.desc("version")).limit(1)
    return current_version.select("version").first()[0]


def create_schema_and_table(spark: SparkSession, schema_name: str, table_name: str, schema: StructType) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.catalog.createTable(
        f"{schema_name}.{table_name}",
        schema=schema,
    )
