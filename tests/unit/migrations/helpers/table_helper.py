import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


def append_to_table(dataframe: DataFrame, full_table: str) -> None:
    dataframe.write.format("delta").mode("append").saveAsTable(full_table)


def get_table_version(
    spark: SparkSession, catalog_name: str, schema_name: str, table_name: str
) -> int:
    if not spark.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}"):
        return 0

    history = spark.sql(f"DESCRIBE HISTORY {catalog_name}.{schema_name}.{table_name}")
    current_version = history.orderBy(F.desc("version")).limit(1)
    versions = current_version.select("version").first()
    if versions is None:
        return 0
    return versions[0]


def create_schema_and_table(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    schema: T.StructType,
) -> None:
    create_schema(spark, catalog_name, schema_name)
    create_table(spark, catalog_name, schema_name, table_name, schema)


def create_schema(spark: SparkSession, catalog_name: str, schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")


def create_table(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    schema: T.StructType,
) -> None:
    table_exists = spark.catalog.tableExists(
        f"{catalog_name}.{schema_name}.{table_name}"
    )

    if not table_exists:
        spark.catalog.createTable(
            f"{catalog_name}.{schema_name}.{table_name}",
            schema=schema,
        )


def get_current_table_version(
    spark: SparkSession, schema_name: str, table_name: str
) -> int:
    history = spark.sql(f"DESCRIBE HISTORY spark_catalog.{schema_name}.{table_name}")

    expected_version = (
        history.orderBy(F.desc("version")).limit(1).select("version").first()
    )
    if expected_version is None:
        return 0

    return expected_version[0]
