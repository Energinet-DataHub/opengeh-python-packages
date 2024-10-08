from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from pyspark.sql import SparkSession
from spark_sql_migrations.models.view import View
from spark_sql_migrations.models.table import Table
from spark_sql_migrations.models.schema import Schema


schema = StructType(
    [
        StructField("column1", StringType(), True),
        StructField("column2", StringType(), True),
    ]
)

schema_config = [
    Schema(
        name="test_schema",
        tables=[
            Table(name="test_table", schema=schema),
            Table(name="test_table_2", schema=schema),
        ],
        views=[View(name="test_view", schema=schema)],
    )
]


def create_test_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.test_schema")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table (column1 STRING, column2 STRING) USING DELTA"
    )
    spark.sql(
        "CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table_2 (column1 STRING, column2 STRING) USING DELTA"
    )
