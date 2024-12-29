from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from pyspark.sql import SparkSession


schema = StructType(
    [
        StructField("column1", StringType(), True),
        StructField("column2", StringType(), True),
    ]
)


def create_test_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.test_schema")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table (column1 STRING, column2 STRING) USING DELTA"
    )
    spark.sql(
        "CREATE TABLE IF NOT EXISTS spark_catalog.test_schema.test_table_2 (column1 STRING, column2 STRING) USING DELTA"
    )
