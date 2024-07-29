from pyspark.sql import SparkSession


def reset_spark_catalog(spark: SparkSession) -> None:
    schemas = spark.sql("SHOW SCHEMAS IN spark_catalog").collect()
    for schema in schemas:
        schema_name = schema["namespace"]
        if schema_name != "default":
            spark.sql(f"DROP SCHEMA IF EXISTS spark_catalog.{schema_name} CASCADE")


def drop_schema(spark: SparkSession, catalog_name: str, schema_name: str) -> None:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")
