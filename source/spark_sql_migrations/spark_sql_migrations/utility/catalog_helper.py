from pyspark.sql import SparkSession


def is_unity_catalog(spark: SparkSession, catalog_name: str) -> bool:
    sql_command = (f'SELECT count(*) AS count FROM system.information_schema.catalogs '
                   f'WHERE catalog_name = {catalog_name}')
    result = spark.sql(sql_command)
    count = result.collect()[0][0]
    return count > 0
