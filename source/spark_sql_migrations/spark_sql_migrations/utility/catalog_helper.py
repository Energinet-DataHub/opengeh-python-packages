from pyspark.sql import SparkSession


def is_unity_catalog(
        spark: SparkSession,
        catalog_name: str,
) -> bool:
    if not spark.catalog.databaseExists("system"):
        # If the system catalog does not exist, then the workspace is not a Unity enabled workspace
        return False

    sql_command = (f'SELECT count(*) AS count FROM system.information_schema.catalogs'
                   f' WHERE catalog_name = {catalog_name}')
    result = spark.sql(sql_command)
    count = result.collect()[0][0]
    return count > 0
