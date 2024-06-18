from pyspark.sql import SparkSession


def is_unity_catalog(
        spark: SparkSession,
        catalog_name: str,
) -> bool:
    if not spark.catalog.databaseExists("system.information_schema"):
        # If the system catalog does not exist, then the workspace is not a Unity enabled workspace
        return False

    result = spark.sql(f'SELECT count(*) AS count FROM system.information_schema.catalogs'
                       f' WHERE catalog_name = "{catalog_name}"').collect()[0][0]
    return result > 0
