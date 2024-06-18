from pyspark.sql import SparkSession


def is_unity_catalog(
        spark: SparkSession,
        catalog_name: str,
) -> bool:
    if not spark.catalog.databaseExists("system.information_schema"):
        # If the system.information schema does not exist, then the workspace is not a Unity enabled workspace
        return False
    result = spark.sql(f'SELECT count(*) AS count FROM system.information_schema.catalogs'
                       f' WHERE catalog_name = "{catalog_name}"')
    first_row = 0
    count_col = 0
    count = result.collect()[first_row][count_col]
    return count > 0
