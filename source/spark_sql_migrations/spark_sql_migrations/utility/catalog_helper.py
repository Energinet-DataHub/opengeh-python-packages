from pyspark.sql import SparkSession

from spark_sql_migrations.constants.catalog_constants import Catalog


def is_unity_catalog(
        spark: SparkSession,
        catalog_name: str,
) -> bool:
    return catalog_name.lower() not in Catalog.hive_metastores
