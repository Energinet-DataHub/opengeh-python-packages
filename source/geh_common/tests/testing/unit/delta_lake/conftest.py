import pytest
from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def clean_catalog(spark: SparkSession) -> None:
    # Drop all databases except the default one
    for db in spark.catalog.listDatabases():
        if db.name != "default":
            spark.sql(f"DROP DATABASE {db.name} CASCADE")
