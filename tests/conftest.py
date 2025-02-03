from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession):
    yield
    # Clear the cache after each test module to avoid memory issues
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    session = (
        SparkSession.builder.master("local[2]")  # type: ignore
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(session).getOrCreate()
    yield session
    session.stop()
