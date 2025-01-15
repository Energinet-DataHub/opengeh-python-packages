from typing import Generator
import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession):
    yield
    # Clear the cache after each test module to avoid memory issues
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = SparkSession.builder.appName("testcommon").getOrCreate()
    yield session
    session.stop()
