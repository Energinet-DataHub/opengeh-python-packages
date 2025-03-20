import shutil
from typing import Generator

import pytest
from pyspark.sql import SparkSession

from geh_common.testing.spark.spark_test_session import get_spark_test_session


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession):
    yield
    # Clear the cache after each test module to avoid memory issues
    spark.catalog.clearCache()


_spark, data_dir = get_spark_test_session()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    yield _spark
    _spark.stop()
    shutil.rmtree(data_dir, ignore_errors=True)
