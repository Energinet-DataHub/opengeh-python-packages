"""
By having a conftest.py in this directory, we can define fixtures to be
shared among all tests in the test suite.
"""

import os
from shutil import rmtree
from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    warehouse_location = os.path.abspath("spark-warehouse")
    if os.path.exists(warehouse_location):
        rmtree(warehouse_location)

    session = configure_spark_with_delta_pip(
        SparkSession.builder.config("spark.sql.streaming.schemaInference", True)  # type: ignore
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .config("spark.default.parallelism", 1)
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        .config("spark.shuffle.spill.compress", False)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", True)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    ).getOrCreate()

    yield session
    session.stop()
