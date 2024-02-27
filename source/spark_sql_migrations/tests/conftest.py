"""
By having a conftest.py in this directory, we can define fixtures to be
shared among all tests in the test suite.
"""

import os
import pytest
from pyspark.sql import SparkSession
from shutil import rmtree
from typing import Generator
from spark_sql_migrations.container import create_and_configure_container
from tests.builders.spark_sql_migrations_configuration_builder import build as build_configuration


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """

    create_and_configure_container(build_configuration())


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    warehouse_location = os.path.abspath("spark-warehouse")
    if os.path.exists(warehouse_location):
        rmtree(warehouse_location)

    session = (
        SparkSession.builder.config("spark.sql.streaming.schemaInference", True)
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
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    ).getOrCreate()

    yield session
    session.stop()
