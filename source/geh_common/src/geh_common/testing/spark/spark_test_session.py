import os
import tempfile
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_test_session(
    config_overrides: dict = {}, static_data_dir: Path | str | None = None, extra_packages: list[str] | None = None
) -> tuple[SparkSession, str]:
    """Get a Spark session for testing.

    This function creates a Spark session with a temporary data directory and
    a set of default configuration values. The configuration values can be
    overridden by passing a dictionary to the `config_overrides` parameter.

    The default configuration values are optimized for small tests and disable
    the Spark UI. The data directory is used to store temporary files and
    tables created during the test.

    The Spark session is created with Hive support and Delta Lake enabled.

    Args:
        config_overrides: A dictionary of configuration overrides.
        static_data_dir: A static data directory to use instead of a temporary one.

    Returns:
        A tuple of the Spark session and the data directory.
    """
    if static_data_dir:
        data_dir = str(static_data_dir)
    else:
        data_dir = tempfile.mkdtemp()

    # Create default configuration and apply overrides
    config = _make_default_config(data_dir)
    config.update(config_overrides)

    # Convert to SparkConf
    conf = SparkConf().setAll(pairs=[(k, v) for k, v in config.items()])

    # Create the Spark session
    builder = configure_spark_with_delta_pip(
        SparkSession.Builder().config(conf=conf).enableHiveSupport(), extra_packages=extra_packages
    )

    # Use a single core when running under xdist
    if os.environ.get("PYTEST_XDIST_WORKER") is not None:
        master = "local[1]"
    else:
        master = "local[*]"

    return builder.master(master).getOrCreate(), data_dir


def _make_default_config(data_dir: str) -> dict:
    return {
        # Delta Lake configuration
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.catalogImplementation": "in-memory",
        # "spark.sql.catalogImplementation": "hive",
        "spark.sql.warehouse.dir": f"{data_dir}/spark-warehouse",
        "spark.local.dir": f"{data_dir}/spark-tmp",
        #"javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={data_dir}/metastore;create=true",
        #"javax.jdo.option.ConnectionUserName": "APP",
        #"javax.jdo.option.ConnectionPassword": "mine",
        # Disable schema verification
        #"hive.metastore.schema.verification": "false",
        #"hive.metastore.schema.verification.record.version": "false",
        #"datanucleus.autoCreateSchema": "true",
        # Disable the UI
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.enabled": "false",
        # Optimize for small tests
        "spark.ui.dagGraph.retainedRootRDDs": "1",
        "spark.ui.retainedJobs": "1",
        "spark.ui.retainedStages": "1",
        "spark.ui.retainedTasks": "1",
        "spark.sql.ui.retainedExecutions": "1",
        "spark.worker.ui.retainedExecutors": "1",
        "spark.worker.ui.retainedDrivers": "1",
        "spark.databricks.delta.snapshotPartitions": "2",
        "spark.sql.shuffle.partitions": "1",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.streaming.schemaInference": "true",
        "spark.rdd.compress": "false",
        "spark.shuffle.compress": "false",
        "spark.shuffle.spill.compress": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.extraJavaOptions": f"-Ddelta.log.cacheSize=3 -Dderby.system.home={data_dir} -XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops",
    }
