import os
import tempfile
from enum import Enum
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkLogLevel(str, Enum):
    ALL = "ALL"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
    INFO = "INFO"
    OFF = "OFF"
    TRACE = "TRACE"
    WARN = "WARN"


def get_spark_test_session(
    config_overrides: dict = {},
    static_data_dir: Path | str | None = None,
    extra_packages: list[str] | None = None,
    spark_log_level: SparkLogLevel = SparkLogLevel.ERROR,
    use_hive: bool = False,
) -> tuple[SparkSession, str]:
    """Get a Spark session for testing.

    This function creates a Spark session with a temporary data directory and
    a set of default configuration values. The configuration values can be
    overridden by passing a dictionary to the `config_overrides` parameter.

    The default configuration values are optimized for small tests and disable
    the Spark UI. The data directory is used to store temporary files and
    tables created during the test.

    The Spark session is created with Hive support and Delta Lake enabled.

    The configuration used in largely inspired by the one used in this article:
    https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6

    Args:
        config_overrides: A dictionary of configuration overrides.
        static_data_dir: A static data directory to use instead of a temporary one.
        extra_packages: A list of extra packages to include in the Spark session.
        spark_log_level: The log level for the Spark session. Default is ERROR.
        use_hive: Flag determining whether hive persistance is used or not. Defaults to False.

    Returns:
        A tuple of the Spark session and the data directory.
    """
    if static_data_dir:
        data_dir = Path(static_data_dir)
    else:
        data_dir = Path(tempfile.mkdtemp())

    # Create default configuration and apply overrides
    config = _make_default_config(data_dir, use_hive)
    config.update(config_overrides)

    # Convert to SparkConf
    conf = SparkConf().setAll(pairs=[(k, v) for k, v in config.items()])

    # Create the Spark session
    builder = configure_spark_with_delta_pip(SparkSession.Builder().config(conf=conf), extra_packages=extra_packages)

    # Use a single core when running under xdist
    if os.environ.get("PYTEST_XDIST_WORKER") is not None:
        master = "local[1]"
    else:
        master = "local[*]"

    # Use hive
    if use_hive:
        builder = builder.enableHiveSupport()

    spark = builder.master(master).getOrCreate()
    spark.sparkContext.setLogLevel(spark_log_level)
    return spark, data_dir


def _make_default_config(data_dir: Path, use_hive: bool) -> dict:
    """Create a default configuration for the Spark session.

    Most of the configuration values are set to optimize for small tests
    and disable the Spark UI. The data directory is used to store temporary
    files and tables created during the test.

    Args:
        data_dir: The data directory to use for temporary files and tables.

    Returns:
        A dictionary of default configuration values.
    """
    warehouse_path = f"{data_dir.resolve()}/spark-warehouse"
    temp_path = f"{data_dir.resolve()}/spark_tmp"
    metastore_path = f"{data_dir.resolve()}/metastore_db"

    extra_java_options = [
        # reduces the memory footprint by limiting the Delta log cache
        "-Ddelta.log.cacheSize=3",
        # allows the JVM garbage collector to remove unused classes that Spark generates a lot of dynamically
        "-XX:+CMSClassUnloadingEnabled",
        # tells JVM to use 32-bit addresses instead of 64 (If you’re not planning to use more than 32G of RAM)
        "-XX:+UseCompressedOops",
        # When running locally, Spark uses the Apache Derby database. This database uses RAM and the local disc to store files.
        # Using the same metastore in all processes can lead to concurrent modifications of the same table metadata.
        # This can lead to errors (e.g. unknown partitions) and undesired interference between tests.
        # To avoid it, use a separate metastore in each process.
        f"-Dderby.system.home={str(data_dir.resolve())}",
    ]
    configuration = {
        "spark.driver.extraJavaOptions": " ".join(extra_java_options),
        # Delta Lake configuration
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": warehouse_path,
        "spark.local.dir": temp_path,
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
    }
    if use_hive:
        configuration.update(
            {
                "spark.sql.catalogImplementation": "hive",
                "javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={metastore_path};create=true",
                "javax.jdo.option.ConnectionDriverName": "org.apache.derby.jdbc.EmbeddedDriver",
                "javax.jdo.option.ConnectionUserName": "APP",
                "javax.jdo.option.ConnectionPassword": "mine",
                "datanucleus.autoCreateSchema": "true",
                "hive.metastore.schema.verification": "false",
                "hive.metastore.schema.verification.record.version": "false",
            }
        )
    return configuration
