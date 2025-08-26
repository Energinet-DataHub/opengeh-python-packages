from pyspark.sql.session import SparkSession


def optimize_table(spark: SparkSession, database: str, table: str) -> None:
    """Optimize a table in the database.

    Args:
        spark (SparkSession): The Spark session.
        database (str): The name of the database.
        table (str): The name of the table to optimize.
    """
    print(f"Optimizing table {database}.{table}...")  # noqa: T201
    result = spark.sql(f"OPTIMIZE {database}.{table}")

    print(result.select("path").collect())  # noqa: T201
    print(result.select("metrics.numFilesAdded").collect())  # noqa: T201
    print(result.select("metrics.numFilesRemoved").collect())  # noqa: T201
