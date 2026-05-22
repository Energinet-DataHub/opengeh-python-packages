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


def optimize_table_zorder(
    spark: SparkSession,
    database: str,
    table: str,
    zorder_columns: list[str],
    where: str | None = None,
) -> None:
    """Optimize a table with Z-Order on specified columns.

    Args:
        spark (SparkSession): The Spark session.
        database (str): The name of the database.
        table (str): The name of the table to optimize.
        zorder_columns (list[str]): The columns to Z-Order by.
        where (str | None): Optional WHERE clause to limit optimization scope.
    """
    zorder_cols = ", ".join(zorder_columns)
    where_clause = f" WHERE {where}" if where else ""

    sql = f"OPTIMIZE {database}.{table}{where_clause} ZORDER BY ({zorder_cols})"

    print(f"Optimizing table {database}.{table} with Z-Order by ({zorder_cols})...")  # noqa: T201
    result = spark.sql(sql)

    print(result.select("path").collect())  # noqa: T201
    print(result.select("metrics.numFilesAdded").collect())  # noqa: T201
    print(result.select("metrics.numFilesRemoved").collect())  # noqa: T201
