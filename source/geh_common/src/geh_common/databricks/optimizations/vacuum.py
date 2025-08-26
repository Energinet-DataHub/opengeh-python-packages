from pyspark.sql import SparkSession


def vacuum_table(spark: SparkSession, database: str, table: str) -> None:
    """Vacuum the specified table in the given database.

    Vacuum remove all files from the table directory that are not managed by Delta.

    Args:
      spark: The SparkSession to use for the vacuuming.
      database: The name of the database containing the table.
      table: The name of the table to vacuum.
    """
    print(f"Vacuuming table '{table}' in database '{database}'")  # noqa: T201
    result = spark.sql(f"VACUUM {database}.{table}")
    print(f"Vacuum result: {result}")  # noqa: T201
