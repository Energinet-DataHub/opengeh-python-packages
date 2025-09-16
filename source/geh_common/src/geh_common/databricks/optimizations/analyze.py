from pyspark.sql.session import SparkSession


def analyze_table(spark: SparkSession, database: str, table: str, analyze_columns: str | None = None) -> None:
    """Analyze the specified table in the given database.

    Analyze helps query performance by collecting statistics on the table.

    Args:
      spark: The SparkSession to use for the analysis.
      database: The name of the database to analyze.
      table: The name of the table to analyze.
      analyze_columns (optional): The columns to analyze (preferably cluster keys).
    """
    if analyze_columns:
        print(f"Analyzing columns: {analyze_columns}")  # noqa: T201
        sql = f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS FOR COLUMNS {analyze_columns}"
    else:
        print("Analyzing columns all columns")  # noqa: T201
        sql = f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS FOR ALL COLUMNS"

    result = spark.sql(sql)
    print(f"Analysis result: {result}")  # noqa: T201
