from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from geh_common.pyspark.read_csv import read_csv_path


def _remove_ignored_columns(df, schema, ignored_value="[IGNORED]") -> DataFrame:
    # Check each column to see if all values are "[IGNORED]"
    ignore_check = df.agg(
        *[F.every(F.col(c).cast(T.StringType()) == F.lit(ignored_value)).alias(c) for c in df.columns]
    ).collect()

    # Get the columns that should be ignored
    ignored_cols = [c for c, v in ignore_check[0].asDict().items() if v and c in schema.fieldNames()]

    df = df.drop(*ignored_cols)

    return df


def read_csv_path_test(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
    sep: str = ";",
    ignored_value="[IGNORED]",
    ignore_additional_columns: bool = True,
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame. Used for testing purposes! For production use read_csv instead.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the CSV file.
        schema (StructType): The schema of the CSV file.
        sep (str, optional): The separator of the CSV file. Defaults to ";".
        ignored_value (str, optional): Columns where all rows is equal to the
            ignored_value will be removed from the resulting DataFrame.
            Defaults to "[IGNORED]".
        ignore_additional_columns (bool): If True,
            ignores extra columns in the CSV.

    Returns:
        DataFrame: The Spark DataFrame.
    """
    df = read_csv_path(spark, path, schema, sep, ignore_additional_columns)
    df = _remove_ignored_columns(df, schema, ignored_value)
    return df
