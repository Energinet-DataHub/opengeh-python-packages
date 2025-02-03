import os

from pyspark.sql import types as T
from pyspark.sql import SparkSession

from opengeh_utilities.testing.dataframes.read_csv import read_csv


def write_when_files_to_delta(
        spark: SparkSession,
        scenario_path: str,
        files: list[tuple[str, T.StructType]]
) -> None:
    """
    Writes a list of files to a delta table, using the filenames (without the file extension) as table names.
    If the Delta table does not exist, the function will create it. Otherwise, if a table already exists, its content
    will be overwritten

    Args:
        spark (SparkSession): The Spark session.
        scenario_path (str): The path to the scenario CSV file.
        files (list[tuple[str, T.StructType]]): A list of tuples containing filenames and their corresponding schemas.
    """

    for file_name, schema in files:
        file_path = f"{scenario_path}/when/{file_name}"
        if not os.path.exists(file_path):
            continue
        df = read_csv(
            spark,
            file_path,
            schema,
        )

        # Overwrite destination table with DataFrame
        try:
            df.write.mode("overwrite").saveAsTable(file_name.removesuffix(".csv"))
        except Exception as e:
            print(f"Error executing overwrite on table {file_name}: {str(e)}")
