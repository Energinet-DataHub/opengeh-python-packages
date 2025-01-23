from pyspark.sql import SparkSession
from pyspark.sql import types as T

from testcommon.dataframes import read_csv

def write_when_files_to_delta(
    spark: SparkSession,
    scenario_path: str,
    files: list[tuple[str, T.StructType]]
) -> None:

    for file_name, schema in files:
        df = read_csv(
            spark,
            f"{scenario_path}/when/{file_name}",
            schema,
        )
        spark.saveAsTable(file_name, df)
