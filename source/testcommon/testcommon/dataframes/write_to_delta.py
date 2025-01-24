import os

from pyspark.sql import types as T
from pyspark.sql import SparkSession

from testcommon.dataframes import read_csv

def write_when_files_to_delta(
    spark: SparkSession,
    scenario_path: str,
    files: list[tuple[str, T.StructType]]
) -> None:

    for file_name, schema in files:
        file_path = f"{scenario_path}/when/{file_name}"
        if not os.path.exists(file_path):
            continue
        df = read_csv(
            spark,
            file_path,
            schema,
        )
        df.write.mode("overwrite").saveAsTable(file_name.removesuffix(".csv"))
