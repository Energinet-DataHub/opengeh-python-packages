from pyspark.sql import DataFrame, SparkSession


def stream(spark: SparkSession, table: str, ignore_changes: bool = True, skip_change_commits: bool = True) -> DataFrame:
    return (
        spark.readStream.format("delta")
        .option("ignoreChanges", ignore_changes)
        .option("skipChangeCommits", skip_change_commits)
        .table(table)
    )
