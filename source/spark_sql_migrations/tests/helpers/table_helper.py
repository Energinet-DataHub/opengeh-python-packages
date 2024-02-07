from pyspark.sql import DataFrame


def append_to_table(dataframe: DataFrame, full_table: str) -> None:
    dataframe.write.format("delta").mode("append").saveAsTable(full_table)
