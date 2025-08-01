from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def read_csv(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
    sep: str = ";",
    ignored_value="[IGNORED]",
    datetime_format: str | None = None,
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the CSV file.
        schema (StructType): The schema of the CSV file.
        sep (str, optional): The separator of the CSV file. Defaults to ";".
        ignored_value (str, optional): Columns where all rows is equal to the
            ignored_value will be removed from the resulting DataFrame.
            Defaults to "[IGNORED]".

    Returns:
        DataFrame: The Spark DataFrame.
    """
    raw_df = spark.read.csv(path, header=True, sep=sep)

    # Check each column to see if all values are "[IGNORED]"
    ignore_check = raw_df.agg(*[F.every(F.col(c) == F.lit(ignored_value)).alias(c) for c in raw_df.columns]).collect()

    # Get the columns that should be ignored
    ignored_cols = [c for c, v in ignore_check[0].asDict().items() if v and c in schema.fieldNames()]

    raw_df = raw_df.drop(*ignored_cols)

    # Filter out ignored columns from the schema
    filtered_schema = T.StructType([field for field in schema.fields if field.name not in ignored_cols])

    transforms = []
    for field in filtered_schema.fields:
        if field.name in raw_df.columns:
            if isinstance(field.dataType, T.ArrayType):
                transforms.append(F.from_json(F.col(field.name), field.dataType).alias(field.name))
            elif isinstance(field.dataType, T.TimestampType) and datetime_format:
                transforms.append(F.to_timestamp(F.col(field.name), datetime_format).alias(field.name))
            else:
                transforms.append(F.col(field.name).cast(field.dataType).alias(field.name))

    df = raw_df.select(*transforms)
    return spark.createDataFrame(df.rdd, schema=filtered_schema, verifySchema=True)
