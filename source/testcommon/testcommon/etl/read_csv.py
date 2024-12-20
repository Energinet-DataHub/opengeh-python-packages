from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T, functions as F


def read_csv(
    spark: SparkSession, path: str, schema: T.StructType, sep: str = ";"
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the CSV file.
        schema (StructType): The schema of the CSV file.
        sep (str, optional): The separator of the CSV file. Defaults to ";".

    Returns:
        DataFrame: The Spark DataFrame.
    """
    raw_df = spark.read.csv(path, header=True, sep=sep)

    transforms = []
    for field in schema.fields:
        if field.name in raw_df.columns:
            if isinstance(field.dataType, T.ArrayType):
                transforms.append(
                    F.from_json(F.col(field.name), field.dataType).alias(
                        field.name
                    )
                )
            else:
                transforms.append(
                    F.col(field.name).cast(field.dataType).alias(field.name)
                )

    df = raw_df.select(*transforms)
    return _fix_nullable(df, schema)


def _fix_nullable(df: DataFrame, schema: T.StructType):
    for field in df.schema.fields:
        assert (
            field.name in schema.fieldNames()
        ), f"Field {field.name} not in schema"
        if isinstance(schema[field.name].dataType, T.ArrayType):
            df.schema[field.name].dataType = schema[field.name].dataType
        else:
            df.schema[field.name].nullable = schema[field.name].nullable

    return df
