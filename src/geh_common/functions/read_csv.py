import copy

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def read_csv_path(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
    sep: str = ";",
    ignore_additional_columns: bool = True,
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the CSV file.
        schema (StructType): The schema of the CSV file.
        sep (str, optional): The separator of the CSV file. Defaults to ";".
        ignore_additional_columns (bool): If True,
            ignores extra columns in the CSV.

    Returns:
        DataFrame: The Spark DataFrame.
    """
    conf = {
        "format": "csv",
        "header": "true",
        "delimiter": sep,
        "encoding": "UTF-8",
        "quote": '"',
        "escape": '"',
        "dateFormat": "yyyy-MM-dd'T'HH:mm:ssXXX",
        "lineSep": "\n",
    }
    schema = copy.deepcopy(schema)
    raw_df = spark.read.load(path, **conf)
    transforms = []
    for field in schema.fields:
        if field.name in raw_df.columns:
            if isinstance(field.dataType, T.ArrayType):
                raw_df.schema[field.name].dataType = field.dataType
                transforms.append(
                    F.from_json(F.col(field.name), field.dataType).alias(field.name)
                )
            else:
                raw_df.schema[field.name].nullable = field.nullable
                transforms.append(
                    F.col(field.name).cast(field.dataType).alias(field.name)
                )
        else:
            raise ValueError(f"Column {field.name} not found in CSV")

    if not ignore_additional_columns:
        remaining_cols = set(raw_df.columns) - set(schema.fieldNames())
        for col in remaining_cols:
            value = raw_df.select(col).take(1)[0][col]
            try:
                if float(value) % 1 == 0:
                    data_type = T.IntegerType()
                else:
                    data_type = T.FloatType()
            except ValueError:
                if value.lower() == "true" or value.lower() == "false":
                    data_type = T.BooleanType()
                else:
                    data_type = T.StringType()

            transforms.append(F.col(col).cast(data_type).alias(col))
            schema.add(T.StructField(col, data_type, True))

    df = raw_df.select(*transforms)

    return spark.createDataFrame(df.rdd, schema)
