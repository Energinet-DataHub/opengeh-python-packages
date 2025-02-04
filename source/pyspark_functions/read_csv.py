from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T, functions as F


def read_csv(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
    sep: str = ";",
    ignored_value="[IGNORED]",
    ignore_additional_columns: bool = True

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
        "quote": "\"",
        "escape": "\"",
        "dateFormat": "yyyy-MM-dd'T'HH:mm:ssXXX",
        "lineSep": "\n",
    }

    raw_df = spark.read.load(path, **conf)

    # Check each column to see if all values are "[IGNORED]"
    ignore_check = raw_df.agg(
        *[F.every(F.col(c) == F.lit(ignored_value)).alias(c) for c in raw_df.columns]
    ).collect()

    # Get the columns that should be ignored
    ignored_cols = [
        c for c, v in ignore_check[0].asDict().items() if v and c in schema.fieldNames()
    ]

    raw_df = raw_df.drop(*ignored_cols)
    schema = T.StructType([field for field in schema.fields if field.name not in ignored_cols])

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
            if value == ignored_value:
                continue
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

