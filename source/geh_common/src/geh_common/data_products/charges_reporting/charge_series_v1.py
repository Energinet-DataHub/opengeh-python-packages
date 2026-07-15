import pyspark.sql.types as T

nullable = True

database_name = "charges_reporting"

view_name = "charge_series_v1"

schema = T.StructType(
    [
        T.StructField("code", T.StringType(), not nullable),
        T.StructField("type", T.StringType(), not nullable),
        T.StructField("owner", T.StringType(), not nullable),
        T.StructField("from_date", T.TimestampType(), not nullable),
        T.StructField("to_date", T.TimestampType(), not nullable),
        T.StructField("resolution", T.StringType(), not nullable),
        T.StructField("price", T.DecimalType(14, 6), not nullable),
    ]
)
