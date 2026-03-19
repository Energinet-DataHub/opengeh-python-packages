import pyspark.sql.types as T

nullable = True

database_name = "charges_wholesale"

view_name = "charge_link_periods_v1"

schema = T.StructType(
    [
        T.StructField("charge_code", T.StringType(), not nullable),
        T.StructField("charge_type", T.StringType(), not nullable),
        T.StructField("charge_owner_id", T.StringType(), not nullable),
        T.StructField("metering_point_id", T.StringType(), not nullable),
        T.StructField("quantity", T.IntegerType(), not nullable),
        T.StructField("from_date", T.TimestampType(), not nullable),
        T.StructField("to_date", T.TimestampType(), not nullable),
    ]
)
