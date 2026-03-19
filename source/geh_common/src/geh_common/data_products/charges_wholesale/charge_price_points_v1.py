import pyspark.sql.types as T

nullable = True

database_name = "charges_wholesale"

view_name = "charge_price_points_v1"

schema = T.StructType(
    [
        T.StructField("charge_code", T.StringType(), not nullable),
        T.StructField("charge_type", T.StringType(), not nullable),
        T.StructField("charge_owner_id", T.StringType(), not nullable),
        T.StructField("charge_price", T.DecimalType(14, 6), not nullable),
        T.StructField("charge_time", T.TimestampType(), not nullable),
    ]
)
