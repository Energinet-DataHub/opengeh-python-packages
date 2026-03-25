import pyspark.sql.types as T

nullable = True

database_name = "charges_wholesale"

view_name = "charge_price_information_periods_v1"

schema = T.StructType(
    [
        T.StructField("charge_code", T.StringType(), not nullable),
        T.StructField("charge_type", T.StringType(), not nullable),
        T.StructField("charge_owner_id", T.StringType(), not nullable),
        T.StructField("resolution", T.StringType(), not nullable),
        T.StructField("is_tax", T.BooleanType(), not nullable),
        T.StructField("from_date", T.TimestampType(), nullable),
        T.StructField("to_date", T.TimestampType(), nullable),
    ]
)
