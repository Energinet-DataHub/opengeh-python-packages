import pyspark.sql.types as t

nullable = True

database_name = "measurements_gold"

view_name = "sap_delta_v1"

schema = t.StructType(
    # Fields are nullable, but we are checking for nulls in the view
    [
        t.StructField("serie_no", t.DecimalType(14, 0), not nullable),
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("transaction_id", t.StringType(), not nullable),
        t.StructField("transaction_creation_datetime", t.TimestampType(), not nullable),
        t.StructField("start_time", t.TimestampType(), not nullable),
        t.StructField("end_time", t.TimestampType(), not nullable),
        t.StructField("unit", t.StringType(), not nullable),
        t.StructField("resolution", t.StringType(), not nullable),
        t.StructField("is_cancelled", t.BooleanType(), not nullable),
        t.StructField("created", t.TimestampType(), not nullable),
    ]
)
