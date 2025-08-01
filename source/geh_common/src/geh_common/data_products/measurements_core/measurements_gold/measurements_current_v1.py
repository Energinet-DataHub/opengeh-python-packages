import pyspark.sql.types as t

nullable = True

database_name = "measurements_gold"

view_name = "measurements_current_v1"

schema = t.StructType(
    # Fields are nullable, but we are checking for nulls in the view, except for quantity
    [
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("observation_time", t.TimestampType(), not nullable),
        t.StructField("quantity", t.DecimalType(18, 3), nullable),
        t.StructField("quality", t.StringType(), not nullable),
        t.StructField("metering_point_type", t.StringType(), not nullable),
    ]
)
