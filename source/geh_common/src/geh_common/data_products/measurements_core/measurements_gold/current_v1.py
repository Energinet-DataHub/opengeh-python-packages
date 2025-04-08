import pyspark.sql.types as t

nullable = True

# Schema name
schema = "measurements_gold"

# View name
current_v1 = t.StructType(
    # Fields are nullable, but we are checking for nulls in the view
    [
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("observation_time", t.TimestampType(), not nullable),
        t.StructField("quantity", t.DecimalType(18, 3), not nullable),
        t.StructField("quality", t.StringType(), not nullable),
        t.StructField("metering_point_type", t.StringType(), not nullable),
    ]
)
