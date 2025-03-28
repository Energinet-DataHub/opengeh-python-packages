import pyspark.sql.types as t

nullable = True

# Schema name
schema = "measurements_gold"

# View name
electrical_heating_v1 = t.StructType(
    # Fields are nullable, but we are checking for nulls in the view
    [
        t.StructField("metering_point_id", t.StringType(), nullable),
        t.StructField("quantity", t.DecimalType(18, 3), nullable),
        t.StructField("observation_time", t.TimestampType(), nullable),
        t.StructField("metering_point_type", t.StringType(), nullable),
    ]
)
