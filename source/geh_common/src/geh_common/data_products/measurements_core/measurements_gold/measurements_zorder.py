import pyspark.sql.types as t

nullable = True

database_name = "measurements_gold"

view_name = "measurements_zorder"

schema = t.StructType(
    [
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("orchestration_type", t.StringType(), not nullable),
        t.StructField("orchestration_instance_id", t.StringType(), not nullable),
        t.StructField("observation_time", t.TimestampType(), not nullable),
        t.StructField("quantity", t.DecimalType(18, 3), nullable),
        t.StructField("quality", t.StringType(), not nullable),
        t.StructField("metering_point_type", t.StringType(), not nullable),
        t.StructField("unit", t.StringType(), not nullable),
        t.StructField("resolution", t.StringType(), not nullable),
        t.StructField("transaction_id", t.StringType(), not nullable),
        t.StructField("transaction_creation_datetime", t.TimestampType(), not nullable),
        t.StructField("created", t.TimestampType(), not nullable),
        t.StructField("modified", t.TimestampType(), not nullable),
        t.StructField("partition_metering_point_id", t.IntegerType(), not nullable),
        t.StructField("partition_year", t.IntegerType(), not nullable),
        t.StructField("partition_month", t.IntegerType(), not nullable),
    ]
)
