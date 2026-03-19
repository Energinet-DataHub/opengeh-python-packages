from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

wholesale_charge_link_periods_schema = StructType(
    [
        StructField("charge_code", StringType(), False),
        StructField("charge_type", StringType(), False),
        StructField("charge_owner_id", StringType(), False),
        StructField("metering_point_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("from_date", TimestampType(), False),
        StructField("to_date", TimestampType(), False),
    ]
)
