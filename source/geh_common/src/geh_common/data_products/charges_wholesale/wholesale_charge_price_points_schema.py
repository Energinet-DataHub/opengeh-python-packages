from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

wholesale_charge_price_points_schema = StructType(
    [
        StructField("charge_code", StringType(), False),
        StructField("charge_type", StringType(), False),
        StructField("charge_owner_id", StringType(), False),
        StructField("charge_price", DecimalType(14, 6), False),
        StructField("charge_time", TimestampType(), False),
    ]
)
