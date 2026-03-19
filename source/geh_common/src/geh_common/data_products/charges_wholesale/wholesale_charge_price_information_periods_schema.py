from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

wholesale_charge_price_information_periods_schema = StructType(
    [
        StructField("charge_code", StringType(), False),
        StructField("charge_type", StringType(), False),
        StructField("charge_owner_id", StringType(), False),
        StructField("resolution", StringType(), False),
        StructField("is_tax", BooleanType(), False),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
    ]
)
