from pyspark.sql.types import (
    StructField,
    StringType,
    TimestampType,
    StructType, IntegerType, DecimalType, FloatType, BooleanType,
)


test_nullability_schema = StructType(
    [
        StructField("string_type_nullable", StringType(), True),
        StructField("string_type", StringType(), False),
        StructField("integer_type_nullable", IntegerType(), True),
        StructField("integer_type", IntegerType(), False),
        StructField("timestamp_type_nullable", TimestampType(), True),
        StructField("timestamp_type", TimestampType(), False),
        StructField("decimal_type_nullable", DecimalType(), True),
        StructField("decimal_type", DecimalType(), False),
        StructField("float_type_nullable", FloatType(), True),
        StructField("float_type", FloatType(), False),
        StructField("boolean_type_nullable", BooleanType(), True),
        StructField("boolean_type", BooleanType(), False),
    ]
)
