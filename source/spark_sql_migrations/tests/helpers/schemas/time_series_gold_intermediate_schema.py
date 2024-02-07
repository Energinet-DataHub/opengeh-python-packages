# Used for intermediate dataframe before written to gold
# Schema does not adhere the gold tables and is used for testing purpose only

from pyspark.sql.types import (
    StructField,
    StringType,
    TimestampType,
    StructType,
    DecimalType,
    IntegerType,
)

"""
Schema defining intermediate gold state of a time series, this is just used for an in memory dataframe not adhering the "not nulls" in the gold schema
"""
time_series_gold_intermediate_schema = StructType(
    [
        # GSRN (18 characters) that uniquely identifies the metering point
        # Example: 578710000000000103
        StructField("metering_point_id", StringType(), True),
        # Energy quantity for the given observation time.
        # Null when quality is missing.
        # Example: 1234.534217
        StructField("quantity", DecimalType(18, 6), True),
        # "missing" | "estimated" | "measured" | "calculated".
        # Example: "calculated"
        StructField("quality", StringType(), True),
        # The time when the energy was consumed/produced/exchanged
        StructField("observation_time", TimestampType(), True),
        # Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed
        StructField("transaction_id", StringType(), True),
        # Contains the local Danish time for when the time series data was persisted in source system
        StructField("transaction_insert_date", TimestampType(), True),
        # Represents the time series start interval, which is in UTC time matching the beginning of a date in the Europe/Copenhagen time zone
        StructField("valid_from_date", TimestampType(), True),
        # Represents the time series end interval, which is in UTC time matching the beginning of a date in the Europe/Copenhagen time zone. The moment is exclusive
        StructField("valid_to_date", TimestampType(), True),
        # The position the time series value had in the received time series
        StructField("position", IntegerType(), True),
        # When the row was initially persisted, in UTC
        StructField("created", TimestampType(), True),
        # Last time the row was updated, in UTC
        StructField("modified", TimestampType(), True),
    ])
