# Used for testing business validation of time series data when going from silver to gold
# Schema does not adhere to any silver schema and is used for testing purpose only
import package.constants.time_series_silver_col_name as TimeSeries
from pyspark.sql.types import (
    StructField,
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

"""
Schema defining the dataframe used when doing business validation of time series going from silver to gold.
"""
time_series_with_combined_mp_data_schema = StructType(
    [
        # Time series' metering point id
        # GSRN (18 characters) that uniquely identifies the metering point
        # Example: 578710000000000103
        StructField(
            TimeSeries.TimeSeriesTemporaryValues.metering_point_id,
            StringType(),
            False,
        ),
        # Type of metering point
        # Example: "E17"
        StructField(
            TimeSeries.TimeSeriesTemporaryValues.type_of_mp,
            StringType(),
            False,
        ),
        # Time series' resolution
        # Example: 'PT1H'
        StructField(
            TimeSeries.TimeSeriesTemporaryValues.resolution, StringType(), False
        ),
        # Time series' transaction id
        StructField("transaction_id", StringType(), False),
        # Time series' valid from date
        StructField(
            TimeSeries.TimeSeriesTemporaryValues.valid_from_date,
            TimestampType(),
            False,
        ),
        # Time series' valid to date
        StructField(
            TimeSeries.TimeSeriesTemporaryValues.valid_to_date,
            TimestampType(),
            False,
        ),
        # The combined metering point state's metering point id
        StructField(
            TimeSeries.MeteringPointAliased.metering_point_id,
            StringType(),
            True,
        ),
        # The combined metering point state's earliest valid from date
        StructField(
            TimeSeries.MeteringPointGrouped.min_valid_from_date,
            TimestampType(),
            True,
        ),
        # The combined metering point state's latest valid to date
        StructField(
            TimeSeries.MeteringPointGrouped.max_valid_to_date,
            TimestampType(),
            True,
        ),
        # The combined metering point state's list of meter reading occurrence during the time series interval
        StructField(
            TimeSeries.MeteringPointCalculatedValues.meter_reading_occurrences,
            ArrayType(StringType(), True),
            True,
        ),
        # The combined metering point state's list of physical statuses during the time series interval
        StructField(
            TimeSeries.MeteringPointCalculatedValues.physical_statuses,
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "values",
            ArrayType(
                StructType(
                    [
                        StructField("position", IntegerType(), True),
                        StructField("quality", StringType(), True),
                        StructField("quantity", DecimalType(18, 6), True)
                    ]
                ),
                True,
            ),
            False,
        )
    ]
)
