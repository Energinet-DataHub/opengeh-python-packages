import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_measurements_input"

view_name = "missing_measurements_log_metering_point_periods_v1"

schema = T.StructType(
    [
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # The code of the grid area that the metering point belongs to
        T.StructField("grid_area_code", T.StringType(), not nullable),
        #
        # Metering point resolution: PT1H/PT15M
        T.StructField("resolution", T.StringType(), not nullable),
        #
        # First time the metering point is connected - or when resolution or grid area code has changed. UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # The date where the the metering point is closed down - or when resolution or grid area code has changed. UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
    ]
)
"""
Metering point periods for missing measurements log. These are the periods where the metering points are active,
and can receive measurements. It includes all metering point types except internal_use (D99) and those calculated by Datahub (sub_type=calculated)
"""
