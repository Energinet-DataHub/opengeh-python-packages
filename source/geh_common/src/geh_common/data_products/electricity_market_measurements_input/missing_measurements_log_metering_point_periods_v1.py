import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_measurements_input"

# View
missing_measurements_log_metering_point_periods_v1 = T.StructType(
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
        # UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
    ]
)
