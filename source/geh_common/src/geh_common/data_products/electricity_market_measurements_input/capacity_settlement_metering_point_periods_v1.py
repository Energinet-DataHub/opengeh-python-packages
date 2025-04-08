import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_measurements_input"

# View
capacity_settlement_metering_point_periods_v1 = T.StructType(
    [
        # ID of the consumption metering point (parent)
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # Date when the consumption metering is either (a) entering 'connected'/'disconnected' first time or (b) move-in has occurred.
        # UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # Date when the consumption metering point is closed down or a move-in has occurred.
        # UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
        #
        # ID of the child metering point, which is of type 'capacity_settlement'
        T.StructField("child_metering_point_id", T.StringType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was created
        # UTC time
        T.StructField("child_period_from_date", T.TimestampType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was closed down
        # UTC time
        T.StructField("child_period_to_date", T.TimestampType(), nullable),
    ]
)
