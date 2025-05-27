import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_reports_input"

view_name = "measurements_report_metering_point_periods_v1"

schema = T.StructType(
    [

        T.StructField("grid_area_code", T.StringType(), not nullable),
        #
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # All types of metering point types
        T.StructField("metering_point_type", T.StringType(), not nullable),
        #
        # 'P15M' | 'PT1H'
        T.StructField("resolution", T.StringType(), not nullable),
        #
        # GLN or EIC number of the energy supplier
        T.StructField("energy_supplier_id", T.StringType(), nullable),
        #
        # 'connected' or 'disconnected'
        T.StructField("physical_status", T.BooleanType(), not nullable),
        #
        # The unit of the quantity (e.g. "kWh", "kVArh" etc.)
        T.StructField("quantity_unit", T.StringType(), not nullable),
        #
        # Required for exchange, otherwise null
        T.StructField("from_grid_area_code", T.StringType(), nullable),
        #
        # Required for exchange, otherwise null
        T.StructField("to_grid_area_code", T.StringType(), nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
    ]
)
"""
Metering point periods used for generation of measurements reports.

Includes all metering points types but only those that are connected or disconnected.
"""
