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
        # 'PT15M' | 'PT1H' | 'P1M'
        T.StructField("resolution", T.StringType(), not nullable),
        #
        # GLN or EIC number of the energy supplier
        T.StructField("energy_supplier_id", T.StringType(), nullable),
        #
        # 'connected' or 'disconnected'
        T.StructField("physical_status", T.StringType(), not nullable),
        #
        # The unit of the quantity (e.g. "kWh", "kVArh", "Tonne" etc.)
        T.StructField("quantity_unit", T.StringType(), not nullable),
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

Includes all metering point types, but only when the physical status is connected or disconnected.
"""
