import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_reports_input"

view_name = "measurements_report_metering_point_periods_v1"

schema = T.StructType(
    [
        T.StructField("grid_area_code", T.StringType(), not nullable),
        T.StructField("metering_point_id", T.StringType(), not nullable),
        T.StructField("metering_point_type", T.StringType(), not nullable),
        T.StructField("resolution", T.StringType(), not nullable),
        T.StructField("energy_supplier_id", T.StringType(), nullable),
        T.StructField("physical_status", T.BooleanType(), not nullable),
        T.StructField("settlement_method", T.StringType(), nullable),
        T.StructField("unit", T.StringType(), not nullable),
        T.StructField("from_grid_area_code", T.StringType(), nullable),
        T.StructField("to_grid_area_code", T.StringType(), nullable),
        T.StructField("period_from_datetime", T.TimestampType(), not nullable),
        T.StructField("period_to_datetime", T.TimestampType(), nullable),
    ]
)
"""
Child metering points related to measurement reports.
"""
