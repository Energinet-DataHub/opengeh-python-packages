import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_measurements_input"

view_name = "electrical_heating_child_metering_points_v1"

schema = T.StructType(
    [
        #
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # 'supply_to_grid' | 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
        T.StructField("metering_point_type", T.StringType(), not nullable),
        #
        # 'calculated' | 'virtual' | 'physical'
        T.StructField("metering_point_sub_type", T.StringType(), not nullable),
        #
        # GSRN number
        T.StructField("parent_metering_point_id", T.StringType(), not nullable),
        #
        # The date when the child metering point was coupled to the parent metering point
        # UTC time
        T.StructField("coupled_date", T.TimestampType(), not nullable),
        #
        # The date when the child metering point was uncoupled from the parent metering point
        # UTC time
        T.StructField("uncoupled_date", T.TimestampType(), nullable),
    ]
)
"""
Child metering points related to electrical heating.

Periods are included when
- the metering point is of type
    'supply_to_grid' | 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
- the metering point is coupled to a parent metering point
    Note: The same child metering point cannot be re-coupled after being uncoupled
- 'supply_to_grid' and 'consumption_from_grid' is only included when the parent consumption metering point has net_settlement_group 2 or 6.
- the child metering point physical status is connected or disconnected.
- the period does not end before 2021-01-01

Formatting is according to ADR-144 with the following constraints:
- No column may use quoted values
- All date/time values must include seconds
"""
