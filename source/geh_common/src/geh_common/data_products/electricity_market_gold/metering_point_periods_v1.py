import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_gold"

view_name = "metering_point_periods_v1"

schema = T.StructType(
    [
        T.StructField("metering_point_period_key", T.StringType(), not nullable),
        T.StructField("metering_point_id", T.StringType(), not nullable),
        T.StructField("valid_from", T.TimestampType(), not nullable),
        T.StructField("valid_to", T.TimestampType(), not nullable),
        T.StructField("is_current", T.BooleanType(), not nullable),
        T.StructField("type", T.StringType(), not nullable),
        T.StructField("sub_type", T.StringType(), not nullable),
        T.StructField("time_resolution", T.StringType(), not nullable),
        T.StructField("grid_area_id", T.StringType(), not nullable),
        T.StructField("asset_type", T.StringType(), nullable),
        T.StructField("asset_capacity", T.DecimalType(29, 7), nullable),
        T.StructField("connection_state", T.StringType(), not nullable),
        T.StructField("disconnection_type", T.StringType(), nullable),
        T.StructField("energy_unit", T.StringType(), not nullable),
        T.StructField("fuel_type", T.BooleanType(), nullable),
        T.StructField("from_grid_area_id", T.StringType(), nullable),
        T.StructField("to_grid_area_id", T.StringType(), nullable),
        T.StructField("meter_id", T.StringType(), nullable),
        T.StructField("connection_type", T.StringType(), nullable),
        T.StructField("settlement_group", T.StringType(), nullable),
        T.StructField("parent_metering_point_id", T.StringType(), nullable),
        T.StructField("power_limit_amperes", T.DecimalType(29, 7), nullable),
        T.StructField("power_limit_kw", T.DecimalType(29, 7), nullable),
        T.StructField("power_plant_gsrn", T.StringType(), nullable),
        T.StructField("product", T.StringType(), nullable),
        T.StructField("production_obligation", T.BooleanType(), nullable),
        T.StructField("settlement_method", T.StringType(), nullable),
        T.StructField("settlement_month", T.IntegerType(), nullable),
        T.StructField("settlement_date", T.TimestampType(), nullable),
        T.StructField("remarks", T.StringType(), nullable),
        T.StructField("street_name", T.StringType(), nullable),
        T.StructField("street_code", T.StringType(), nullable),
        T.StructField("building_number", T.StringType(), nullable),
        T.StructField("city_name", T.StringType(), nullable),
        T.StructField("additional_city_name", T.StringType(), nullable),
        T.StructField("dar_reference", T.StringType(), nullable),
        T.StructField("is_actual_address", T.BooleanType(), nullable),
        T.StructField("country_code", T.StringType(), nullable),
        T.StructField("floor", T.StringType(), nullable),
        T.StructField("suite_number", T.StringType(), nullable),
        T.StructField("postal_code", T.StringType(), nullable),
        T.StructField("municipality_code", T.StringType(), nullable),
    ]
)
