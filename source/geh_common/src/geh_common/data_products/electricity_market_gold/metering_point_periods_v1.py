import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_gold"

view_name = "metering_point_periods_v1"

schema = T.StructType(
    [
        T.StructField("metering_point_period_key", T.StringType(), False),
        T.StructField("metering_point_id", T.StringType(), False),
        T.StructField("valid_from", T.TimestampType(), False),
        T.StructField("valid_to", T.TimestampType(), False),
        T.StructField("is_current", T.BooleanType(), False),
        T.StructField("type", T.StringType(), False),
        T.StructField("sub_type", T.StringType(), False),
        T.StructField("time_resolution", T.StringType(), False),
        T.StructField("grid_area_id", T.StringType(), False),
        T.StructField("asset_type", T.StringType(), True),
        T.StructField("asset_capacity", T.DecimalType(29, 7), True),
        T.StructField("connection_state", T.StringType(), False),
        T.StructField("disconnection_type", T.StringType(), True),
        T.StructField("energy_unit", T.StringType(), False),
        T.StructField("fuel_type", T.BooleanType(), True),
        T.StructField("from_grid_area_id", T.StringType(), True),
        T.StructField("to_grid_area_id", T.StringType(), True),
        T.StructField("meter_id", T.StringType(), True),
        T.StructField("connection_type", T.StringType(), True),
        T.StructField("settlement_group", T.StringType(), True),
        T.StructField("parent_metering_point_id", T.StringType(), True),
        T.StructField("power_limit_amperes", T.DecimalType(29, 7), True),
        T.StructField("power_limit_kw", T.DecimalType(29, 7), True),
        T.StructField("power_plant_gsrn", T.StringType(), True),
        T.StructField("product", T.StringType(), True),
        T.StructField("production_obligation", T.BooleanType(), True),
        T.StructField("settlement_method", T.StringType(), True),
        T.StructField("settlement_month", T.IntegerType(), True),
        T.StructField("settlement_date", T.TimestampType(), True),
        T.StructField("remarks", T.StringType(), True),
        T.StructField("street_name", T.StringType(), True),
        T.StructField("street_code", T.StringType(), True),
        T.StructField("building_number", T.StringType(), True),
        T.StructField("city_name", T.StringType(), True),
        T.StructField("additional_city_name", T.StringType(), True),
        T.StructField("dar_reference", T.StringType(), True),
        T.StructField("is_actual_address", T.BooleanType(), True),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("floor", T.StringType(), True),
        T.StructField("suite_number", T.StringType(), True),
        T.StructField("postal_code", T.StringType(), True),
        T.StructField("municipality_code", T.StringType(), True),
    ]
)
