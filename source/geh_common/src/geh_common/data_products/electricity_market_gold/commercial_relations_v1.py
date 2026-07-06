import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_gold"

view_name = "commercial_relations_v1"

schema = T.StructType(
    [
        T.StructField("metering_point_id", T.StringType(), not nullable),
        T.StructField("energy_supplier_id", T.StringType(), nullable),
        T.StructField("customer_id", T.StringType(), nullable),
        T.StructField("valid_from", T.TimestampType(), not nullable),
        T.StructField("valid_to", T.TimestampType(), not nullable),
        T.StructField(
            "electrical_heating_periods",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("valid_from", T.TimestampType(), not nullable),
                        T.StructField("valid_to", T.TimestampType(), not nullable),
                    ]
                )
            ),
            not nullable,
        ),
        T.StructField("is_current", T.BooleanType(), not nullable),
        T.StructField(
            "energy_supplier_periods",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("energy_supplier_period_id", T.StringType(), nullable),
                        T.StructField("valid_from", T.TimestampType(), not nullable),
                        T.StructField("valid_to", T.TimestampType(), not nullable),
                        T.StructField("energy_supplier_id", T.StringType(), nullable),
                        T.StructField("web_access_code", T.StringType(), nullable),
                    ]
                )
            ),
            not nullable,
        ),
    ]
)
