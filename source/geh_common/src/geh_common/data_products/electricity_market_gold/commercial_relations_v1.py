import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_gold"

view_name = "commercial_relations_v1"

schema = T.StructType(
    [
        T.StructField("metering_point_id", T.StringType(), not nullable),
        T.StructField("energy_supplier_id", T.StringType(), not nullable),
        T.StructField("customer_id", T.StringType(), nullable),
        T.StructField("valid_from", T.TimestampType(), not nullable),
        T.StructField("valid_to", T.TimestampType(), nullable),
        T.StructField(
            "electrical_heating_periods",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("valid_from", T.LongType(), nullable),
                        T.StructField("valid_to", T.LongType(), nullable),
                    ]
                )
            ),
            not nullable,
        ),
        T.StructField("electrical_heating_active", T.BooleanType(), not nullable),
        T.StructField("is_current", T.BooleanType(), not nullable),
        T.StructField(
            "energy_supplier_periods",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("valid_from", T.LongType(), nullable),
                        T.StructField("valid_to", T.LongType(), nullable),
                        T.StructField("energy_supplier_id", T.StringType(), nullable),
                        T.StructField("web_access_code", T.StringType(), nullable),
                        T.StructField(
                            "contacts",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("disponent_name", T.StringType(), nullable),
                                        T.StructField("name", T.StringType(), nullable),
                                        T.StructField("relation_type", T.StringType(), nullable),
                                        T.StructField("has_cpr", T.BooleanType(), nullable),
                                        T.StructField("cvr", T.StringType(), nullable),
                                        T.StructField("is_protected_name", T.BooleanType(), nullable),
                                        T.StructField("email", T.StringType(), nullable),
                                        T.StructField("phone", T.StringType(), nullable),
                                        T.StructField("mobile", T.StringType(), nullable),
                                        T.StructField(
                                            "address",
                                            T.StructType(
                                                [
                                                    T.StructField("is_protected_address", T.BooleanType(), nullable),
                                                    T.StructField("attention", T.StringType(), nullable),
                                                    T.StructField("street_code", T.StringType(), nullable),
                                                    T.StructField("street_name", T.StringType(), nullable),
                                                    T.StructField("building_number", T.StringType(), nullable),
                                                    T.StructField("city_name", T.StringType(), nullable),
                                                    T.StructField("additional_city_name", T.StringType(), nullable),
                                                    T.StructField("dar_reference", T.StringType(), nullable),
                                                    T.StructField("country_code", T.StringType(), nullable),
                                                    T.StructField("floor", T.StringType(), nullable),
                                                    T.StructField("suite_number", T.StringType(), nullable),
                                                    T.StructField("postal_code", T.StringType(), nullable),
                                                    T.StructField("po_box", T.StringType(), nullable),
                                                    T.StructField("municipality_code", T.StringType(), nullable),
                                                ]
                                            ),
                                            nullable,
                                        ),
                                    ]
                                ),
                            ),
                            nullable,
                        ),
                    ]
                )
            ),
            not nullable,
        ),
    ]
)
