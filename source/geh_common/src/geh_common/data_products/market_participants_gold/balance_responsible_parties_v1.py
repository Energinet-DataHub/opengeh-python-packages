import pyspark.sql.types as T

nullable = True

database_name = "market_participants_gold"

view_name = "balance_responsible_parties_v1"

schema = T.StructType(
    [
        T.StructField("balance_responsible_key", T.StringType(), not nullable),
        #
        T.StructField("balance_responsible_name", T.StringType(), not nullable),
        #
        T.StructField("balance_responsible_cvr", T.StringType(), not nullable),
        #
        T.StructField("balance_responsible_party_id", T.StringType(), not nullable),
        #
        # UTC time
        T.StructField("valid_from", T.TimestampType(), not nullable),
        #
        # UTC time
        T.StructField("valid_to", T.TimestampType(), nullable),
        #
        T.StructField("grid_area_id", T.StringType(), not nullable),
        #
        T.StructField("energy_supplier_id", T.StringType(), not nullable),
        #
        # 'consumption' or 'production'
        T.StructField("metering_point_type", T.StringType(), not nullable),
        #
        T.StructField("created", T.TimestampType(), nullable),
    ]
)
