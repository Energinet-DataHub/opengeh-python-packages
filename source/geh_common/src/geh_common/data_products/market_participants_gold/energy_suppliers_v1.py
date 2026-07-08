import pyspark.sql.types as T

nullable = True

database_name = "market_participant_gold"

view_name = "energy_suppliers_v1"

schema = T.StructType(
    [
        T.StructField("energy_supplier_key", T.StringType(), not nullable),
        #
        T.StructField("energy_supplier_name", T.StringType(), not nullable),
        #
        T.StructField("energy_supplier_cvr", T.StringType(), not nullable),
        #
        T.StructField("energy_supplier_id", T.StringType(), not nullable),
        #
        T.StructField("created", T.TimestampType(), nullable),
    ]
)
