import pyspark.sql.types as T

nullable = True

database_name = "market_participants_gold"

view_name = "grid_access_providers_v1"

schema = T.StructType(
    [
        T.StructField("grid_access_provider_key", T.StringType(), not nullable),
        #
        T.StructField("grid_access_provider_name", T.StringType(), not nullable),
        #
        T.StructField("grid_access_provider_cvr", T.StringType(), not nullable),
        #
        T.StructField("grid_access_provider_id", T.StringType(), not nullable),
        #
        T.StructField("grid_area_id", T.StringType(), not nullable),
        #
        T.StructField("price_area_code", T.StringType(), not nullable),
        #
        T.StructField("created", T.TimestampType(), nullable),
    ]
)
