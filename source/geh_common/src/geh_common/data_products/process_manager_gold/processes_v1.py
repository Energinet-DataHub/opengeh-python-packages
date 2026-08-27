import pyspark.sql.types as T

nullable = True

database_name = "process_manager_gold"

view_name = "processes_v1"

schema = T.StructType(
    [
        T.StructField("id", T.StringType(), not nullable),
        T.StructField("business_reason", T.StringType(), nullable),
        T.StructField("validity_date", T.TimestampType(), nullable),
        T.StructField("metering_point_id", T.StringType(), nullable),
        T.StructField("state", T.StringType(), not nullable),
        T.StructField("created_at", T.TimestampType(), not nullable),
        T.StructField("started_at", T.TimestampType(), nullable),
        T.StructField("terminated_at", T.TimestampType(), nullable),
        T.StructField("created_by_actor_id", T.StringType(), nullable),
        T.StructField("created_by_actor_role", T.StringType(), nullable),
    ]
)
