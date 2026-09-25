import pyspark.sql.types as T

nullable = True

database_name = "process_manager_gold"

view_name = "process_steps_v1"

schema = T.StructType(
    [
        T.StructField("id", T.StringType(), not nullable),
        T.StructField("process_id", T.StringType(), not nullable),
        T.StructField("description", T.StringType(), nullable),
        T.StructField("state", T.StringType(), not nullable),
        T.StructField("created_at", T.TimestampType(), not nullable),
        T.StructField("completed_at", T.TimestampType(), nullable),
        T.StructField("actor_number", T.StringType(), nullable),
        T.StructField("actor_role", T.StringType(), nullable),
        T.StructField("archived_message_id", T.StringType(), nullable),
    ]
)
