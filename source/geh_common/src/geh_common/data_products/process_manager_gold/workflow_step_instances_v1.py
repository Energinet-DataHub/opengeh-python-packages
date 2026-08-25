import pyspark.sql.types as T

nullable = True

database_name = "process_manager_gold"

view_name = "workflow_step_instances_v1"

schema = T.StructType(
    [
        T.StructField("id", T.StringType(), not nullable),
        T.StructField("workflow_instance_id", T.StringType(), not nullable),
        T.StructField("orchestration_description_id", T.StringType(), nullable),
        T.StructField("orchestration_instance_id", T.StringType(), nullable),
        T.StructField("description", T.StringType(), nullable),
        T.StructField("lifecycle_state", T.IntegerType(), not nullable),
        T.StructField("lifecycle_created_at", T.TimestampType(), not nullable),
        T.StructField("lifecycle_completed_at", T.TimestampType(), nullable),
        T.StructField("actor_number", T.StringType(), nullable),
        T.StructField("actor_role", T.StringType(), nullable),
        T.StructField("archived_message_id", T.StringType(), nullable),
        T.StructField("is_deleted", T.BooleanType(), not nullable),
        T.StructField("exported_at", T.TimestampType(), not nullable),
    ]
)
