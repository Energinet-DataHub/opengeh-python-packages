import pyspark.sql.types as T

nullable = True

database_name = "process_manager_gold"

view_name = "workflow_instances_v1"

schema = T.StructType(
    [
        T.StructField("workflow_id", T.StringType(), not nullable),
        T.StructField("workflow_description_id", T.StringType(), not nullable),
        T.StructField("business_reason", T.StringType(), nullable),
        T.StructField("validity_date", T.TimestampType(), nullable),
        T.StructField("metering_point_id", T.StringType(), nullable),
        T.StructField("lifecycle_state", T.IntegerType(), not nullable),
        T.StructField("lifecycle_termination_id", T.IntegerType(), nullable),
        T.StructField("lifecycle_created_at", T.TimestampType(), not nullable),
        T.StructField("lifecycle_started_at", T.TimestampType(), nullable),
        T.StructField("lifecycle_terminated_at", T.TimestampType(), nullable),
        T.StructField("lifecycle_created_by_actor_id", T.StringType(), nullable),
        T.StructField("lifecycle_created_by_actor_role", T.StringType(), nullable),
        T.StructField("is_deleted", T.BooleanType(), not nullable),
        T.StructField("exported_at", T.TimestampType(), not nullable),
    ]
)
