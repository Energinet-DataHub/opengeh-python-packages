import pyspark.sql.types as T

from geh_common.data_products.process_manager_gold import processes_v1


def test_processes_v1_contract() -> None:
    assert processes_v1.database_name == "process_manager_gold"
    assert processes_v1.view_name == "processes_v1"
    assert processes_v1.schema == T.StructType(
        [
            T.StructField("id", T.StringType(), nullable=False),
            T.StructField("business_reason", T.StringType(), nullable=True),
            T.StructField("validity_date", T.TimestampType(), nullable=True),
            T.StructField("metering_point_id", T.StringType(), nullable=True),
            T.StructField("state", T.StringType(), nullable=False),
            T.StructField("termination_state", T.StringType(), nullable=True),
            T.StructField("created_at", T.TimestampType(), nullable=False),
            T.StructField("started_at", T.TimestampType(), nullable=True),
            T.StructField("terminated_at", T.TimestampType(), nullable=True),
            T.StructField("created_by_actor_id", T.StringType(), nullable=True),
            T.StructField("created_by_actor_role", T.StringType(), nullable=True),
        ]
    )
