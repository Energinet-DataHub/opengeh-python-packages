import pyspark.sql.types as T

from geh_common.data_products.process_manager_gold import process_steps_v1


def test_process_steps_v1_contract() -> None:
    assert process_steps_v1.database_name == "process_manager_gold"
    assert process_steps_v1.view_name == "process_steps_v1"
    assert process_steps_v1.schema == T.StructType(
        [
            T.StructField("id", T.StringType(), nullable=False),
            T.StructField("process_id", T.StringType(), nullable=False),
            T.StructField("description", T.StringType(), nullable=True),
            T.StructField("state", T.StringType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
            T.StructField("completed_at", T.TimestampType(), nullable=True),
            T.StructField("actor_number", T.StringType(), nullable=True),
            T.StructField("actor_role", T.StringType(), nullable=True),
            T.StructField("archived_message_id", T.StringType(), nullable=True),
        ]
    )
