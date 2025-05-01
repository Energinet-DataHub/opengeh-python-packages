import os


def test__new_env_variables():
    # Arrange
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    assert databricks_token is not None, "DATABRICKS_TOKEN is not set"
    assert databricks_host is not None, "DATABRICKS_HOST is not set"
    assert databricks_warehouse_id is not None, "DATABRICKS_WAREHOUSE_ID is not set"
