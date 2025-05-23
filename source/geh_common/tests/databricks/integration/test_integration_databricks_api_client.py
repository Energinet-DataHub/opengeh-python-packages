import os

from databricks.sdk.service.sql import (
    StatementState,
)

from geh_common.databricks.databricks_api_client import DatabricksApiClient


def test__execute_statement_returns_correct_state_and_data():
    """
    Test that the execute_statement method returns the correct state and data.
    """

    # Arrange
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    assert databricks_token is not None, "DATABRICKS_TOKEN environment variable is not set"
    assert databricks_host is not None, "DATABRICKS_HOST environment variable is not set"
    assert databricks_warehouse_id is not None, "DATABRICKS_WAREHOUSE_ID environment variable is not set"

    client = DatabricksApiClient(databricks_host=databricks_host, databricks_token=databricks_token)

    statement = "SELECT * FROM system.information_schema.catalogs"

    # Act
    response = client.execute_statement(
        statement=statement,
        warehouse_id=databricks_warehouse_id,
    )

    assert response.status.state == StatementState.SUCCEEDED, f"Query failed with error: {response.status.error}"
    assert response.result is not None, "Result is None"
