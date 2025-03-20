from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.sql import Disposition, StatementState

from geh_common.databricks.databricks_api_client import DatabricksApiClient, RunLifeCycleState


def create_sut():
    databricks_token = "fake_token"
    databricks_host = "https://fake-host"
    return DatabricksApiClient(databricks_token, databricks_host)


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__cancel_job_run__should_call_cancel_and_get_run(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "TERMINATED"
    mock_client.jobs.get_run.return_value.state.result_state = "CANCELED"

    job_run_id = 12345
    sut = create_sut()

    # Act
    sut.cancel_job_run(job_run_id)

    # Assert
    mock_client.jobs.cancel_run.assert_called_once_with(run_id=job_run_id)
    mock_client.jobs.get_run.assert_called()


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test_cancel_job_run_with_wait_for_cancellation(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "TERMINATED"
    mock_client.jobs.get_run.return_value.state.result_state = "CANCELED"

    job_run_id = 12345
    sut = create_sut()

    # Act
    sut.cancel_job_run(job_run_id, wait_for_cancellation=True)

    # Assert
    mock_client.jobs.cancel_run.assert_called_once_with(run_id=job_run_id)
    mock_client.jobs.get_run.assert_called()


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__cancel_job_run__without_wait_for_cancellation__should_not_call(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value

    job_run_id = 12345
    sut = create_sut()

    # Act
    sut.cancel_job_run(job_run_id, wait_for_cancellation=False)

    # Assert
    mock_client.jobs.cancel_run.assert_called_once_with(run_id=job_run_id)
    mock_client.jobs.get_run.assert_not_called()


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__wait_for_job_state__reaches_target_state(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "TERMINATED"

    run_id = 12345
    target_states = [RunLifeCycleState.TERMINATED.value]
    sut = create_sut()

    # Act & Assert
    sut.wait_for_job_state(run_id, target_states)


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__wait_for_job_state__when_job_keeps_running__raises_timeout_error(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "RUNNING"

    run_id = 12345
    target_states = [RunLifeCycleState.TERMINATED.value]
    sut = create_sut()

    # Act & Assert
    with pytest.raises(TimeoutError):
        sut.wait_for_job_state(run_id, target_states, timeout=1, poll_interval=1)


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__wait_for_job_state__when_run_return_is_none__raises_exception_for_none_state(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.get_run.return_value.state = None

    run_id = 12345
    target_states = [RunLifeCycleState.TERMINATED.value]
    sut = create_sut()

    # Act & Assert
    with pytest.raises(Exception) as context:
        sut.wait_for_job_state(run_id, target_states)
        assert "Job run status state is None" in str(context.from_exception(context.value))


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__wait_for_job_state__when_life_cycle_state_is_none__raises_exception_for_none_lifecycle_state(
    MockWorkspaceClient,
):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.get_run.return_value.state.life_cycle_state = None

    run_id = 12345
    target_states = [RunLifeCycleState.TERMINATED.value]
    sut = create_sut()

    # Act & Assert
    with pytest.raises(Exception) as context:
        sut.wait_for_job_state(run_id, target_states)
        assert "Job run lifecycle state is None" in str(context.from_exception(context.value))


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__get_latest_job_run_id__returns_run_id(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.list_runs.return_value = iter([MagicMock(run_id=12345)])

    job_id = 67890
    sut = create_sut()

    # Act
    run_id = sut.get_latest_job_run_id(job_id)

    # Assert
    assert run_id == 12345
    mock_client.jobs.list_runs.assert_called_once_with(job_id=job_id, active_only=True)


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__get_latest_job_run_id__when_no_runs_found__returns_none(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.list_runs.return_value = iter([])

    job_id = 67890
    sut = create_sut()

    # Act
    run_id = sut.get_latest_job_run_id(job_id)

    # Assert
    assert run_id is None


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__get_latest_job_run_id_when_active_only_is_false__should_call_with_active_only_set_to_false(
    MockWorkspaceClient,
):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_client.jobs.list_runs.return_value = iter([MagicMock(run_id=12345)])

    job_id = 67890
    sut = create_sut()

    # Act
    run_id = sut.get_latest_job_run_id(job_id, active_only=False)

    # Assert
    assert run_id == 12345
    mock_client.jobs.list_runs.assert_called_once_with(job_id=job_id, active_only=False)


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__execute_statement__when_query_is_invalid__should_raise_exception(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_response = MagicMock()
    mock_response.status.state = StatementState.FAILED
    mock_client.statement_execution.execute_statement.return_value = mock_response

    sut = create_sut()

    invalid_query = "invalid query"

    # Act & Assert
    with pytest.raises(Exception) as context:
        sut.execute_statement(warehouse_id="fake_warehouse_id", statement=invalid_query)

    assert context.value is not None
    assert "Statement execution failed" in str(context.value)

    mock_client.statement_execution.execute_statement.assert_called_once_with(
        warehouse_id="fake_warehouse_id", statement=invalid_query, disposition=Disposition.INLINE
    )


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__should_succeed_on_valid_query(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_response = MagicMock()
    mock_response.status.state = StatementState.SUCCEEDED
    mock_client.statement_execution.execute_statement.return_value = mock_response

    sut = create_sut()

    valid_query = "valid query"

    # Act & Assert
    try:
        response = sut.execute_statement(warehouse_id="fake_warehouse_id", statement=valid_query)
    except Exception as e:
        pytest.fail(f"An exception was raised: {e}")

    assert response is not None
    assert response.status.state is StatementState.SUCCEEDED

    mock_client.statement_execution.execute_statement.assert_called_once_with(
        warehouse_id="fake_warehouse_id", statement=valid_query, disposition=Disposition.INLINE
    )


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__should_raise_exception_if_disposition_is_external_links(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_response = MagicMock()
    mock_response.status.state = StatementState.FAILED
    mock_client.statement_execution.execute_statement.return_value = mock_response

    sut = create_sut()

    statement = "query"

    # Act & Assert
    with pytest.raises(NotImplementedError) as context:
        sut.execute_statement(
            warehouse_id="fake_warehouse_id", statement=statement, disposition=Disposition.EXTERNAL_LINKS
        )

    assert context.value is not None
    assert "Execute statement only supports disposition INLINE" in str(context.value)


@patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
def test__should_raise_exception_after_exceeding_timeout_input(MockWorkspaceClient):
    # Arrange
    mock_client = MockWorkspaceClient.return_value
    mock_response = MagicMock()
    mock_response.status.state = StatementState.RUNNING
    mock_client.statement_execution.execute_statement.return_value = mock_response
    mock_client.statement_execution.get_statement.return_value = mock_response

    sut = create_sut()

    statement = "query"

    # Act & Assert
    # Asserts for timeout = 2
    with pytest.raises(Exception) as context:
        sut.execute_statement(
            warehouse_id="fake_warehouse_id",
            statement=statement,
            disposition=Disposition.INLINE,
            wait_for_response=True,
            timeout=2,
        )

    assert context.value is not None
    assert "Statement execution timed out after" in str(context.value)

    # Asserts for timeout = 11
    with pytest.raises(Exception) as context:
        sut.execute_statement(
            warehouse_id="fake_warehouse_id",
            statement=statement,
            disposition=Disposition.INLINE,
            wait_for_response=True,
            timeout=11,
        )

    assert context.value is not None
    assert "Statement execution timed out after" in str(context.value)
