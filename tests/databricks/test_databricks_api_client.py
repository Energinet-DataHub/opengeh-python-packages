import unittest
from unittest.mock import patch

from geh_common.databricks.databricks_api_client import DatabricksApiClient, RunLifeCycleState


class TestDatabricksApiClient(unittest.TestCase):
    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test__cancel_job_run__should_call_cancel_and_get_run(self, MockWorkspaceClient):
        # Arrange
        mock_client = MockWorkspaceClient.return_value
        mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "TERMINATED"
        mock_client.jobs.get_run.return_value.state.result_state = "CANCELED"

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        job_run_id = 12345

        # Act
        sut.cancel_job_run(job_run_id)

        # Assert
        mock_client.jobs.cancel_run.assert_called_once_with(run_id=job_run_id)
        mock_client.jobs.get_run.assert_called()

    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test_cancel_job_run_with_wait_for_cancellation(self, MockWorkspaceClient):
        # Arrange
        mock_client = MockWorkspaceClient.return_value
        mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "TERMINATED"
        mock_client.jobs.get_run.return_value.state.result_state = "CANCELED"

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        job_run_id = 12345

        # Act
        sut.cancel_job_run(job_run_id, wait_for_cancellation=True)

        # Assert
        mock_client.jobs.cancel_run.assert_called_once_with(run_id=job_run_id)
        mock_client.jobs.get_run.assert_called()

    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test__cancel_job_run__without_wait_for_cancellation__should_not_call(self, MockWorkspaceClient):
        # Arrange
        mock_client = MockWorkspaceClient.return_value

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        job_run_id = 12345

        # Act
        sut.cancel_job_run(job_run_id, wait_for_cancellation=False)

        # Assert
        mock_client.jobs.cancel_run.assert_called_once_with(run_id=job_run_id)
        mock_client.jobs.get_run.assert_not_called()

    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test__wait_for_job_state__reaches_target_state(self, MockWorkspaceClient):
        # Arrange
        mock_client = MockWorkspaceClient.return_value
        mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "TERMINATED"

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        run_id = 12345
        target_states = [RunLifeCycleState.TERMINATED.value]

        # Act & Assert
        sut.wait_for_job_state(run_id, target_states)

    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test__wait_for_job_state__when_job_keeps_running__raises_timeout_error(self, MockWorkspaceClient):
        # Arrange
        mock_client = MockWorkspaceClient.return_value
        mock_client.jobs.get_run.return_value.state.life_cycle_state.value = "RUNNING"

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        run_id = 12345
        target_states = [RunLifeCycleState.TERMINATED.value]

        # Act & Assert
        with self.assertRaises(TimeoutError):
            sut.wait_for_job_state(run_id, target_states, timeout=1, poll_interval=1)

    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test__wait_for_job_state__when_run_return_is_none__raises_exception_for_none_state(self, MockWorkspaceClient):
        # Arrange
        mock_client = MockWorkspaceClient.return_value
        mock_client.jobs.get_run.return_value.state = None

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        run_id = 12345
        target_states = [RunLifeCycleState.TERMINATED.value]

        # Act & Assert
        with self.assertRaises(Exception) as context:
            sut.wait_for_job_state(run_id, target_states)
        self.assertTrue("Job run status state is None" in str(context.exception))

    @patch("geh_common.databricks.databricks_api_client.WorkspaceClient")
    def test__wait_for_job_state__when_life_cycle_state_is_none__raises_exception_for_none_lifecycle_state(
        self, MockWorkspaceClient
    ):
        # Arrange
        mock_client = MockWorkspaceClient.return_value
        mock_client.jobs.get_run.return_value.state.life_cycle_state = None

        databricks_token = "fake_token"
        databricks_host = "https://fake-host"
        sut = DatabricksApiClient(databricks_token, databricks_host)

        run_id = 12345
        target_states = [RunLifeCycleState.TERMINATED.value]

        # Act & Assert
        with self.assertRaises(Exception) as context:
            sut.wait_for_job_state(run_id, target_states)
        self.assertTrue("Job run lifecycle state is None" in str(context.exception))
