import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


class DatabricksApiClient:
    def __init__(self, databricks_token: str, databricks_host: str) -> None:
        self.client = WorkspaceClient(host=databricks_host, token=databricks_token)

    def get_job_id(self, job_name: str) -> int:
        """Get the job ID for a Databricks job.

        Args:
            job_name (str): The name of the job.

        Returns:
            int: The ID of the job.
        """
        job_list = self.client.jobs.list()
        for job in job_list:
            if job.settings is not None and job.settings.name == job_name:
                if job.job_id is not None:
                    return job.job_id
        raise Exception(f"Job '{job_name}' not found.")

    def get_latest_job_run_id(self, job_id: int, active_only: bool = True) -> int | None:
        """Get the latest run ID for a Databricks job.

        Args:
            job_id (int): The ID of the job.

        Returns:
            int: The run ID of the job.
        """
        runs = self.client.jobs.list_runs(job_id=job_id, active_only=active_only)
        run = next(runs, None)

        if run is None:
            return None
        else:
            return run.run_id

    def start_job(self, job_id: int, python_params: list[str]) -> int:
        """Start a Databricks job.

        Args:
            job_id (int): The ID of the job.
            python_params (list[str]): The parameters to pass to the job.

        Returns:
            int: The run ID of the job.
        """
        response = self.client.jobs.run_now(job_id=job_id, python_params=python_params)
        return response.run_id

    def cancel_job_run(self, job_run_id: int, wait_for_cancellation: bool = True) -> None:
        """Stop a Databricks job.

        Args:
            job_run_id (int): The ID of the job run.
            wait_for_cancellation (bool, optional): Whether to wait for the job to be canceled. Defaults to True.
        """
        self.client.jobs.cancel_run(run_id=job_run_id)
        if wait_for_cancellation:
            self.wait_for_job_state(run_id=job_run_id, target_states=[RunLifeCycleState.TERMINATED.value])

    def wait_for_job_completion(self, run_id: int, timeout: int = 1000, poll_interval: int = 10) -> RunResultState:
        """Wait for a Databricks job to complete.

        Args:
            run_id (int): The run ID of the job.
            timeout (int, optional): The maximum time to wait for the job to complete. Defaults to 1000.
            poll_interval (int, optional): The interval between polling the job status. Defaults to 10.

        Returns:
            RunResultState: The result state of the job.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            run_status = self.client.jobs.get_run(run_id=run_id)
            if run_status.state is None:
                raise Exception("Job run status state is None")

            if run_status.state.life_cycle_state is None:
                raise Exception("Job run lifecycle state is None")
            lifecycle_state = run_status.state.life_cycle_state.value

            result_state = None
            if run_status.state.result_state is not None:
                result_state = run_status.state.result_state.value

            if lifecycle_state == "TERMINATED":
                if result_state is None:
                    raise Exception("Job terminated but result state is None")
                return RunResultState(result_state)
            elif lifecycle_state == "INTERNAL_ERROR":
                raise Exception(f"Job failed with an internal error: {run_status.state.state_message}")

            time.sleep(poll_interval)

        raise TimeoutError(f"Job did not complete within {timeout} seconds.")

    def execute_statement(
        self,
        warehouse_id: str,
        statement: str,
    ) -> "DatabricksApiClient":
        """Execute a SQL statement.

        Args:
            warehouse_id (str): Databricks warehouse/cluster ID
            statement (str): SQL statement to execute

        Returns:
            DatabricksApiClient: Self reference for method chaining
        """
        try:
            response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
            )

            if response.status.state == "FAILED":
                raise Exception(f"Statement execution failed: {response.status.error}")

            return self

        except Exception as e:
            raise Exception(f"Failed to execute statement: {str(e)}")

    def wait_for_job_state(
        self, run_id: int, target_states: list[str], timeout: int = 1000, poll_interval: int = 10
    ) -> None:
        """Wait for a Databricks job to reach one of the target states.

        Args:
            run_id (int): The run ID of the job.
            target_states (list[str]): The target states to wait for.
            timeout (int, optional): The maximum time to wait for the job to reach the target state. Defaults to 1000.
            poll_interval (int, optional): The interval between polling the job status. Defaults to 10.

        Raises:
            TimeoutError: If the job does not reach the target state within the timeout period.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            run_status = self.client.jobs.get_run(run_id=run_id)
            if run_status.state is None:
                raise Exception("Job run status state is None")

            if run_status.state.life_cycle_state is None:
                raise Exception("Job run lifecycle state is None")
            lifecycle_state = run_status.state.life_cycle_state.value

            if lifecycle_state in target_states:
                return

            time.sleep(poll_interval)

        raise TimeoutError(f"Job did not reach the target state(s) {target_states} within {timeout} seconds.")
