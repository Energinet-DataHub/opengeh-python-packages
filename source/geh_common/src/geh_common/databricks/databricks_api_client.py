import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import Wait
from databricks.sdk.service.jobs import BaseRun, Run, RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import Disposition, StatementResponse, StatementState


class DatabricksApiClient:
    def __init__(self, databricks_token: str, databricks_host: str) -> None:
        self.client = WorkspaceClient(host=databricks_host, token=databricks_token)

    def get_job_id(self, job_name: str) -> int | None:
        """Get the job object form a Databricks job name.

        Args:
            job_name (str): The name of the job.

        Returns:
            BaseJob: The job object
        """
        jobs = list(self.client.jobs.list(name=job_name))
        if len(jobs) == 0:
            raise ValueError(f"No job found with name {job_name}.")
        if len(jobs) > 1:
            raise ValueError(f"Multiple jobs found with name {job_name}.")
        return jobs[0].job_id

    def get_latest_job_run_id(self, job_id: int, active_only: bool = True) -> BaseRun | None:
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
            return run

    def start_job(self, job_id: int, python_params: list[str] | None = None) -> Wait[Run]:
        """Start a Databricks job.

        Args:
            job_id (int): The ID of the job.
            python_params (list[str]): The parameters to pass to the job.

        Returns:
            int: The run ID of the job.
        """
        # change job_id to job object
        response = self.client.jobs.run_now(job_id=job_id, python_params=python_params)
        return response

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
            job Wait[Run]: the object of an active.
            timeout (int, optional): The maximum time to wait for the job to complete. Defaults to 1000.
            poll_interval (int, optional): The interval between polling the job status. Defaults to 10.

        Returns:
            RunResultState: The result state of the job.
        """
        # run = job.result(timeout=timedelta(minutes=timeout))
        # return run.state.result_state
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

    def get_statement(
        self,
        statement_id: str,
    ) -> StatementResponse:
        """Retrieve the result of a previously executed statement.

        Args:
            statement_id (str): The ID of the statement to retrieve.

        Returns:
            StatementResponse: The response object containing status and results.
        """
        return self.client.statement_execution.get_statement(statement_id)

    def execute_statement(
        self,
        warehouse_id: str,
        statement: str,
        timeout_seconds: int = 600,
        poll_interval_seconds: int = 5,
        disposition=Disposition.INLINE,
    ) -> StatementResponse:
        """Execute a SQL statement. Only supports small result set (<= 25 MiB).

        Args:
            warehouse_id (str): The ID of the Databricks warehouse or cluster.
            statement (str): The SQL statement to execute.
            wait_for_response (bool, optional): Whether to wait for the execution result. Defaults to True.
            timeout_seconds (int, optional): Maximum wait time in seconds when waiting for a response. Defaults to 10.
            poll_interval_seconds (int): time between recalls to databricks to get the state of the statement.
            disposition (Disposition): Mode of result retrieval. Currently supports only Disposition.INLINE.

        Returns:
            StatementResponse: A StatementResponse object. It may optionally contain a `statement_id`, `status`,
            `manifest` (object that provides the schema and metadata of the result set), and a `result`
            (object containing the result data). Notice, the result data currently only supports the INLINE mode.

        Raises:
            NotImplementedError: If a non-INLINE disposition is specified.
            TimeoutError: If the statement execution exceeds the timeout limit.
            Exception: For any execution failure.
        """
        response = self.client.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement)

        if disposition != Disposition.INLINE:
            raise NotImplementedError("Execute statement only supports disposition INLINE")

        # Wait for the statement to complete
        start_time = time.time()
        elapsed_time = 0
        while elapsed_time < timeout_seconds:
            response = self.client.statement_execution.get_statement(response.statement_id)
            if response.status.state not in [StatementState.RUNNING, StatementState.PENDING, StatementState.SUCCEEDED]:
                raise ValueError(
                    f"Statement execution failed with state {response.status.state} and error {response.status.error}"
                )
            if response.status.state == StatementState.SUCCEEDED:
                return response
            elapsed_time = time.time() - start_time
            print(f"Query did not complete in {elapsed_time} seconds. Retrying in {poll_interval_seconds} seconds...")  # noqa: T201
            time.sleep(poll_interval_seconds)
        raise TimeoutError(f"Statement execution timed out after {timeout_seconds} seconds.")

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
