import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import Wait
from databricks.sdk.service.jobs import BaseRun, Run, RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import Disposition, StatementResponse, WaitTimeoutResponse

from geh_common.telemetry.logger import Logger

logger = Logger(__name__)


class DatabricksApiClient:
    def __init__(self, databricks_token: str, databricks_host: str) -> None:
        self.client = WorkspaceClient(host=databricks_host, token=databricks_token)

    def get_job_id(self, job_name: str) -> int | None:
        """Get the job id form a Databricks job name.

        Args:
            job_name (str): The name of the job.

        Returns:
            job_id (int): the id of the job.
        """
        jobs = list(self.client.jobs.list(name=job_name))
        if len(jobs) == 0:
            raise ValueError(f"No job found with name {job_name}.")
        if len(jobs) > 1:
            raise ValueError(f"Multiple jobs found with name {job_name}.")
        return jobs[0].job_id

    def get_latest_job_run(self, job_id: int, active_only: bool = True) -> BaseRun | None:
        """Get the latest run for a Databricks job.

        Args:
            job_id (int): The ID of the job.
            active_only (bool): If active_only is `true`, only active runs are included in the results

        Returns:
            BaseRun: The run object of the latest job.
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
            Wait[Run]: the object of the running job you just started.
        """
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
        timeout_seconds: int = 600,
        disposition=Disposition.INLINE,
<<<<<<< HEAD
        on_wait_timeout=WaitTimeoutResponse.CANCEL,
=======
        on_wait_timeout="CANCEL",
>>>>>>> main
    ) -> StatementResponse:
        """Execute a SQL statement. Only supports small result set (<= 25 MiB).

        Args:
            warehouse_id (str): The ID of the Databricks warehouse or cluster.
            statement (str): The SQL statement to execute.
<<<<<<< HEAD
            on_wait_timeout (WaitTimeoutResponse, optional): What to do when the timeout period has been met. Defaults to CANCEL.
=======
            on_wait_timeout (str, optional): What to do when the timeout period has been met. Defaults to CANCEL.
>>>>>>> main
            timeout_seconds (int, optional): Maximum wait time in seconds when waiting for a response. Defaults to 10.
            disposition (Disposition): Mode of result retrieval. Currently supports only Disposition.INLINE.

        Returns:
            StatementResponse: A StatementResponse object. It may optionally contain a `statement_id`, `status`,
            `manifest` (object that provides the schema and metadata of the result set), and a `result`
            (object containing the result data). Notice, the result data currently only supports the INLINE mode.

        """
        if disposition != Disposition.INLINE:
            raise NotImplementedError("Execute statement only supports disposition INLINE")

        return self.client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            on_wait_timeout=on_wait_timeout,
            wait_timeout=timeout_seconds,
        )

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
