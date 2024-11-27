# Copyright 2020 Energinet DataHub A/S
#
# Licensed under the Apache License, Version 2.0 (the "License2");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time
import sys
import uuid
import pytest
from datetime import timedelta
from typing import cast, Callable
from azure.monitor.query import LogsQueryClient, LogsQueryResult

from tests.integration_test_configuration import IntegrationTestConfiguration
from telemetry_logging.logger import Logger
from telemetry_logging.logging_configuration import configure_logging


INTEGRATION_TEST_LOGGER_NAME = "test-logger"
INTEGRATION_TEST_CLOUD_ROLE_NAME = "test-cloud-role-name"
INTEGRATION_TEST_TRACER_NAME = "test-tracer-name"
LOOK_BACK_FOR_QUERY = timedelta(minutes=5)


def _wait_for_condition(
    logs_client: LogsQueryClient,
    workspace_id: str,
    query: str,
    expected_count: int,
    timeout: timedelta = timedelta(minutes=3),
    step: timedelta = timedelta(seconds=10),
) -> None:
    """
    Wait for a condition to be met, or timeout.
    The function keeps invoking the callback until it returns without raising an exception.
    """

    def _assert_row_count(actual: int, expected_count: int) -> None:
        actual = cast(LogsQueryResult, actual)
        table = actual.tables[0]
        row = table.rows[0]
        value = row["Count"]
        count = cast(int, value)
        assert count == expected_count

    def _assert_logged(
        logs_client: LogsQueryClient, workspace_id: str, query: str, expected_count: int
    ) -> None:
        actual = logs_client.query_workspace(
            workspace_id, query, timespan=LOOK_BACK_FOR_QUERY
        )
        _assert_row_count(actual, expected_count)

    start_time = time.time()
    while True:
        elapsed_ms = int((time.time() - start_time) * 1000)
        # noinspection PyBroadException
        try:
            _assert_logged(
                logs_client=logs_client,
                workspace_id=workspace_id,
                query=query,
                expected_count=expected_count,
            )
            print(f"Condition met in {elapsed_ms} ms")
            return
        except Exception:
            if elapsed_ms > timeout.total_seconds() * 1000:
                print(
                    f"Condition failed to be met before timeout. Timed out after {elapsed_ms} ms",
                    file=sys.stderr,
                )
                raise
            time.sleep(step.seconds)
            print(f"Condition not met after {elapsed_ms} ms. Retrying...")


@pytest.mark.parametrize(
    "logging_level, azure_log_name",
    [
        (Logger.info, "AppTraces"),
        (Logger.warning, "AppTraces"),
        (Logger.error, "AppTraces"),
    ],
)
def test_add_log_record_to_azure_monitor_with_expected_settings(
    logging_level: Callable[[str], None],
    azure_log_name: str,
    integration_test_configuration: IntegrationTestConfiguration,
) -> None:
    # Arrange
    new_uuid = uuid.uuid4()
    new_unique_cloud_role_name = f"{INTEGRATION_TEST_CLOUD_ROLE_NAME}-{new_uuid}"
    message = "test message"
    extras = {"test-key": "test-value"}
    applicationinsights_connection_string = (
        integration_test_configuration.get_applicationinsights_connection_string()
    )

    configure_logging(
        cloud_role_name=new_unique_cloud_role_name,
        tracer_name=INTEGRATION_TEST_TRACER_NAME,
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras=extras,
    )
    logger = Logger(INTEGRATION_TEST_LOGGER_NAME)

    # Act
    logging_level(logger, message)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        {azure_log_name}
        | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
        | where AppRoleName == "{new_unique_cloud_role_name}"
        | where message == "{message}"
        | where test-key == "{extras['test-key']}"
        | count
        """

    print(query)

    workspace_id = integration_test_configuration.get_analytics_workspace_id()

    # Assert, but timeout if not succeeded
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query,
        expected_count=1,
    )
