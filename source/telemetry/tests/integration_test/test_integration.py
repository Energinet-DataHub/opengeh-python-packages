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
from datetime import timedelta
from typing import cast, Callable
from azure.monitor.query import LogsQueryClient, LogsQueryResult

from integration_test_configuration import IntegrationTestConfiguration
from telemetry_logging.logger import Logger
from telemetry_logging.logging_configuration import configure_logging

def _wait_for_condition(callback: Callable, *, timeout: timedelta, step: timedelta):
    """
    Wait for a condition to be met, or timeout.
    The function keeps invoking the callback until it returns without raising an exception.
    """
    start_time = time.time()
    while True:
        elapsed_ms = int((time.time() - start_time) * 1000)
        # noinspection PyBroadException
        try:
            callback()
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


def _assert_row_count(actual, expected_count):
    actual = cast(LogsQueryResult, actual)
    table = actual.tables[0]
    row = table.rows[0]
    value = row["Count"]
    count = cast(int, value)
    assert count == expected_count

def _assert_logged(logs_client: LogsQueryClient, workspace_id: str, query: str):
    actual = logs_client.query_workspace(
        workspace_id, query, timespan=timedelta(minutes=5)
    )
    _assert_row_count(actual, 1)


def test_add_info_log_record_to_azure_monitor_with_expected_settings(
        integration_test_configuration: IntegrationTestConfiguration,
    ) -> None:
        # Arrange
        message = "test message"
        logger_name = "test-logger"
        cloud_name = "test-cloud-role-name"
        tracer_name = "test-tracer-name"
        extras = {"test-key": "test-value"}
        applicationinsights_connection_string = (
            integration_test_configuration.get_applicationinsights_connection_string()
        )

        configure_logging(
            cloud_role_name=cloud_name,
            tracer_name=tracer_name,
            applicationinsights_connection_string=applicationinsights_connection_string,
            extras=extras,
        )
        logger = Logger(logger_name)

        # Act
        logger.info(message)

        # Assert
        # noinspection PyTypeChecker
        logs_client = LogsQueryClient(integration_test_configuration.credential)

        query = f"""
        AppTraces
        | where AppRoleName == "{cloud_name}"
        | count
        """

        workspace_id = integration_test_configuration.get_analytics_workspace_id()


        # Assert, but timeout if not succeeded
        _wait_for_condition(
            _assert_logged(logs_client, workspace_id, query), timeout=timedelta(minutes=3), step=timedelta(seconds=10)
        )
