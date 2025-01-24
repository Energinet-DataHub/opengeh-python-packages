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
import os
import pytest
from datetime import timedelta
from typing import cast, Callable
from azure.monitor.query import LogsQueryClient, LogsQueryResult
from opentelemetry.trace import SpanKind

from tests.integration_test_configuration import IntegrationTestConfiguration
from telemetry_logging.logger import Logger
import telemetry_logging.logging_configuration as config
from telemetry_logging.decorators import start_trace, use_span


INTEGRATION_TEST_LOGGER_NAME = "test-logger"
INTEGRATION_TEST_CLOUD_ROLE_NAME = "test-cloud-role-name"
INTEGRATION_TEST_TRACER_NAME = "test-tracer-name"
LOOK_BACK_FOR_QUERY = timedelta(minutes=5)

@pytest.fixture()
def fixture_logging_settings(integration_test_configuration):
    # Get the application insights string, which can be retrieved from the integration_test_configuration fixture
    applicationinsights_connection_string = (
        integration_test_configuration.get_applicationinsights_connection_string()
    )
    os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = applicationinsights_connection_string
    os.environ['CLOUD_ROLE_NAME'] = INTEGRATION_TEST_CLOUD_ROLE_NAME
    os.environ['SUBSYSTEM'] = INTEGRATION_TEST_TRACER_NAME
    os.environ['ORCHESTRATION_INSTANCE_ID'] = str(uuid.uuid4())
    logging_settings = config.LoggingSettings()
    return logging_settings

@pytest.fixture()
def fixture_extras():
    return {"key": "value"}

def _wait_for_condition(
    logs_client: LogsQueryClient,
    workspace_id: str,
    query: str,
    expected_count: int,
    timeout: timedelta = timedelta(minutes=2),
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


def test__exception_adds_log_to_app_exceptions(
    integration_test_configuration: IntegrationTestConfiguration,
    fixture_logging_settings, fixture_extras
) -> None:
    # Arrange
    logging_settings = fixture_logging_settings
    logging_settings.force_configuration = True
    extras = fixture_extras
    new_uuid = uuid.uuid4()
    message = f"test exception {new_uuid}"

    config.configure_logging(logging_settings=logging_settings, extras=extras)

    # Act
    with config.start_span(__name__) as span:
        try:
            raise ValueError(message)
        except ValueError as e:
            span.record_exception(e)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        AppExceptions
        | where AppRoleName == "{INTEGRATION_TEST_CLOUD_ROLE_NAME}"
        | where ExceptionType == "ValueError"
        | where OuterMessage == "{message}"
        | count
        """

    workspace_id = integration_test_configuration.get_analytics_workspace_id()

    # Assert, but timeout if not succeeded
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query,
        expected_count=1,
    )


@pytest.mark.parametrize(
    "logging_level, severity_level",
    [
        (Logger.info, 1),
        (Logger.warning, 2),
        (Logger.error, 3),
    ],
)
def test__add_log_record_to_azure_monitor_with_expected_settings(
    logging_level: Callable[[str], None],
    severity_level: int,
    integration_test_configuration: IntegrationTestConfiguration,
    fixture_logging_settings
) -> None:
    # Arrange
    new_uuid = uuid.uuid4()
    new_unique_cloud_role_name = f"{INTEGRATION_TEST_CLOUD_ROLE_NAME}-{new_uuid}"
    message = "test message"
    key = "key"
    extras = {key: "value"}
    new_settings = fixture_logging_settings
    new_settings.cloud_role_name = new_unique_cloud_role_name
    new_settings.force_configuration = True

    config.configure_logging(logging_settings=new_settings, extras=extras)
    logger = Logger(INTEGRATION_TEST_LOGGER_NAME)

    # Act
    logging_level(logger, message)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        AppTraces
        | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
        | where AppRoleName == "{new_unique_cloud_role_name}"
        | where Message == "{message}"
        | where Properties.{key} == "{extras[key]}"
        | where SeverityLevel == {severity_level}
        | count
        """

    workspace_id = integration_test_configuration.get_analytics_workspace_id()

    # Assert, but timeout if not succeeded
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query,
        expected_count=1,
        step=timedelta(seconds=10)
    )


def test__add_log_records_to_azure_monitor_keeps_correct_count(
    integration_test_configuration: IntegrationTestConfiguration,
        fixture_logging_settings,
        fixture_extras
) -> None:
    # Arrange
    log_count = 5
    new_uuid = uuid.uuid4()
    new_unique_cloud_role_name = f"{INTEGRATION_TEST_CLOUD_ROLE_NAME}-{new_uuid}"
    message = "test message"
    new_settings = fixture_logging_settings
    new_settings.cloud_role_name = new_unique_cloud_role_name
    new_settings.force_configuration = True
    extras = fixture_extras

    config.configure_logging(logging_settings=new_settings, extras=extras)

    logger = Logger(INTEGRATION_TEST_LOGGER_NAME)

    # Act
    for _ in range(log_count):
        logger.info(message)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        AppTraces
        | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
        | where AppRoleName == "{new_unique_cloud_role_name}"
        | where Message == "{message}"
        | count
        """

    workspace_id = integration_test_configuration.get_analytics_workspace_id()

    # Assert, but timeout if not succeeded
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query,
        expected_count=log_count,
    )


def test__decorators_integration_test(
    integration_test_configuration: IntegrationTestConfiguration,
        fixture_logging_settings,
        fixture_extras
) -> None:
    # Arrange
    new_uuid = uuid.uuid4()
    new_unique_cloud_role_name = f"{INTEGRATION_TEST_CLOUD_ROLE_NAME}-{new_uuid}"

    new_settings = fixture_logging_settings
    new_settings.cloud_role_name = new_unique_cloud_role_name
    new_settings.force_configuration = True
    extras = fixture_extras

    # Configuring logging, setting the name of the trace based on new_settings.cloud_role_name
    config.configure_logging(logging_settings=new_settings, extras=extras)
    logger = Logger(INTEGRATION_TEST_LOGGER_NAME)

    # Use the start_trace to start the trace based on new_settings.cloud_role_name, and start the first span,
    # taking the name of the function using the decorator @start_trace: app_sample_function

    test_message_start_trace = "test message app_sample_function"
    test_message_use_span = "test message app_sample_subfunction"

    @start_trace
    def app_sample_function(initial_span=None):
        assert (1 + 1) == 2
        log_message = test_message_start_trace
        logger.info(log_message)
        app_sample_subfunction()

    @use_span()
    def app_sample_subfunction():
        assert (2 + 2) == 4
        log_message = test_message_use_span
        logger.info(log_message)

    # Act
    app_sample_function()

    # Assert
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query_start_trace = f"""
            AppTraces
            | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
            | where AppRoleName == "{new_unique_cloud_role_name}"
            | where Message == "{test_message_start_trace}"
            | count
            """

    query_use_span = f"""
                AppTraces
                | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
                | where AppRoleName == "{new_unique_cloud_role_name}"
                | where Message == "{test_message_use_span}"
                | count
                """

    # Assert that we can query the specific logs created in the context of the spans
    workspace_id = integration_test_configuration.get_analytics_workspace_id()

    # Assert, but timeout if not succeeded
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query_start_trace,
        expected_count=1,
    )

    # Assert, but timeout if not succeeded
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query_use_span,
        expected_count=1,
    )
