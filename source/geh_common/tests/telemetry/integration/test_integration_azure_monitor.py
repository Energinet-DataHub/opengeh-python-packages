import logging
import sys
import time
import uuid
from datetime import timedelta
from typing import Callable, cast
from unittest.mock import patch

import pytest
from azure.monitor.query import LogsQueryClient, LogsQueryPartialResult, LogsQueryResult

from geh_common.telemetry.decorators import start_trace, use_span
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import configure_logging, start_span
from tests.telemetry.conftest import cleanup_logging
from tests.telemetry.integration.integration_test_configuration import (
    IntegrationTestConfiguration,
)

INTEGRATION_TEST_LOGGER_NAME = "test-logger"
INTEGRATION_TEST_CLOUD_ROLE_NAME = "integration-test-python-packages"
SUBSYSTEM = "integration_test_subsystem"
LOOK_BACK_FOR_QUERY = timedelta(minutes=5)


@pytest.fixture
def fixture_logger():
    yield Logger(INTEGRATION_TEST_LOGGER_NAME)


@pytest.fixture
def integration_logging_configuration_setup(integration_test_configuration):
    new_uuid = uuid.uuid4()
    sys_argv = ["dummy_script_name", "--orchestration-instance-id", str(new_uuid)]
    unique_cloud_role_name = INTEGRATION_TEST_CLOUD_ROLE_NAME + "_" + str(new_uuid)
    with patch("sys.argv", sys_argv):
        with pytest.MonkeyPatch.context() as ctx:
            ctx.setenv(
                "APPLICATIONINSIGHTS_CONNECTION_STRING",
                integration_test_configuration.get_applicationinsights_connection_string(),
            )
            # Remove any previously attached log handlers. Without it, handlers from previous tests can accumulate, causing multiple log messages for each event.
            logging.getLogger().handlers.clear()
            yield configure_logging(subsystem=SUBSYSTEM, cloud_role_name=unique_cloud_role_name)
            cleanup_logging()


@pytest.fixture()
def integration_logging_configuration_setup_with_extras(integration_test_configuration):
    key = "key"
    extras = {key: "value"}
    new_uuid = uuid.uuid4()

    sys_argv = ["dummy_script_name", "--orchestration-instance-id", str(new_uuid)]
    unique_cloud_role_name = INTEGRATION_TEST_CLOUD_ROLE_NAME + "_" + str(new_uuid)
    with patch("sys.argv", sys_argv):
        with pytest.MonkeyPatch.context() as ctx:
            ctx.setenv(
                "APPLICATIONINSIGHTS_CONNECTION_STRING",
                integration_test_configuration.get_applicationinsights_connection_string(),
            )
    # Remove any previously attached log handlers. Without it, handlers from previous tests can accumulate, causing multiple log messages for each event.
    logging.getLogger().handlers.clear()
    yield (
        configure_logging(cloud_role_name=unique_cloud_role_name, subsystem=SUBSYSTEM, extras=extras),
        extras,
    )  # 2nd par beforelogging_settings, extras
    cleanup_logging()


def _assert_row_count(actual: LogsQueryResult | LogsQueryPartialResult, expected_count: int) -> None:
    actual = cast(LogsQueryResult, actual)
    table = actual.tables[0]
    row = table.rows[0]
    value = row["Count"]
    count = cast(int, value)
    assert count == expected_count


def _assert_logged(logs_client: LogsQueryClient, workspace_id: str, query: str, expected_count: int) -> None:
    actual = logs_client.query_workspace(workspace_id, query, timespan=LOOK_BACK_FOR_QUERY)
    _assert_row_count(actual, expected_count)


def _wait_for_condition(
    logs_client: LogsQueryClient,
    workspace_id: str,
    query: str,
    expected_count: int,
    timeout: timedelta = timedelta(minutes=5),
    step: timedelta = timedelta(seconds=10),
) -> None:
    """
    Wait for a condition to be met, or timeout.
    The function keeps invoking the callback until it returns without raising an exception.
    """
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
            print(f"Condition met in {elapsed_ms} ms")  # noqa
            return
        except Exception:
            if elapsed_ms > timeout.total_seconds() * 1000:
                print(  # noqa
                    f"Condition failed to be met before timeout. Timed out after {elapsed_ms} ms",
                    file=sys.stderr,
                )
                raise
            time.sleep(step.seconds)
            print(f"Condition not met after {elapsed_ms} ms. Retrying...")  # noqa


def test__exception_adds_log_to_app_exceptions(
    integration_test_configuration: IntegrationTestConfiguration,
    integration_logging_configuration_setup,
) -> None:
    _, logging_settings_from_fixture = integration_logging_configuration_setup
    new_uuid = uuid.uuid4()
    message = f"test exception {new_uuid}"
    cloud_role_name = logging_settings_from_fixture.cloud_role_name

    # Act
    with start_span(__name__) as span:
        try:
            raise ValueError(message)
        except ValueError as e:
            span.record_exception(e)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        AppExceptions
        | where AppRoleName == "{cloud_role_name}"
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
    logging_level: Callable[[Logger, str], None],
    severity_level: int,
    integration_test_configuration: IntegrationTestConfiguration,
    integration_logging_configuration_setup_with_extras,
    fixture_logger,
) -> None:
    _, logging_settings_from_fixture, extras_from_fixture = integration_logging_configuration_setup_with_extras
    logger = fixture_logger
    # Arrange
    new_uuid = uuid.uuid4()
    message = f"test message {new_uuid}"
    cloud_role_name = logging_settings_from_fixture.cloud_role_name

    extras = extras_from_fixture
    key = list(extras.keys())[0]  # Get the keyname of the extras dict

    # Act
    logging_level(logger, message)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        AppTraces
        | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
        | where AppRoleName == "{cloud_role_name}"
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
        step=timedelta(seconds=10),
    )


def test__add_log_records_to_azure_monitor_keeps_correct_count(
    integration_test_configuration: IntegrationTestConfiguration,
    integration_logging_configuration_setup_with_extras,
    fixture_logger,
) -> None:
    _, logging_settings_from_fixture, _ = integration_logging_configuration_setup_with_extras
    logger = fixture_logger
    # Arrange
    log_count = 5
    new_uuid = uuid.uuid4()
    message = f"test message {new_uuid}"
    cloud_role_name = logging_settings_from_fixture.cloud_role_name

    # Act
    for _ in range(log_count):
        logger.info(message)

    # Assert
    # noinspection PyTypeChecker
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query = f"""
        AppTraces
        | where Properties.CategoryName == "Energinet.DataHub.{INTEGRATION_TEST_LOGGER_NAME}"
        | where AppRoleName == "{cloud_role_name}"
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
    integration_logging_configuration_setup_with_extras,
    fixture_logger,
) -> None:
    # Arrange
    new_uuid = uuid.uuid4()
    _, logging_settings_from_fixture, _ = integration_logging_configuration_setup_with_extras
    logger = fixture_logger
    cloud_role_name = logging_settings_from_fixture.cloud_role_name

    # Use the start_trace to start the trace based on new_settings.cloud_role_name, and start the first span,
    # taking the name of the function using the decorator @start_trace: app_sample_function

    decorator_message_start_trace = "Started executing function: app_sample_function"
    test_message_start_trace = f"test message app_sample_function {new_uuid}"

    decorator_message_use_span = (
        "Started executing function: test__decorators_integration_test.<locals>.app_sample_subfunction"
    )
    test_message_use_span = f"test message app_sample_subfunction {new_uuid}"

    @start_trace()
    def app_sample_function():
        log_message = test_message_start_trace
        logger.info(log_message)
        app_sample_subfunction()

    @use_span()
    def app_sample_subfunction():
        log_message = test_message_use_span
        logger.info(log_message)

    # Act
    app_sample_function()

    # Assert
    logs_client = LogsQueryClient(integration_test_configuration.credential)

    query_start_trace_decorator_message = f"""
            AppTraces
            | where AppRoleName == "{cloud_role_name}"
            | where Message == "{decorator_message_start_trace}"
            | count
            """

    query_start_trace_test_message = f"""
            AppTraces
            | where AppRoleName == "{cloud_role_name}"
            | where Message == "{test_message_start_trace}"
            | count
            """

    query_use_span_decorator_message = f"""
                AppTraces
                | where AppRoleName == "{cloud_role_name}"
                | where Message == "{decorator_message_use_span}"
                | count
                """

    query_use_span_test_message = f"""
                AppTraces
                | where AppRoleName == "{cloud_role_name}"
                | where Message == "{test_message_use_span}"
                | count
                """

    # Assert that we can query the specific logs created in the context of the spans
    workspace_id = integration_test_configuration.get_analytics_workspace_id()

    # Assert, but timeout if not succeeded
    # wait_for_condition only needed for the first query as the delay of the logs query client will be over when the first assert succeeds
    _wait_for_condition(
        logs_client=logs_client,
        workspace_id=workspace_id,
        query=query_start_trace_decorator_message,
        expected_count=1,
    )
    _assert_logged(
        logs_client=logs_client, workspace_id=workspace_id, query=query_start_trace_test_message, expected_count=1
    )
    _assert_logged(
        logs_client=logs_client, workspace_id=workspace_id, query=query_use_span_decorator_message, expected_count=1
    )
    _assert_logged(
        logs_client=logs_client, workspace_id=workspace_id, query=query_use_span_test_message, expected_count=1
    )
