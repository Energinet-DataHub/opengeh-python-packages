from unittest import mock
from unittest.mock import patch

import pytest

from geh_common.telemetry.decorators import start_trace, use_span
from geh_common.telemetry.logging_configuration import (
    LoggingSettings,
    configure_logging,
)


# Mocking the Logger and start_span
@pytest.fixture
def mock_logger():
    with patch("geh_common.telemetry.decorators.Logger") as MockLogger:
        yield MockLogger


@pytest.fixture
def mock_start_span():
    with patch("geh_common.telemetry.decorators.start_span") as MockStartSpan:
        yield MockStartSpan


@pytest.fixture
def mock_env_args():
    env_args = {
        "CLOUD_ROLE_NAME": "cloud_role_name from environment",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "subsystem from environment",
    }
    yield env_args


def test_use_span__when_name_is_defined(mock_logger, mock_start_span):
    # Arrange
    mock_logger_instance = mock_logger.return_value

    @use_span(name="test_span")
    def sample_function():
        return "test"

    # Act
    result = sample_function()

    # Assert
    mock_start_span.assert_called_once_with("test_span")
    mock_logger.assert_called_once_with("test_span")
    mock_logger_instance.info.assert_called_once_with("Started executing function: test_span")
    assert result == "test"


def test_use_span__when_name_is_not_defined(mock_logger, mock_start_span):
    # Arrange
    mock_logger_instance = mock_logger.return_value

    @use_span()
    def sample_function():
        return "test"

    # Act
    result = sample_function()

    # Assert
    mock_start_span.assert_called_once_with("test_use_span__when_name_is_not_defined.<locals>.sample_function")
    mock_logger.assert_called_once_with("test_use_span__when_name_is_not_defined.<locals>.sample_function")
    mock_logger_instance.info.assert_called_once_with(
        "Started executing function: test_use_span__when_name_is_not_defined.<locals>.sample_function"
    )
    assert result == "test"


def test_start_trace__when_logging_not_configured():
    # Prepare
    @start_trace()
    def app_sample_function():
        assert (1 + 1) == 2
        return "I am an app sample function. Doing important calculations"

    def entry_point():
        app_sample_function()

    # Act and assert
    with pytest.raises(Exception):
        entry_point()


def test_start_trace__when_logging_is_configured(mock_env_args):
    with (
        patch("geh_common.telemetry.decorators.Logger") as mock_logger,
        patch("geh_common.telemetry.logging_configuration.configure_azure_monitor") as mock_configure_azure_monitor,
    ):  # Intercepts Logger(func.__name__)
        log_instance_in_test = mock_logger.return_value  # Intercepts log = Logger(func.__name__)

        # Prepare
        @start_trace()
        def app_sample_function(initial_span=None):
            assert (1 + 1) == 2
            return "I am an app sample function. Doing important calculations"

        def entry_point():
            # Initial LoggingSettings
            settings = LoggingSettings()
            settings.applicationinsights_connection_string = "connection_string"  # For testing purposes

            configure_logging(
                logging_settings=settings,
                extras={"key1": "value1", "key2": "value2"},
            )
            app_sample_function()

        with mock.patch.dict("os.environ", mock_env_args, clear=False):
            # Act
            entry_point()
            # Assert
            mock_logger.assert_called_once_with("app_sample_function")
            log_instance_in_test.info.assert_called_once_with("Started executing function: app_sample_function")


@patch("geh_common.telemetry.decorators.Logger")
@patch("geh_common.telemetry.decorators.span_record_exception")
def test_logging_is_configured_error_thrown_span_records_exception(
    mock_span_record_exception,
    mock_logger,
    mock_env_args,
):
    log_instance_in_test = mock_logger.return_value

    # Prepare
    @start_trace()
    def app_sample_function():
        assert (1 + 1) == 2
        raise Exception  # Mimmic an raised exception during runtime
        return "I am an app sample function. Doing important calculations"

    def entry_point():
        # Initial LoggingSettings
        settings = LoggingSettings()
        settings.applicationinsights_connection_string = "connection_string"  # For testing purposes

        configure_logging(
            logging_settings=settings,
            extras={"key1": "value1", "key2": "value2"},
        )
        app_sample_function()

    # Mimic machine setting environment variables
    with mock.patch.dict("os.environ", mock_env_args, clear=False):
        with pytest.raises(SystemExit):
            entry_point()
            # Assert
            mock_logger.assert_called_once_with("app_sample_function")
            log_instance_in_test.info.assert_called_once_with("Started executing function: app_sample_function")
            mock_span_record_exception.assert_called_once()
