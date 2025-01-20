import pytest
import os
from unittest.mock import patch, MagicMock
from telemetry_logging.decorators import use_span, start_trace
from telemetry_logging.logging_configuration import configure_logging, LoggingSettings


# Mocking the Logger and start_span
@pytest.fixture
def mock_logger():
    with patch('telemetry_logging.decorators.Logger') as MockLogger:
        yield MockLogger


@pytest.fixture
def mock_start_span():
    with patch('telemetry_logging.decorators.start_span') as MockStartSpan:
        yield MockStartSpan


@pytest.fixture
def mock_start_trace():
    with patch('telemetry_logging.decorators.start_trace') as MockStartTrace:
        yield MockStartTrace

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
    mock_logger_instance.info.assert_called_once_with("Started executing function: test_use_span__when_name_is_not_defined.<locals>.sample_function")
    print(mock_logger.call_args_list)
    assert result == "test"


def test_start_trace__when_logging_not_configured(mock_logger, mock_start_trace):
    mock_logger_instance = mock_logger.return_value
    mock_start_trace_instance = mock_start_trace.return_value

    # Prepare
    @start_trace
    def app_sample_function(initial_span=None):
        assert (1 + 1) == 2
        return "I am an app sample function. Doing important calculations"

    def entry_point():
        print("I am an entry point function, who is supposed to configure logging - but I don't in this case")
        app_sample_function()

    # Act and assert
    with pytest.raises(NotImplementedError):
        entry_point()


# def test_start_trace__when_logging_is_configured():
#     with patch('telemetry_logging.decorators.Logger') as mock_logger2:
#         mock_logger_instance = mock_logger2.return_value
#
#         # Prepare
#         @start_trace
#         def app_sample_function(initial_span=None):
#             assert (1 + 1) == 2
#             return "I am an app sample function. Doing important calculations"
#
#         def entry_point() -> None:
#             print("I am an entry point function, who is supposed to configure logging - which I do here")
#             configure_logging(
#                 cloud_role_name="test_cloud_role_name",
#                 tracer_name="subsystem_tracer_name",
#                 applicationinsights_connection_string=None,
#                 extras={'key1': 'value1', 'key2': 'value2'},
#             )
#             app_sample_function()
#
#         # Act
#         print("SEE MEEEE")
#         print(mock_logger_instance.call_args_list)
#         #print(mock_start_trace_instance.call_args_list)
#
#         entry_point()

def test_start_trace__when_logging_is_configured():
    with patch('telemetry_logging.decorators.Logger') as mock_logger2:
        mock_logger_instance = mock_logger2.return_value

        # Prepare
        @start_trace
        def app_sample_function(initial_span=None):
            assert (1 + 1) == 2
            return "I am an app sample function. Doing important calculations"

        os.environ['CLOUD_ROLE_NAME'] = 'cloud_role_name from environment'
        os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'applicationinsights_connection_string from environment'
        os.environ['SUBSYSTEM'] = 'subsystem from environment'
        os.environ['ORCHESTRATION_INSTANCE_ID'] = '4a540892-2c0a-46a9-9257-c4e13051d76b'

        settings = LoggingSettings()

        configure_logging(
            logging_settings=settings,
            extras={'key1': 'value1', 'key2': 'value2'}
        )
        app_sample_function()

        # Act
        print("SEE MEEEE")
        print(mock_logger_instance.call_args_list)




