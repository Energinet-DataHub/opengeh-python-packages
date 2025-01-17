import pytest
from unittest.mock import patch, MagicMock
from telemetry_logging.decorators import use_span, start_trace


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
    assert result == "test"


def test_start_trace__when_logging_not_configured(mock_logger, mock_start_trace):
    mock_logger_instance = mock_logger.return_value
    mock_start_trace_instance = mock_start_trace.return_value

    @start_trace
    def app_sample_function():
        return "I am an app sample function"

    def entry_point():
        print("I am an entry point function, who is supposed to configure logging - but I don't in this case")
        app_sample_function()

    with pytest.raises(NotImplementedError):
        entry_point()
