import pytest
from unittest.mock import patch, MagicMock
from telemetry_logging.decorators import use_span


# Mocking the Logger and start_span
@pytest.fixture
def mock_logger():
    with patch('telemetry_logging.decorators.Logger') as MockLogger:
        yield MockLogger


@pytest.fixture
def mock_start_span():
    with patch('telemetry_logging.decorators.start_span') as MockStartSpan:
        yield MockStartSpan


def test_use_span__when_name_is_defined(mock_logger, mock_start_span):
    mock_logger_instance = mock_logger.return_value
    mock_start_span_instance = mock_start_span.return_value

    @use_span(name="test_span")
    def sample_function():
        return "test"

    result = sample_function()

    mock_start_span.assert_called_once_with("test_span")
    mock_logger.assert_called_once_with("test_span")
    mock_logger_instance.info.assert_called_once_with("Started executing function: test_span")
    assert result == "test"


def test_use_span__when_name_is_not_defined(mock_logger, mock_start_span):
    mock_logger_instance = mock_logger.return_value
    mock_start_span_instance = mock_start_span.return_value

    @use_span()
    def sample_function():
        return "test"

    result = sample_function()

    mock_start_span.assert_called_once_with("test_use_span__when_name_is_not_defined.<locals>.sample_function")
    mock_logger.assert_called_once_with("test_use_span__when_name_is_not_defined.<locals>.sample_function")
    mock_logger_instance.info.assert_called_once_with("Started executing function: test_use_span__when_name_is_not_defined.<locals>.sample_function")
    assert result == "test"
