from unittest.mock import patch

import pytest

from geh_common.telemetry.decorators import use_span


# Mocking the Logger and start_span
@pytest.fixture
def mock_logger():
    with patch("opengeh_common.telemetry.decorators.Logger") as MockLogger:
        yield MockLogger


@pytest.fixture
def mock_start_span():
    with patch("opengeh_common.telemetry.decorators.start_span") as MockStartSpan:
        yield MockStartSpan


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
