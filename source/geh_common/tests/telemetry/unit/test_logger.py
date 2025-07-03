import logging
from unittest.mock import patch

import pytest

from geh_common.telemetry.logger import Logger


@pytest.mark.parametrize(
    "log_method, log_func",
    [
        ("debug", logging.Logger.debug),
        ("info", logging.Logger.info),
        ("warning", logging.Logger.warning),
        ("warning", logging.Logger.error),
    ],
)
def test__log_method__when_called_with_custom_extras__passes_correct_extras(log_method, log_func):
    # Arrange
    logger = Logger("test_logger")
    test_message = f"Test {log_method} message"
    custom_extras = {"key": "value"}
    expected_extras = custom_extras | logger._extras

    with patch.object(logging.Logger, log_method) as mock_log_method:
        # Act
        getattr(logger, log_method)(test_message, extras=custom_extras)

        # Assert
        mock_log_method.assert_called_once_with(test_message, extra=expected_extras)
