import pytest
from unittest.mock import Mock
from telemetry_logging.span_recording import span_record_exception
from opentelemetry.trace import Status, StatusCode, Span


def test_span_record__exception_with_exception():
    # Arrange
    exception = Exception("General exception")
    span = Mock(spec=Span)

    # Act
    span_record_exception(exception, span)

    # Assert
    span.record_exception.assert_called_once()
    recorded_exception, attributes = span.record_exception.call_args
    assert recorded_exception[0] == exception
    assert "CategoryName" in attributes['attributes']
    assert attributes['attributes']["CategoryName"].startswith("Energinet.DataHub.")
