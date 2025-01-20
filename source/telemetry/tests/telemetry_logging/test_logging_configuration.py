import os
import sys
import pytest
from unittest.mock import patch
import unittest.mock as mock
from telemetry_logging.logging_configuration import (
    configure_logging,
    get_extras,
    add_extras,
    get_tracer,
    start_span,
    _IS_INSTRUMENTED,
    LoggingSettings
)

@pytest.fixture
def mock_logging_settings():
    """
    Fixture to mock logging settings
    Imitates use case behavior by having an outer process set environment variables and parsing
    command line arguments to the script
    """
    # Environment variables:
    os.environ['CLOUD_ROLE_NAME'] = 'test_role'
    os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'connection_string'
    os.environ['SUBSYSTEM'] = 'test_subsystem'
    os.environ['ORCHESTRATION_INSTANCE_ID'] = '4a540892-2c0a-46a9-9257-c4e13051d76b'
    # Command line arguments
    with mock.patch('sys.argv', ['program_name', '--force_configuration', 'false',
                                 '--orchestration_instance_id', '4a540892-2c0a-46a9-9257-c4e13051d76a']):
        logging_settings = LoggingSettings()
        logging_settings.applicationinsights_connection_string = None # for testing purposes
        yield logging_settings

@pytest.fixture
def mock_logging_extras():
    return {'extra1': 'extra value1'}

def test_configure_logging__then_environmental_variables_are_set(mock_logging_settings, mock_logging_extras):
    """
    Testing that the environment variable OTEL_SERVICE_NAME is set during invocation of configure_logging
    """
    # Arrange
    expected_cloud_role_name = 'test_role'

    # Act
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == expected_cloud_role_name


def test_configure_logging__configure_does_update_environmental_variables(mock_logging_settings, mock_logging_extras):
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"

    # Set the Logging Settings mock based on the initial value:
    mock_logging_settings.cloud_role_name = initial_cloud_role_name
    mock_logging_settings.subsystem = tracer_name

    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Act
    # Update the Logging Settings mock with updated_cloud_role_name:
    mock_logging_settings.cloud_role_name = updated_cloud_role_name
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == updated_cloud_role_name


def test_get_extras__when_no_extras_none_are_returned(mock_logging_settings):
    # Arrange
    initial_extras = {}
    # orhcestration_instance_id is always added to extras during logging configuration
    expected_extras = {'orchestration_instance_id': mock_logging_settings.orchestration_instance_id}

    configure_logging(logging_settings=mock_logging_settings, extras=initial_extras)

    # Act
    actual_extras = get_extras()

    # Assert
    assert actual_extras == expected_extras


def test_get_extras__when_set_extras_are_returned(mock_logging_settings):
    # Arrange
    initial_extras = {"key": "value"}
    expected_extras = initial_extras.copy()  # Create a copy
    # Add the orchestration_instance_id
    expected_extras['orchestration_instance_id'] = mock_logging_settings.orchestration_instance_id
    configure_logging(logging_settings=mock_logging_settings, extras=initial_extras)

    # Act
    actual_extras = get_extras()

    # Assert
    assert actual_extras == expected_extras


def test_configure_logging__when_no_connection_string_is_instrumented_does_not_reconfigure(mock_logging_settings,
                                                                                           mock_logging_extras):
    # Arrange
    initial_is_instrumented = _IS_INSTRUMENTED

    # Act
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    actual_is_instrumented = _IS_INSTRUMENTED
    assert actual_is_instrumented == initial_is_instrumented


def test_add_extras__extras_can_be_added_and_initial_extras_are_kept(mock_logging_settings, mock_logging_extras):
    # Arrange
    initial_extras = {"initial_key": "initial_value"}
    orchestration_instance_id = {"orchestration_instance_id": mock_logging_settings.orchestration_instance_id}
    new_extras = {"new_key": "new_value"}
    # We expect the orchestration ID
    expected_extras = orchestration_instance_id | initial_extras | new_extras

    # Act
    #configure_logging(cloud_role_name="test_role", tracer_name="test_tracer", extras=initial_extras)
    configure_logging(logging_settings=mock_logging_settings, extras=initial_extras)
    add_extras(new_extras)

    # Assert
    assert get_extras() == expected_extras


def test_get_tracer__then_a_tracer_is_returned(mock_logging_settings, mock_logging_extras):
    # Arrange
    tracer_name = "test_tracer"
    mock_logging_settings.subsystem = tracer_name

    # Act
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)
    tracer = get_tracer()

    # Assert
    assert tracer is not None


def test_get_tracer__then_a_tracer_is_returned_also_with_force_configure(mock_logging_settings, mock_logging_extras):
    # Arrange
    tracer_name = "test_tracer"
    mock_logging_settings.subsystem = tracer_name
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    mock_logging_settings.force_configuration = True
    # Act
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)
    tracer = get_tracer()

    # Assert
    assert tracer is not None

    # Clean up
    # Reset force_configuration for the fixture
    mock_logging_settings.force_configuration = False


def test_start_span__span_is_started(mock_logging_settings, mock_logging_extras):
    # Arrange
    tracer_name = "test_tracer"
    mock_logging_settings.subsystem = tracer_name

    # Act
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    with start_span("test_span") as span:
        assert span is not None


def test_start_span__span_is_started_with_force_configuration(mock_logging_settings, mock_logging_extras):
    # Arrange
    tracer_name = "test_tracer"
    mock_logging_settings.subsystem = tracer_name
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Act
    mock_logging_settings.force_configuration = True
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)
    # Assert
    with start_span("test_span") as span:
        assert span is not None

    # Clean up
    # Reset force_configuration for the fixture
    mock_logging_settings.force_configuration = False


@mock.patch("telemetry_logging.logging_configuration.configure_azure_monitor")
def test_configure_logging__when_connection_string_is_provided__azure_monitor_is_configured(
        mock_configure_azure_monitor, mock_logging_settings, mock_logging_extras):
    # Arrange
    connection_string = "overridden_connection_string"
    mock_logging_settings.applicationinsights_connection_string = connection_string

    # Act
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    mock_configure_azure_monitor.assert_called_once_with(connection_string=connection_string)

@mock.patch("telemetry_logging.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_not_updated_when_reconfigured(
        mock_configure_azure_monitor,
        mock_logging_settings,
        mock_logging_extras):

    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"

    mock_logging_settings.cloud_role_name = initial_cloud_role_name
    mock_logging_settings.applicationinsights_connection_string = connection_string
    mock_logging_settings.subsystem = tracer_name

    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Act
    # Update mock to verify that configure_logging() will not change env var "OTEL_SERVICE_NAME"
    mock_logging_settings.cloud_role_name = updated_cloud_role_name
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == initial_cloud_role_name


@mock.patch("telemetry_logging.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_updated_when_reconfigured_with_force_configure(
        mock_configure_azure_monitor,
        mock_logging_settings,
        mock_logging_extras):
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"

    mock_logging_settings.cloud_role_name = initial_cloud_role_name
    mock_logging_settings.applicationinsights_connection_string = connection_string
    mock_logging_settings.subsystem = tracer_name

    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Act
    mock_logging_settings.cloud_role_name = updated_cloud_role_name
    mock_logging_settings.force_configuration = True
    configure_logging(logging_settings=mock_logging_settings, extras=mock_logging_extras)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == updated_cloud_role_name

    # Clean up
    # Reset force_configuration for the fixture
    mock_logging_settings.force_configuration = False
