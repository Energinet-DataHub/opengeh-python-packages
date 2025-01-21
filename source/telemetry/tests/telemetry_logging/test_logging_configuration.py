import os
import sys
import pytest
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
from uuid import UUID
from pydantic import ValidationError


# ---------------------------------------------------------------------------------------------
@pytest.fixture
def mock_logging_settings():
    os.environ['CLOUD_ROLE_NAME'] = 'cloud_role_name_value'
    os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'applicationinsights_connection_string_value'
    os.environ['SUBSYSTEM'] = 'subsystem_value'
    with mock.patch('sys.argv', ['program_name', '--force_configuration', 'true', '--orchestration_instance_id', '4a540892-2c0a-46a9-9257-c4e13051d76a']):
        yield LoggingSettings()

def test_logging_settings_from_mock(mock_logging_settings):
    # Arrange
    expected_cloud_role_name = 'cloud_role_name_value'
    expected_applicationinsights_connection_string = 'applicationinsights_connection_string_value'
    expected_subsystem = 'subsystem_value'
    expected_orchestration_instance_id = UUID('4a540892-2c0a-46a9-9257-c4e13051d76a')
    expected_force_configuration = True
    # Act
    # Assert
    assert mock_logging_settings.cloud_role_name == expected_cloud_role_name
    assert mock_logging_settings.applicationinsights_connection_string == expected_applicationinsights_connection_string
    assert mock_logging_settings.subsystem == expected_subsystem
    assert mock_logging_settings.orchestration_instance_id == expected_orchestration_instance_id
    assert mock_logging_settings.force_configuration is expected_force_configuration


def test_logging_settings_without_any_params():
    # Arrange

    # Act
    with pytest.raises(ValidationError):
        logging_settings = LoggingSettings()
    # Assert



def test_logging_settings_without_cli_params():
    # Arrange
    os.environ['CLOUD_ROLE_NAME'] = 'cloud_role_name_value'
    os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'applicationinsights_connection_string_value'
    os.environ['SUBSYSTEM'] = 'subsystem_value'


    expected_cloud_role_name = 'cloud_role_name_value'
    expected_applicationinsights_connection_string = 'applicationinsights_connection_string_value'
    expected_subsystem = 'subsystem_value'
    expected_orchestration_instance_id = None
    expected_force_configuration = False
    # Act

    logging_settings = LoggingSettings()

    # Assert
    assert logging_settings.cloud_role_name == expected_cloud_role_name
    assert logging_settings.applicationinsights_connection_string == expected_applicationinsights_connection_string
    assert logging_settings.subsystem == expected_subsystem
    assert logging_settings.orchestration_instance_id == expected_orchestration_instance_id
    assert logging_settings.force_configuration is expected_force_configuration

def test_logging_settings_all_params_from_env():
    # Arrange
    os.environ['CLOUD_ROLE_NAME'] = 'cloud_role_name_value'
    os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'applicationinsights_connection_string_value'
    os.environ['SUBSYSTEM'] = 'subsystem_value'
    os.environ['ORCHESTRATION_INSTANCE_ID'] = '4a540892-2c0a-46a9-9257-c4e13051d76a'
    os.environ['FORCE_CONFIGURATION'] = 'True'

    expected_cloud_role_name = 'cloud_role_name_value'
    expected_applicationinsights_connection_string = 'applicationinsights_connection_string_value'
    expected_subsystem = 'subsystem_value'
    expected_orchestration_instance_id = UUID('4a540892-2c0a-46a9-9257-c4e13051d76a')
    expected_force_configuration = True


    # Act
    logging_settings = LoggingSettings()

    # Assert
    assert logging_settings.cloud_role_name == expected_cloud_role_name
    assert logging_settings.applicationinsights_connection_string == expected_applicationinsights_connection_string
    assert logging_settings.subsystem == expected_subsystem
    assert logging_settings.orchestration_instance_id == expected_orchestration_instance_id
    assert logging_settings.force_configuration is expected_force_configuration



def test_logging_settings_all_params_from_cli():
    # Arrange
    sysargs = [
        'program_name',
        '--cloud_role_name', 'cloud_role_name_value',
        '--applicationinsights_connection_string', 'applicationinsights_connection_string_value',
        '--subsystem', 'subsystem_value',
        '--force_configuration', 'true',
        '--orchestration_instance_id', '4a540892-2c0a-46a9-9257-c4e13051d76a'
    ]

    # Act
    with mock.patch('sys.argv', sysargs):
        logging_settings = LoggingSettings()



        expected_cloud_role_name = 'cloud_role_name_value'
        expected_applicationinsights_connection_string = 'applicationinsights_connection_string_value'
        expected_subsystem = 'subsystem_value'
        expected_orchestration_instance_id = UUID('4a540892-2c0a-46a9-9257-c4e13051d76a')
        expected_force_configuration = True


    # Assert
        assert logging_settings.cloud_role_name == expected_cloud_role_name
        assert logging_settings.applicationinsights_connection_string == expected_applicationinsights_connection_string
        assert logging_settings.subsystem == expected_subsystem
        assert logging_settings.orchestration_instance_id == expected_orchestration_instance_id
        assert logging_settings.force_configuration is expected_force_configuration

        assert os.environ.get('CLOUD_ROLE_NAME') is None
        assert os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING') is None
        assert os.environ.get('SUBSYSTEM') is None
        assert os.environ.get('ORCHESTRATION_INSTANCE_ID') is None
        assert os.environ.get('FORCE_CONFIGURATION') is None


def test_logging_settings_params_from_both_cli_and_env(mock_logging_settings):
    # Arrange
    sysargs = [
        'program_name',
        '--cloud_role_name', 'cloud_role_name_value_from_cli',
        '--force_configuration', 'true',
        '--orchestration_instance_id', '4a540892-2c0a-46a9-9257-c4e13051d76a'
    ]
    # Act
    with mock.patch('sys.argv', sysargs):
        logging_settings = LoggingSettings()

        os.environ['CLOUD_ROLE_NAME'] = 'cloud_role_name_value_from_env'
        os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'applicationinsights_connection_string_value'
        os.environ['SUBSYSTEM'] = 'subsystem_value'

        expected_cloud_role_name = 'cloud_role_name_value_cli'
        expected_applicationinsights_connection_string = 'applicationinsights_connection_string_value'
        expected_subsystem = 'subsystem_value'
        expected_orchestration_instance_id = UUID('4a540892-2c0a-46a9-9257-c4e13051d76a')
        expected_force_configuration = True

    # Assert
        assert logging_settings.cloud_role_name == expected_cloud_role_name
        assert logging_settings.applicationinsights_connection_string == expected_applicationinsights_connection_string
        assert logging_settings.subsystem == expected_subsystem
        assert logging_settings.orchestration_instance_id == expected_orchestration_instance_id
        assert logging_settings.force_configuration is expected_force_configuration

# ---------------------------------------------------------------------------------------------


def test_mocker(mock_logging_settings):
    #logging_settings.force_configuration = True
    print("sys.argv:")
    print(sys.argv)
    print("Mock settings object:")
    print(mock_logging_settings)
    assert 1 == 1

# ---------------------------------------------------------------------------------------------

def test_configure_logging__then_environmental_variables_are_set():
    # Arrange
    cloud_role_name = "test_role"
    tracer_name = "test_tracer"

    # Act
    configure_logging(cloud_role_name=cloud_role_name, tracer_name=tracer_name)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == cloud_role_name


def test_configure_logging__configure_does_update_environmental_variables():
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    configure_logging(cloud_role_name=initial_cloud_role_name, tracer_name=tracer_name)

    # Act
    configure_logging(cloud_role_name=updated_cloud_role_name, tracer_name=tracer_name)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == updated_cloud_role_name


def test_get_extras__when_no_extras_none_are_returned():
    # Arrange
    cloud_role_name = "test_role"
    tracer_name = "test_tracer"
    initial_extras = {}
    configure_logging(cloud_role_name=cloud_role_name, tracer_name=tracer_name)

    # Act
    actual_extras = get_extras()

    # Assert
    assert actual_extras == initial_extras


def test_get_extras__when_set_extras_are_returned():
    # Arrange
    cloud_role_name = "test_role"
    tracer_name = "test_tracer"
    initial_extras = {"key": "value"}
    configure_logging(cloud_role_name=cloud_role_name, tracer_name=tracer_name, extras=initial_extras)

    # Act
    actual_extras = get_extras()

    # Assert
    assert actual_extras == initial_extras


def test_configure_logging__when_no_connection_string_is_instrumented_does_not_reconfigure():
    # Arrange
    cloud_role_name = "test_role"
    tracer_name = "test_tracer"
    initial_is_instrumented = _IS_INSTRUMENTED

    # Act
    configure_logging(cloud_role_name=cloud_role_name, tracer_name=tracer_name)

    # Assert
    actual_is_instrumented = _IS_INSTRUMENTED
    assert actual_is_instrumented == initial_is_instrumented


def test_add_extras__extras_can_be_added_and_initial_extras_are_kept():
    # Arrange
    initial_extras = {"initial_key": "initial_value"}
    new_extras = {"new_key": "new_value"}
    combined_extras = initial_extras | new_extras

    # Act
    configure_logging(cloud_role_name="test_role", tracer_name="test_tracer", extras=initial_extras)
    add_extras(new_extras)

    # Assert
    assert get_extras() == combined_extras


def test_get_tracer__then_a_tracer_is_returned():
    # Arrange
    tracer_name = "test_tracer"

    # Act
    configure_logging(cloud_role_name="test_role", tracer_name=tracer_name)
    tracer = get_tracer()

    # Assert
    assert tracer is not None


def test_get_tracer__then_a_tracer_is_returned_also_with_force_configure():
    # Arrange
    tracer_name = "test_tracer"
    configure_logging(cloud_role_name="test_role", tracer_name=tracer_name)

    # Act
    configure_logging(cloud_role_name="test_role", tracer_name=tracer_name, force_configuration=True)
    tracer = get_tracer()

    # Assert
    assert tracer is not None


def test_start_span__span_is_started():
    # Arrange
    tracer_name = "test_tracer"

    # Act
    configure_logging(cloud_role_name="test_role", tracer_name=tracer_name)

    # Assert
    with start_span("test_span") as span:
        assert span is not None


def test_start_span__span_is_started_with_force_configuration():
    # Arrange
    tracer_name = "test_tracer"
    configure_logging(cloud_role_name="test_role", tracer_name=tracer_name)

    # Act
    configure_logging(cloud_role_name="test_role", tracer_name=tracer_name, force_configuration=True)

    # Assert
    with start_span("test_span") as span:
        assert span is not None


@mock.patch("telemetry_logging.logging_configuration.configure_azure_monitor")
def test_configure_logging__when_connection_string_is_provided__azure_monitor_is_configured(mock_configure_azure_monitor):
    # Arrange
    cloud_role_name = "test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"

    # Act
    configure_logging(cloud_role_name=cloud_role_name, tracer_name=tracer_name, applicationinsights_connection_string=connection_string)

    # Assert
    mock_configure_azure_monitor.assert_called_once_with(connection_string=connection_string)


@mock.patch("telemetry_logging.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_not_updated_when_reconfigured(mock_configure_azure_monitor):
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"
    configure_logging(cloud_role_name=initial_cloud_role_name, tracer_name=tracer_name, applicationinsights_connection_string=connection_string)

    # Act
    configure_logging(cloud_role_name=updated_cloud_role_name, tracer_name=tracer_name, applicationinsights_connection_string=connection_string)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == initial_cloud_role_name


@mock.patch("telemetry_logging.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_updated_when_reconfigured_with_force_configure(mock_configure_azure_monitor):
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"
    configure_logging(cloud_role_name=initial_cloud_role_name, tracer_name=tracer_name, applicationinsights_connection_string=connection_string)

    # Act
    configure_logging(cloud_role_name=updated_cloud_role_name, tracer_name=tracer_name, applicationinsights_connection_string=connection_string, force_configuration=True)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == updated_cloud_role_name




