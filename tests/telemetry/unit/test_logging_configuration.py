import os
from unittest import mock
from uuid import uuid4

import pytest
from pydantic_core import ValidationError

from geh_common.telemetry import logging_configuration
from geh_common.telemetry.logging_configuration import (
    _IS_INSTRUMENTED,
    LoggingSettings,
    add_extras,
    configure_logging,
    get_extras,
    get_logging_configured,
    get_tracer,
    set_logging_configured,
    start_span,
)


def cleanup_logging() -> None:
    set_logging_configured(False)
    logging_configuration._EXTRAS = {}
    logging_configuration._IS_INSTRUMENTED = False
    logging_configuration._TRACER = None
    logging_configuration._TRACER_NAME = ""
    os.environ.pop("OTEL_SERVICE_NAME", None)


def test_configure_logging__then_environmental_variables_are_set(unit_logging_configuration):
    """
    Testing that the environment variable OTEL_SERVICE_NAME is set during invocation of configure_logging
    """
    # Arrange
    expected_cloud_role_name = "test_role"
    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == expected_cloud_role_name


def test_configure_logging__configure_does_update_environmental_variables(unit_logging_configuration):
    # Arrange
    _, logging_settings_from_fixture = unit_logging_configuration

    # Create an updated logging_configuration, checking if it gets updated with a force_configuration = True
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        force_configuration=True,
        orchestration_instance_id=uuid4(),
    )
    # Act
    configure_logging(logging_settings=updated_logging_config)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == updated_logging_config.cloud_role_name
    assert os.environ["OTEL_SERVICE_NAME"] != logging_settings_from_fixture.cloud_role_name


def test_get_extras__when_no_extras_none_are_returned(unit_logging_configuration):
    # Arrange
    _, logging_settings_from_fixture = unit_logging_configuration

    default_expected_extras = {
        "orchestration_instance_id": str(logging_settings_from_fixture.orchestration_instance_id),
        "subsystem": str(logging_settings_from_fixture.subsystem),
    }

    # Act
    actual_extras = get_extras()  # Get extras from logging_configuration module

    # Assert
    assert actual_extras == default_expected_extras


def test_get_extras__when_set_extras_are_returned(unit_logging_configuration):
    # Arrange
    _, logging_settings_from_fixture = unit_logging_configuration
    # Add the orchestration_instance_id and subsystem expected to be added automatically by configure_logging
    default_expected_extras = {
        "orchestration_instance_id": str(logging_settings_from_fixture.orchestration_instance_id),
        "subsystem": str(logging_settings_from_fixture.subsystem),
    }
    extras_to_add = {"extra1": "extra value1"}

    # Act
    add_extras(extras_to_add)
    expected_extras = default_expected_extras | extras_to_add
    actual_extras = get_extras()

    # Assert
    assert actual_extras == expected_extras


def test_configure_logging__when_no_connection_string_is_instrumented_does_not_reconfigure(unit_logging_configuration):
    # Verifies that the initial log configuration fom the unit_logging_configuration will remain not instrumented, even though we force configuration with an empty connection string
    # Arrange
    initial_is_instrumented = _IS_INSTRUMENTED

    # Act
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        applicationinsights_connection_string=None,
        orchestration_instance_id=uuid4(),
        force_configuration=True,
    )
    # Act
    configure_logging(logging_settings=updated_logging_config)

    # Assert
    actual_is_instrumented = _IS_INSTRUMENTED

    assert actual_is_instrumented == initial_is_instrumented


def test_add_extras__extras_can_be_added_and_initial_extras_are_kept(unit_logging_configuration_with_extras):
    # Arrange
    _, logging_settings_from_fixture, initial_extras_from_fixture = unit_logging_configuration_with_extras

    default_expected_extras = {
        "orchestration_instance_id": str(logging_settings_from_fixture.orchestration_instance_id),
        "subsystem": str(logging_settings_from_fixture.subsystem),
    }
    new_extras = {"new_key": "new_value"}

    # Act
    add_extras(new_extras)

    expected_extras = initial_extras_from_fixture | default_expected_extras | new_extras

    actual_extras = get_extras()

    assert expected_extras == actual_extras


def test_get_tracer__then_a_tracer_is_returned(unit_logging_configuration):
    # Arrange
    tracer = get_tracer()

    # Assert
    assert tracer is not None


def test_get_tracer__then_a_tracer_is_returned_also_with_force_configure(unit_logging_configuration):
    # Arrange
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        applicationinsights_connection_string=None,
        orchestration_instance_id=uuid4(),
        force_configuration=True,
    )
    # Act
    configure_logging(logging_settings=updated_logging_config)
    tracer = get_tracer()

    # Assert
    assert tracer is not None


def test_start_span__span_is_started(unit_logging_configuration):
    # Assert
    tracer = get_tracer()
    with start_span("test_span") as span:
        assert span is not None
    assert tracer is not None


def test_start_span__span_is_started_with_force_configuration(unit_logging_configuration):
    # Arrange
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        applicationinsights_connection_string=None,
        orchestration_instance_id=uuid4(),
        force_configuration=True,
    )
    # Act
    configure_logging(logging_settings=updated_logging_config)

    # Assert
    with start_span("test_span") as span:
        assert span is not None


@mock.patch("geh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__when_connection_string_is_provided__azure_monitor_is_configured(
    mock_configure_azure_monitor,
):
    # Arrange
    connection_string = "overridden_connection_string"

    # Arrange
    logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        applicationinsights_connection_string=connection_string,
        orchestration_instance_id=uuid4(),
    )

    # Act
    configure_logging(logging_settings=logging_config)

    # Assert
    mock_configure_azure_monitor.assert_called_once_with(connection_string=connection_string)

    # Clean up manually
    cleanup_logging()


@mock.patch("geh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_not_updated_when_reconfigured(
    mock_configure_azure_monitor, unit_logging_configuration_with_connection_string
):
    # Arrange
    # Fixture unit_logging_configuration_with_connection_string sets the configuration initally, with expected value
    _, logging_settings_from_fixture = unit_logging_configuration_with_connection_string
    initial_cloud_role_name = logging_settings_from_fixture.cloud_role_name
    # Arrange new logging config
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        applicationinsights_connection_string="newConnectionString",
        orchestration_instance_id=uuid4(),
    )
    # Act
    configure_logging(logging_settings=updated_logging_config)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] != updated_logging_config.cloud_role_name
    assert os.environ["OTEL_SERVICE_NAME"] == initial_cloud_role_name


@mock.patch("geh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_updated_when_reconfigured_with_force_configure(
    mock_configure_azure_monitor, unit_logging_configuration_with_connection_string
):
    # Arrange
    # Fixture unit_logging_configuration_with_connection_string sets the configuration initally
    _, logging_settings_from_fixture = unit_logging_configuration_with_connection_string
    initial_cloud_role_name = logging_settings_from_fixture.cloud_role_name
    # Arrange new logging config
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
        applicationinsights_connection_string="newConnectionString",
        orchestration_instance_id=uuid4(),
        force_configuration=True,
    )
    # Act
    configure_logging(logging_settings=updated_logging_config)

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] != initial_cloud_role_name
    assert os.environ["OTEL_SERVICE_NAME"] == updated_logging_config.cloud_role_name


def test_configure_logging_check_if_logging_configured(unit_logging_configuration):
    # Arrange
    expected_logging_is_configured = True

    # Assert
    actual_logging_is_configured = get_logging_configured()
    assert actual_logging_is_configured == expected_logging_is_configured


def test_logging_settings_without_any_params():
    """
    Test that LoggingSettings fails instantiation without any paframeters or environment variables
    """
    # Act
    with pytest.raises(ValidationError):
        LoggingSettings()
