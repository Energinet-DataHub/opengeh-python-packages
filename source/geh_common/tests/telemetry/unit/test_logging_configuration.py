import os
import sys
from unittest import mock
from unittest.mock import patch

import pytest
from pydantic_core import ValidationError

from geh_common.telemetry.logging_configuration import (
    LoggingSettings,
    add_extras,
    configure_logging,
    get_extras,
    get_is_instrumented,
    get_tracer,
    start_span,
)


@patch("geh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__then_environmental_variables_are_set_and_configure_azure_monitor_called(
    mock_configure_azure_monitor, unit_logging_configuration_with_connection_string, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "connection_string")
    # Arrange
    logging_settings_from_fixture, orchestration_instance_id = unit_logging_configuration_with_connection_string
    expected_cloud_role_name = logging_settings_from_fixture.cloud_role_name
    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == expected_cloud_role_name
    mock_configure_azure_monitor.assert_called_once


@patch("geh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__configure_twice_does_not_reconfigure(
    mock_configure_azure_monitor, unit_logging_configuration_with_connection_string, monkeypatch: pytest.MonkeyPatch
):
    # Arrange
    orchestration_instance_id = "4a540892-2c0a-46a9-9257-c4e13051d76a"
    sys_args = ["program_name", "--orchestration-instance-id", orchestration_instance_id]

    # Command line arguments
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "connection_string")
    monkeypatch.setattr(sys, "argv", sys_args)
    monkeypatch.setattr("geh_common.telemetry.logging_configuration.configure_azure_monitor", mock.Mock())

    # Arrange
    logging_settings_from_fixture, _ = unit_logging_configuration_with_connection_string

    # Create an updated logging_configuration, checking if it gets updated with a force_configuration = True
    updated_logging_config = LoggingSettings(
        cloud_role_name="test_role_updated",
        subsystem="test_subsystem_updated",
    )
    # Act (control that configure_azure_monitor() is not called using a patch)
    with mock.patch("geh_common.telemetry.logging_configuration.configure_azure_monitor"):
        configure_logging(cloud_role_name="test_role_updated", subsystem="test_subsystem_updated")

    # Assert that environment variable does not update when log has already been configured ()
    assert os.environ["OTEL_SERVICE_NAME"] != updated_logging_config.cloud_role_name
    assert os.environ["OTEL_SERVICE_NAME"] == logging_settings_from_fixture.cloud_role_name
    mock_configure_azure_monitor.assert_not_called


def test_get_extras__when_no_extras_none_are_returned(
    unit_logging_configuration_with_connection_string, monkeypatch: pytest.MonkeyPatch
):
    # Arrange
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "connection_string")
    logging_settings_from_fixture, orchestration_instance_id_from_fixture = (
        unit_logging_configuration_with_connection_string
    )

    default_expected_extras = {
        "orchestration_instance_id": str(orchestration_instance_id_from_fixture),
        "Subsystem": logging_settings_from_fixture.subsystem,
    }

    # Act
    actual_extras = get_extras()  # Get extras from logging_configuration module

    # Assert
    assert actual_extras == default_expected_extras


def test_get_extras__when_set_extras_are_returned(
    unit_logging_configuration_with_connection_string, monkeypatch: pytest.MonkeyPatch
):
    # Arrange
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "connection_string")
    logging_settings_from_fixture, orchestration_instance_id_from_fixture = (
        unit_logging_configuration_with_connection_string
    )
    # Add the orchestration_instance_id and subsystem expected to be added automatically by configure_logging
    default_expected_extras = {
        "orchestration_instance_id": str(orchestration_instance_id_from_fixture),
        "Subsystem": logging_settings_from_fixture.subsystem,
    }
    extras_to_add = {"extra1": "extra value1"}

    # Act
    add_extras(extras_to_add)
    expected_extras = default_expected_extras | extras_to_add
    actual_extras = get_extras()

    # Assert
    assert actual_extras == expected_extras


def test_add_extras__extras_can_be_added_and_initial_extras_are_kept(
    unit_logging_configuration_with_connection_string_with_extras, monkeypatch: pytest.MonkeyPatch
):
    # Arrange
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "connection_string")

    logging_settings_from_fixture, orchestration_instance_id, initial_extras_from_fixture = (
        unit_logging_configuration_with_connection_string_with_extras
    )

    default_expected_extras = {
        "orchestration_instance_id": str(orchestration_instance_id),
        "Subsystem": logging_settings_from_fixture.subsystem,
    }
    new_extras = {"new_key": "new_value"}

    # Act
    add_extras(new_extras)

    expected_extras = initial_extras_from_fixture | default_expected_extras | new_extras

    actual_extras = get_extras()

    assert expected_extras == actual_extras


def test_configure_logging_check_returns_correct_object(monkeypatch: pytest.MonkeyPatch):
    # Arrange
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "connection_string")
    monkeypatch.setattr("geh_common.telemetry.logging_configuration.configure_azure_monitor", mock.Mock())
    cloud_role_name = "unknown"
    subsystem = "subsystem"
    logging_settings = configure_logging(cloud_role_name=cloud_role_name, subsystem=subsystem)

    # Assert
    assert logging_settings.cloud_role_name == cloud_role_name
    assert logging_settings.subsystem == subsystem


def test_get_tracer__then_a_tracer_is_returned(unit_logging_configuration_with_connection_string):
    # Arrange
    tracer = get_tracer()

    # Assert
    assert tracer is not None


def test_start_span__span_is_started(unit_logging_configuration_with_connection_string):
    # Assert
    tracer = get_tracer()
    with start_span("test_span") as span:
        assert span is not None
    assert tracer is not None


def test_configure_logging_check_if_logging_configured(unit_logging_configuration_with_connection_string):
    # Arrange
    expected_logging_is_configured = True

    # Assert
    actual_logging_is_configured = get_is_instrumented()
    assert actual_logging_is_configured == expected_logging_is_configured


def test_logging_settings_without_any_params():
    """
    Test that LoggingSettings fails instantiation without any parameters or environment variables
    """
    # Act
    with pytest.raises(ValidationError):
        LoggingSettings()
