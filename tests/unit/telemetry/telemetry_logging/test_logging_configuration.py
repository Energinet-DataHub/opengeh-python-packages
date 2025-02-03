import os
import unittest.mock as mock

from opengeh_common.telemetry.logging_configuration import (
    _IS_INSTRUMENTED,
    add_extras,
    configure_logging,
    get_extras,
    get_tracer,
    start_span,
)


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


@mock.patch("opengeh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__when_connection_string_is_provided__azure_monitor_is_configured(
    mock_configure_azure_monitor,
):
    # Arrange
    cloud_role_name = "test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"

    # Act
    configure_logging(
        cloud_role_name=cloud_role_name,
        tracer_name=tracer_name,
        applicationinsights_connection_string=connection_string,
    )

    # Assert
    mock_configure_azure_monitor.assert_called_once_with(connection_string=connection_string)


@mock.patch("opengeh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_not_updated_when_reconfigured(
    mock_configure_azure_monitor,
):
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"
    configure_logging(
        cloud_role_name=initial_cloud_role_name,
        tracer_name=tracer_name,
        applicationinsights_connection_string=connection_string,
    )

    # Act
    configure_logging(
        cloud_role_name=updated_cloud_role_name,
        tracer_name=tracer_name,
        applicationinsights_connection_string=connection_string,
    )

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == initial_cloud_role_name


@mock.patch("opengeh_common.telemetry.logging_configuration.configure_azure_monitor")
def test_configure_logging__cloud_role_name_is_updated_when_reconfigured_with_force_configure(
    mock_configure_azure_monitor,
):
    # Arrange
    initial_cloud_role_name = "test_role"
    updated_cloud_role_name = "updated_test_role"
    tracer_name = "test_tracer"
    connection_string = "connection_string"
    configure_logging(
        cloud_role_name=initial_cloud_role_name,
        tracer_name=tracer_name,
        applicationinsights_connection_string=connection_string,
    )

    # Act
    configure_logging(
        cloud_role_name=updated_cloud_role_name,
        tracer_name=tracer_name,
        applicationinsights_connection_string=connection_string,
        force_configuration=True,
    )

    # Assert
    assert os.environ["OTEL_SERVICE_NAME"] == updated_cloud_role_name
