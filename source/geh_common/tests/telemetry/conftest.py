import os
import sys
from unittest import mock

import pytest

from geh_common.telemetry.logging_configuration import (
    configure_logging,
    set_extras,
    set_is_instrumented,
    set_tracer,
    set_tracer_name,
)

UNIT_TEST_CLOUD_ROLE_NAME = "test_role"
UNIT_TEST_SUBSYSTEM = "test_subsystem"
UNIT_TEST_DUMMY_CONNECTION_STRING = "connectionString"


def cleanup_logging() -> None:
    set_extras({})
    set_is_instrumented(False)
    set_tracer(None)
    set_tracer_name("")
    os.environ.pop("OTEL_SERVICE_NAME", None)


@pytest.fixture(scope="function")  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_connection_string(monkeypatch: pytest.MonkeyPatch):
    """
    Fixture to setup the logging configuration used for unit tests
    Fixture sets up the logging, but patches configure_azure_monitor so it will not try to actually configure a real connection
    """
    orchestration_instance_id = "4a540892-2c0a-46a9-9257-c4e13051d76a"
    sys_args = ["program_name", "--orchestration-instance-id", orchestration_instance_id]

    # Command line arguments
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", UNIT_TEST_DUMMY_CONNECTION_STRING)
    monkeypatch.setattr(sys, "argv", sys_args)
    monkeypatch.setattr("geh_common.telemetry.logging_configuration.configure_azure_monitor", mock.Mock())

    yield (
        configure_logging(cloud_role_name=UNIT_TEST_CLOUD_ROLE_NAME, subsystem=UNIT_TEST_SUBSYSTEM),
        orchestration_instance_id,
    )

    # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
    cleanup_logging()


@pytest.fixture(scope="function")  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_connection_string_with_extras(monkeypatch: pytest.MonkeyPatch):
    """
    Fixture to setup the logging configuration used for unit tests
    Fixture sets up the logging, but patches configure_azure_monitor so it will not try to actually configure a real connection
    """
    initial_extras = {"extra_key": "extra_value"}
    orchestration_instance_id = "4a540892-2c0a-46a9-9257-c4e13051d76a"
    sys_args = ["program_name", "--orchestration-instance-id", orchestration_instance_id]

    # Command line arguments
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", UNIT_TEST_DUMMY_CONNECTION_STRING)
    monkeypatch.setattr(sys, "argv", sys_args)
    monkeypatch.setattr("geh_common.telemetry.logging_configuration.configure_azure_monitor", mock.Mock())

    yield (
        configure_logging(
            cloud_role_name=UNIT_TEST_CLOUD_ROLE_NAME, subsystem=UNIT_TEST_SUBSYSTEM, extras=initial_extras
        ),
        orchestration_instance_id,
        initial_extras,
    )

    # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
    cleanup_logging()
