import os
from unittest import mock

import pytest

from geh_common.telemetry.logging_configuration import (
    LoggingSettings,
    configure_logging,
    set_extras,
    set_is_instrumented,
    set_logging_configured,
    set_tracer,
    set_tracer_name,
)


def cleanup_logging() -> None:
    set_logging_configured(False)
    set_extras({})
    set_is_instrumented(False)
    set_tracer(None)
    set_tracer_name("")
    os.environ.pop("OTEL_SERVICE_NAME", None)


@pytest.fixture(scope="function")  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_connection_string():
    """
    Fixture to setup the logging configuration used for unit tests
    Fixture sets up the logging, but patches configure_azure_monitor so it will not try to actually configure a real connection
    """
    sys_args = ["program_name", "--orchestration-instance-id", "4a540892-2c0a-46a9-9257-c4e13051d76a"]

    # Command line arguments
    with (
        mock.patch("sys.argv", sys_args),
        mock.patch("geh_common.telemetry.logging_configuration.configure_azure_monitor"),
    ):
        logging_settings = LoggingSettings(
            cloud_role_name="test_role",
            subsystem="test_subsystem",
            applicationinsights_connection_string="connectionString",
        )
        yield configure_logging(logging_settings=logging_settings), logging_settings

    # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
    cleanup_logging()


@pytest.fixture(scope="function")  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_connection_string_with_extras():
    """
    Fixture to setup the logging configuration used for unit tests
    Fixture sets up the logging, but patches configure_azure_monitor so it will not try to actually configure a real connection
    """
    initial_extras = {"extra_key": "extra_value"}
    sys_args = ["program_name", "--orchestration-instance-id", "4a540892-2c0a-46a9-9257-c4e13051d76a"]

    # Command line arguments
    with (
        mock.patch("sys.argv", sys_args),
        mock.patch("geh_common.telemetry.logging_configuration.configure_azure_monitor"),
    ):
        logging_settings = LoggingSettings(
            cloud_role_name="test_role",
            subsystem="test_subsystem",
            applicationinsights_connection_string="connectionString",
        )
        yield (
            configure_logging(logging_settings=logging_settings, extras=initial_extras),
            logging_settings,
            initial_extras,
        )

    # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
    cleanup_logging()
