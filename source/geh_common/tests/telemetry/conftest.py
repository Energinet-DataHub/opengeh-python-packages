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
UNIT_TEST_SYS_ARGS = ["program_name", "--orchestration-instance-id", "4a540892-2c0a-46a9-9257-c4e13051d76a"]


def cleanup_logging() -> None:
    set_extras({})
    set_is_instrumented(False)
    set_tracer(None)
    set_tracer_name("")
    os.environ.pop("OTEL_SERVICE_NAME", None)


@pytest.fixture()  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_connection_string():
    """
    Fixture to setup the logging configuration used for unit tests
    Fixture sets up the logging, but patches configure_azure_monitor so it will not try to actually configure a real connection
    """
    sys_args = UNIT_TEST_SYS_ARGS
    orchestration_instance_id = sys_args[2]
    # Command line arguments
    with pytest.MonkeyPatch.context() as ctx:
        ctx.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", UNIT_TEST_DUMMY_CONNECTION_STRING)
        ctx.setattr(sys, "argv", sys_args)
        ctx.setattr("geh_common.telemetry.logging_configuration.configure_azure_monitor", mock.Mock())

        yield (
            configure_logging(subsystem=UNIT_TEST_SUBSYSTEM, cloud_role_name=UNIT_TEST_CLOUD_ROLE_NAME),
            # UNIT_TEST_CLOUD_ROLE_NAME,
            # UNIT_TEST_SUBSYSTEM,
            orchestration_instance_id,
        )

        # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
        cleanup_logging()


@pytest.fixture()  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_connection_string_with_extras():
    """
    Fixture to setup the logging configuration used for unit tests
    Fixture sets up the logging, but patches configure_azure_monitor so it will not try to actually configure a real connection
    """
    initial_extras = {"extra_key": "extra_value"}
    sys_args = UNIT_TEST_SYS_ARGS
    orchestration_instance_id = sys_args[2]
    # Command line arguments
    with pytest.MonkeyPatch.context() as ctx:
        ctx.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", UNIT_TEST_DUMMY_CONNECTION_STRING)
        ctx.setattr(sys, "argv", sys_args)
        ctx.setattr("geh_common.telemetry.logging_configuration.configure_azure_monitor", mock.Mock())

        yield (
            configure_logging(
                subsystem=UNIT_TEST_SUBSYSTEM, cloud_role_name=UNIT_TEST_CLOUD_ROLE_NAME, extras=initial_extras
            ),
            orchestration_instance_id,
            initial_extras,
        )

        # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
        cleanup_logging()
