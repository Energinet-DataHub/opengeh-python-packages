import os
from unittest import mock

import pytest

from geh_common.telemetry import logging_configuration
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging, set_logging_configured


def cleanup_logging() -> None:
    set_logging_configured(False)
    logging_configuration._EXTRAS = {}
    logging_configuration._IS_INSTRUMENTED = False
    logging_configuration._TRACER = None
    logging_configuration._TRACER_NAME = ""
    os.environ.pop("OTEL_SERVICE_NAME", None)


@pytest.fixture(scope="function")  # We want to reset the fixture object after each function has used it
def unit_logging_configuration():
    """
    Fixture to setup the logging configuration used for unit tests
    Unit tests do not need an actual connectionstring to application insights
    """
    sys_args = ["program_name", "--orchestration-instance-id", "4a540892-2c0a-46a9-9257-c4e13051d76a"]

    # Command line arguments
    with mock.patch("sys.argv", sys_args):
        logging_settings = LoggingSettings(cloud_role_name="test_role", subsystem="test_subsystem")
        yield configure_logging(logging_settings=logging_settings), logging_settings

    # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
    cleanup_logging()


@pytest.fixture(scope="function")  # We want to reset the fixture object after each function has used it
def unit_logging_configuration_with_extras():
    """
    Fixture to setup the logging configuration used for unit tests, here with an added set of initial extras
    """
    sys_args = ["program_name", "--orchestration-instance-id", "4a540892-2c0a-46a9-9257-c4e13051d76a"]

    initial_extras = {"extra_key": "extra_value"}

    # Command line arguments
    with mock.patch("sys.argv", sys_args):
        logging_settings = LoggingSettings(cloud_role_name="test_role", subsystem="test_subsystem")
        yield (
            configure_logging(logging_settings=logging_settings, extras=initial_extras),
            logging_settings,
            initial_extras,
        )

    # Clean up logging configuration module after each usage of the fixture, by setting logging configured to False
    cleanup_logging()
