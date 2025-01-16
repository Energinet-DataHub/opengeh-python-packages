"""
New development approach: testing the logging setup using the decorator approach
"""
import os
from telemetry_logging import logging_configuration
from telemetry_logging import decorators as logging_decorators
from telemetry_logging import Logger

@logging_decorators.use_span()
def run_method():
    print("I am a new app and this is my functionality")
    log = Logger(__name__)
    log.info(f"Inside method")
    log.warning("I am now a warning inside the app")

@logging_decorators.start_trace
def entry_point(initial_span=None) -> None:
    try:
        run_method()
    except Exception as e:
        log = Logger(__name__)
        log.error(str(e))

# Set up environment variables for testing
os.environ['CLOUD_ROLE_NAME'] = 'dbr-electrical-heating'
os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'test_string'
os.environ['SUBSYSTEM'] = 'electrical-heating'

if __name__ == '__main__':
    # Create settings for logging using the LoggingSettings class
    log_settings = logging_configuration.LoggingSettings.load()

    # TODO: Force usage of LoggingSettings as input object for configure_logging()
    # Call configure_logging
    logging_configuration.configure_logging(
        cloud_role_name=log_settings.cloud_role_name,
        tracer_name=log_settings.subsystem,
        #applicationinsights_connection_string=log_settings.applicationinsights_connection_string,
        applicationinsights_connection_string=None,
        extras=log_settings.logging_extras
    )
    # Call Entry point
    entry_point()
