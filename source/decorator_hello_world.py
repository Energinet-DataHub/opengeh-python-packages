"""
New development approach: testing the logging setup using the decorator approach
"""
from telemetry_logging import logging_configuration
from telemetry_logging import decorators as logging_decorators
from telemetry_logging import Logger

@logging_decorators.use_span()
def run_method():
    print("I am a new app and this is my functionality")
    log = Logger(__name__)
    log.info(f"Inside method")
    log.warning("I am now a warning inside the app")

@logging_decorators.use_logging
def entry_point(span=None, message=None) -> None:
    try:
        if span:
            print(message)
        run_method()
    except Exception as e:
        log = Logger(__name__)
        log.error(str(e))


log_settings = logging_configuration.LoggingSettings(
        cloud_role_name="MyAppRole",
        tracer_name="MyTracerName",
        applicationinsights_connection_string=None,
        logging_extras={"key1": "value1", "key2": "value2"},
        force_configuration=False
    )
logging_configuration.configure_logging(
    cloud_role_name=log_settings.cloud_role_name,
    tracer_name=log_settings.tracer_name,
    applicationinsights_connection_string=log_settings.applicationinsights_connection_string,
    extras=log_settings.logging_extras,
    force_configuration=log_settings.force_configuration
)
entry_point()






