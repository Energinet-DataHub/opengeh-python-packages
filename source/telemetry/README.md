# Telemetry

Shared telemetry logging library for Python. Useful for creating spans in Azure.

## Usage

Initial configuration of logging:
```python
import telemetry_logging.logging_configuration as config
from telemetry_logging.span_recording import span_record_exception
from telemetry_logging import Logger, use_span

# Span will create a nested span starting from
@use_span()
def run_method() -> None:
    # Create a logger (will inherit extras and name from the span and logging configuration setup initially)
    log = Logger(__name__)
    log.info(f"Inside method")


def entry_point() -> None :
    applicationinsights_connection_string = os.getenv(
            "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )

    # Setup the configuration for the logger
    # Extras will be shown as custom dimensions in application insights
    # Setting the application insights connection string for logging to Azure
    config.configure_logging(
            cloud_role_name="some-cloud-name",
            tracer_name="some-tracer-name",
            applicationinsights_connection_string=applicationinsights_connection_string,
            extras={"some_extra": "extra_value"},
    )

    # Starting the first span
    with config.get_tracer().start_as_current_span(
        __name__, kind=SpanKind.SERVER
    ) as span:
        try:
            # Ability to add extras at runtime (i.e. after parsing some parameters)
            config.add_extras({"some_id": "some_id_value"})
            span.set_attributes(config.get_extras())

            # Running a method which in turn runs use_span will create a nested span
            # and will inherit the logging configuration
            run_method()
        except Exception as e:
                # Exceptions will be logged as an exception in the span
                span_record_exception(e, span)
                sys.exit(4)
```
