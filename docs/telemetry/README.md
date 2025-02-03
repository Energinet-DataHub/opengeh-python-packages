# Telemetry

Shared telemetry logging library for Python. Useful for creating spans in Azure.

The `telemetry_logging` package in this module provides utilities for logging telemetry data. It includes functions and classes to facilitate the collection, formatting, and storage of telemetry information, which can be used for monitoring application performance, diagnosing issues, and gathering usage statistics. The package aims to offer a flexible and extensible framework for integrating telemetry logging into various parts of an application.

## Usage

This code snippet demonstrates the initial configuration of logging using the telemetry_logging library. It includes the following steps:

1) Import necessary modules from the telemetry_logging library.
2) Define a function run_method decorated with @use_span(), which creates a nested span for tracing. Inside this function, a logger instance is created and used to log an informational message.
3) Define an entry_point method that configures the logging setup using the configure_logging function. It starts the initial span, which following nested method calls can use, i.e. the run_method defined previously will inherit the same settings.

```python
import opengeh_common.telemetry.logging_configuration as config
from opengeh_common.telemetry.span_recording import span_record_exception
from opengeh_common.telemetry import Logger, use_span

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
