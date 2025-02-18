# Telemetry

Shared telemetry logging library for Python. Useful for creating spans in Azure.

The `telemetry_logging` package in this module provides utilities for logging telemetry data. It includes functions and classes to facilitate the collection, formatting, and storage of telemetry information, which can be used for monitoring application performance, diagnosing issues, and gathering usage statistics. The package aims to offer a flexible and extensible framework for integrating telemetry logging into various parts of an application.

## Usage

This code snippet demonstrates the initial configuration of logging using the telemetry_logging library. It includes the following steps:

1. **Configure Logging**  
   - Instantiate a `LoggingSettings` object, which automatically reads settings from CLI arguments or environment variables.  
   - Set up additional custom dimensions (extras) that will appear as properties in Azure Application Insights.  
   - Call `configure_logging()` using the `LoggingSettings` object and extras to initialize the logging configuration

2. **Create and Start Tracing**  
   - Use the `@start_trace()` decorator on `orchestrate_business_logic()`, which ensures that an OpenTelemetry tracer is created if one does not already exist.  
   - The decorator automatically starts an initial span named after the function (`orchestrate_business_logic`).  

3. **Use Nested Spans for Tracing**  
   - Define a function `run_method()` decorated with `@use_span()`, which creates a nested span within `orchestrate_business_logic`.  
   - Inside each function, a `Logger` instance can be created, inheriting settings and extras from the span.  
   - Log messages are recorded, capturing relevant execution details.  


```python
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging
from geh_common.telemetry.span_recording import span_record_exception
from geh_common.telemetry import Logger, use_span


def entry_point() -> None:
    # Setup the configuration for logging, by instantiating a LoggingSettings object. This object reads settings from CLI / env vars
    # Extras will be shown as custom dimensions in application insights under properties
    # Call configuration of logging using the logging_settings object and extras
    logging_settings = LoggingSettings(subsystem="measurements", cloud_role_name="some-cloud-name")
    some_extras = {"some_id": "some_id_value"}
    configure_logging(logging_settings=logging_settings, extras=some_extras)
    orchestrate_business_logic()


# Use the start_trace() decorator, which will create an OpenTelemetry tracer if it has not yet been created.
# Then, it creates an initial span using the decorated function's name "orchestrate_business_logic".
# Create a logger (will inherit extras and name from the span and logging configuration setup initially).
@start_trace()
def orchestrate_business_logic() -> None:
    spark = initialize_spark()
    log = Logger(__name__)
    log.info("Inside method orchestrate_business_logic - I will now orchestrate the execution of some other code")
    run_method()


# run_method will create a nested span named "run_method" within the initial span "orchestrate_business_logic".
@use_span()
def run_method() -> None:
    # Create a logger (will inherit extras and name from the span and logging configuration setup initially)
    log = Logger(__name__)
    log.info("Inside method run_method")