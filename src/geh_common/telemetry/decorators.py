import sys
from typing import Any, Callable, Dict, Tuple

from opentelemetry.trace import SpanKind

from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import (
    get_is_instrumented,
    get_tracer,
    start_span,
)
from geh_common.telemetry.span_recording import span_record_exception


def use_span(name: str | None = None) -> Callable[..., Any]:
    """Create a decorator that starts a span before executing the decorated function.

    If name is not provided then the __qualname__ of the decorated function is used.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> Any:
            name_to_use = name or func.__qualname__
            with start_span(name_to_use):
                log = Logger(name_to_use)
                log.info(f"Started executing function: {name_to_use}")
                return func(*args, **kwargs)

        return wrapper

    return decorator


def start_trace() -> Callable[..., Any]:
    """Start an OpenTelemetry span for tracing function execution.

    This decorator sets up a tracer (if not initialized) and starts a new span before executing
    the decorated function. If no active trace exists, a new one is created. It also ensures logging
    is configured, raising an error if not.

    Returns:
        Callable[..., Any]: A wrapped function that starts a span before execution.

    Raises:
        NotImplementedError: If logging is not configured before using the decorator.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> Any:  # TODO: Add comment about this
            # Retrieve the logging_configured flag from logging_configuration to see if configure_logging() has been called
            logging_configured = get_is_instrumented()
            name_to_use = func.__name__

            if not logging_configured:
                raise NotImplementedError("Logging has not been configured before use of decorator.")

            # Start the tracer span using the current function name
            with get_tracer().start_as_current_span(name_to_use, kind=SpanKind.SERVER) as initial_span:
                # Log the start of the function execution
                log = Logger(name_to_use)
                log.info(f"Started executing function: {name_to_use}")

                # Call the original function with both positional and keyword arguments
                try:
                    return func(*args, **kwargs)
                except SystemExit as e:
                    if e.code != 0:
                        span_record_exception(e, initial_span)
                    sys.exit(e.code)

                except Exception as e:
                    span_record_exception(e, initial_span)
                    sys.exit(4)

        return wrapper

    return decorator
