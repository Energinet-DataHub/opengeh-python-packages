import sys
from typing import Callable, Any, Tuple, Dict
from telemetry_logging.logging_configuration import start_span, get_tracer, get_logging_configured
from telemetry_logging.span_recording import span_record_exception
from telemetry_logging import Logger
from opentelemetry.trace import SpanKind

def use_span(name: str | None = None) -> Callable[..., Any]:
    """
    Decorator for creating spans.
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

def start_trace(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator that checks if the logging_configuration.configure_logging method has been called prior to starting the
    trace
    Provides an initial span that can be retrieved through span
    """
    def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> Any:
        # Retrieve the logging_configured flag from logging_configuration to see if configure_logging() has been called
        logging_configured = get_logging_configured()
        if logging_configured:
            # Start the tracer span using the current function name
            with get_tracer().start_as_current_span(func.__name__, kind=SpanKind.SERVER) as initial_span:
                # Log the start of the function execution
                log = Logger(func.__name__)
                log.info(f"Started executing function: {func.__name__}")

                # Add the span and message to kwargs in order to be able to pass it back to func
                # (if the initial span is of interest)
                kwargs['initial_span'] = initial_span

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
        else:
            raise NotImplementedError("Logging has not been configured before use of decorator.")

    return wrapper
