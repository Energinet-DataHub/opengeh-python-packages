import inspect
import sys
from typing import Callable, Any, Tuple, Dict
from geh_common.telemetry.logging_configuration import (
    start_span,
    get_tracer,
    get_logging_configured,
)
from geh_common.telemetry.span_recording import span_record_exception
from opentelemetry.trace import SpanKind
from geh_common.telemetry.logger import Logger

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


def start_trace(initial_span_name: str | None = None) -> Callable[..., Any]:
    """
    Decorator that checks if the logging_configuration.configure_logging method has been called prior to starting the
    trace
    Provides an initial span based on the provided initial_span_name parameter
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func_signature = inspect.signature(
            func
        )  # Get function signature of function applying the decorator
        accepts_initial_span = (
            "initial_span" in func_signature.parameters
        )  # Check if initial_span is in function params

        def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> Any:
            # Retrieve the logging_configured flag from logging_configuration to see if configure_logging() has been called
            logging_configured = get_logging_configured()
            name_to_use = initial_span_name or func.__name__
            if logging_configured:
                # Start the tracer span using the current function name
                with get_tracer().start_as_current_span(
                    name_to_use, kind=SpanKind.SERVER
                ) as initial_span:
                    # Log the start of the function execution
                    log = Logger(name_to_use)
                    log.info(f"Started executing function: {name_to_use}")

                    # Add the span and message to kwargs in order to be able to pass it back to func, only if the function accepts it
                    if accepts_initial_span:
                        kwargs["initial_span"] = initial_span

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
                raise NotImplementedError(
                    "Logging has not been configured before use of decorator."
                )

        return wrapper

    return decorator
