from typing import Callable, Any, Tuple, Dict
from telemetry_logging.logging_configuration import start_span
from .logging_configuration import get_logging_configured, get_tracer # works with relative import, but not absolute (because telemetry_logging is an imported module from requirements.tx: opengeh-telemetry
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


def use_logging(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to add tracing for a function.
    Starts a tracing span before executing the function.
    """
    def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> Any:
        logging_configured = get_logging_configured()
        if logging_configured:
            # Start the tracer span using the current function name
            with get_tracer().start_as_current_span(func.__name__, kind=SpanKind.SERVER):
                # Log the start of the function execution
                log = Logger(func.__name__)
                log.info(f"Started executing function: {func.__name__}")

                # Call the original function
                return func(*args, **kwargs)
        else:
            raise NotImplementedError("Logging has not been configured before use of decorator.")

    return wrapper
