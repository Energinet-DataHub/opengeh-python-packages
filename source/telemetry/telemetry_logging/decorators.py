from typing import Callable, Any, Tuple, Dict
from telemetry_logging.logging_configuration import start_span, get_tracer, get_logging_configured
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
    Decorator checks if the logging has been configured

    """
    def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> Any:
        logging_configured = get_logging_configured()
        if logging_configured:
            # Start the tracer span using the current function name
            with get_tracer().start_as_current_span(func.__name__, kind=SpanKind.SERVER) as span:
                # Log the start of the function execution
                log = Logger(func.__name__)
                log.info(f"Started executing function: {func.__name__}")

                # Add the span and message to kwargs
                kwargs['span'] = span

                # Call the original function with both positional and keyword arguments
                return func(*args, **kwargs)
        else:
            raise NotImplementedError("Logging has not been configured before use of decorator.")

    return wrapper
