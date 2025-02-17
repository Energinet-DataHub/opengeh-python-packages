from typing import Any, Callable, Dict, Tuple

from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import start_span


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
