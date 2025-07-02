import time
from typing import Any, Callable

from geh_common.telemetry.logger import Logger

logger = Logger(__name__)


def retry_policy(delay=2, retries=4):
    """Apply retry policy with exponential backoff to a function or method. This will enable retries upon exception.

    Based on https://medium.com/@suryasekhar/exponential-backoff-decorator-in-python-26ddf783aea0.

    Args:
        delay (int, optional): The initial delay in seconds before retrying.
        retries (int, optional): The maximum number of retry attempts.

    Returns:
        function: The decorated function with retry policy applied.

    Raises:
        Exception: Re-raises the last exception encountered if all retries are exhausted.

    Example:
        @retry_policy(delay=1, retries=5)
        def unreliable_function():
            # function implementation
    """

    def decorator(func) -> Callable[..., Any | None]:
        def wrapper(*args, **kwargs) -> Any | None:
            current_retry = 0
            current_delay = delay
            while current_retry < retries + 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if current_retry >= retries:
                        raise RuntimeError(
                            f"Failed to execute function '{func.__name__}'. Giving up after {current_retry} attempts."
                        ) from e
                    logger.warning(
                        f"Failed to execute function '{func.__name__}'. Retrying in {current_delay} seconds..."
                    )
                    time.sleep(current_delay)
                    current_delay *= 2
                    current_retry += 1

        return wrapper

    return decorator
