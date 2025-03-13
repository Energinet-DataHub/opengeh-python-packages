import inspect
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame

TESTING = False
ROWS = 50


def configure_testing(is_testing: bool = TESTING, rows: int = ROWS) -> None:
    """Configure the testing environment.

    Use this function to configure the testing environment.
    The `rows` parameter sets the number of rows to display when logging data frames.
    """
    global TESTING, ROWS
    TESTING = is_testing
    ROWS = rows


def testing() -> Callable[..., Any]:
    """Use this decorator to log data frame return value of functions.

    Example:
    ```python
    from geh_common.testing.dataframes import configure_testing, testing


    @testing()
    def my_function(spark: SparkSession) -> DataFrame:
        return spark.read.parquet("data.parquet")


    if __name__ == "__main__":
        configure_testing(True)
        my_function()
    ```
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if not TESTING:
                return result

            if isinstance(result, DataFrame):
                _log_dataframe(result, func.__name__)
            return result

        return wrapper

    return decorator


def _log_dataframe(df: DataFrame, function_name: str) -> None:
    caller = inspect.stack()[2]
    file_name = Path(caller.filename).name
    print(f">>>In {file_name}:{caller.lineno}: {function_name}(...) returned:")  # noqa: T201

    df.show(truncate=False, n=ROWS)
