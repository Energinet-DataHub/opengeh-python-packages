import inspect
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper

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


def testing(selector: Callable[..., "DataFrame | DataFrameWrapper"] | None = None) -> Callable[..., Any]:
    """Use this decorator to log data frame return value of functions.

    Args:
        selector: A function to select the `pyspark.sql.DataFrame` or `DataFrameWrapper` from the function result

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

    def decorator(func) -> Callable[..., Any]:
        """Decorate to log data frame return value of functions.

        Provide function `selector` to select the `pyspark.sql.DataFrame` or `DataFrameWrapper` from the function result.
        Apply multiple decorators to log multiple data frames from a composite result.
        """

        def wrapper(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            if not TESTING:
                return result

            data = selector(result) if selector else result

            if isinstance(data, DataFrame):
                _log_dataframe(data, func.__name__)

            # Import DataFrameWrapper locally to prevent circular import error
            from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper

            if isinstance(data, DataFrameWrapper):
                _log_dataframe(data.df, func.__name__)

            return result

        return wrapper

    return decorator


def _log_dataframe(df: DataFrame, function_name: str) -> None:
    caller = inspect.stack()[2]
    file_name = Path(caller.filename).name
    print(f">>>In {file_name}:{caller.lineno}: {function_name}(...) returned:")  # noqa: T201

    df.show(truncate=False, n=ROWS)
