import inspect
from typing import Tuple
from unittest.mock import patch

from pyspark.sql import DataFrame, SparkSession

from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from geh_common.testing.dataframes.logging import (
    configure_testing,
    testing,
)


def test_configure_testing():
    # Arrange
    is_testing = True
    rows = 100

    # Act
    configure_testing(is_testing, rows)

    # Assert
    from geh_common.testing.dataframes.logging import ROWS, TESTING

    assert TESTING == is_testing
    assert ROWS == rows


def test_testing_decorator_not_testing():
    # Arrange
    configure_testing(False)

    @testing()
    def dummy_function() -> str:
        return "test"

    # Act
    result = dummy_function()

    # Assert
    assert result == "test"


def test_testing_decorator_with_dataframe(spark: SparkSession) -> None:
    # Arrange
    configure_testing(True)
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    @testing()
    def dummy_function() -> DataFrame:
        return df

    with patch("geh_common.testing.dataframes.logging._log_dataframe") as mock_log_dataframe:
        # Act
        result = dummy_function()

        # Assert
        assert result == df
        mock_log_dataframe.assert_called_once()


def test_testing_decorator_without_dataframe():
    # Arrange
    configure_testing(True)

    @testing()
    def dummy_function() -> str:
        return "test"

    with patch("geh_common.testing.dataframes.logging._log_dataframe") as mock_log_dataframe:
        # Act
        result = dummy_function()

        # Assert
        assert result == "test"
        mock_log_dataframe.assert_not_called()


def test_log_dataframe(spark, capsys, monkeypatch, original_print):
    # Arrange
    monkeypatch.setattr("builtins.print", original_print)
    configure_testing(True)
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    @testing()
    def dummy_function() -> DataFrame:
        return df

    # The function invocation occurs X lines below current line.
    # This complexity is introduced to make the test more robust to other changes in the file.
    frame = inspect.currentframe()
    line_number = frame.f_lineno + 4 if frame else "error!"
    message = f">>>In test_logging.py:{line_number}: dummy_function(...) returned:"

    # Act
    dummy_function()

    # Assert
    captured = capsys.readouterr()
    assert message in captured.out
    assert "id" in captured.out
    assert "name" in captured.out
    assert "1" in captured.out
    assert "a" in captured.out


##################################


class MyDataFrameWrapper(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=df.schema)


def test_decorator_when_function_return_dataframewrapper(spark: SparkSession) -> None:
    # Arrange
    configure_testing(True)
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    wrapper = MyDataFrameWrapper(df)

    @testing()
    def dummy_function() -> DataFrameWrapper:
        return wrapper

    with patch("geh_common.testing.dataframes.logging._log_dataframe") as mock_log_dataframe:
        # Act
        dummy_function()

        # Assert
        mock_log_dataframe.assert_called_once()


def test_decorator_when_used_multiple_times(spark: SparkSession) -> None:
    # Arrange
    configure_testing(True)
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    wrapper = MyDataFrameWrapper(df)

    @testing(selector=lambda result: result[0])
    @testing(selector=lambda result: result[1])
    def dummy_function() -> Tuple[DataFrame, DataFrameWrapper]:
        return df, wrapper

    with patch("geh_common.testing.dataframes.logging._log_dataframe") as mock_log_dataframe:
        # Act
        dummy_function()

        # Assert
        assert mock_log_dataframe.call_count == 2
