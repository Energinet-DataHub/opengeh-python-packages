from dataclasses import dataclass

from pyspark.sql import DataFrame
from testcommon.etl import read_csv


@dataclass
class TestCase:
    """
    Represents a test case for a specific scenario.
    The expected data is lazy-loaded from a CSV file as it may not exist in all scenarios.
    """

    def __init__(self, expected_csv_path: str, actual: DataFrame) -> None:
        if not isinstance(expected_csv_path, str):
            raise TypeError("expected_csv_path must be a string")
        self.expected_csv_path: str = expected_csv_path
        self.actual: DataFrame = actual

    @property
    def expected(self) -> DataFrame:
        return read_csv(
            self.actual.sparkSession, self.expected_csv_path, self.actual.schema
        )


@dataclass
class TestCases(dict):
    def __init__(self, test_cases: list[TestCase]) -> None:
        super().__init__()
        for test_case in test_cases:
            self[test_case.expected_csv_path] = test_case
