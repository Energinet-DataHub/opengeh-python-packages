"""Represents a test case for a specific scenario.

The expected data is lazy-loaded from a CSV file as it may not exist in all scenarios.
"""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame

from geh_common.testing.dataframes.read_csv import read_csv


@dataclass
class TestCase:
    __test__ = False

    def __init__(self, expected_csv_path: str, actual: DataFrame, sep: str = ";") -> None:
        if not isinstance(expected_csv_path, str):
            raise TypeError("expected_csv_path must be a string")
        self.expected_csv_path: str = expected_csv_path
        self.actual: DataFrame = actual
        self.sep: str = sep

    @property
    def expected(self) -> DataFrame:
        """The expected DataFrame."""
        expected = read_csv(
            self.actual.sparkSession,
            self.expected_csv_path,
            self.actual.schema,
            self.sep,
        )
        headers = [
            h for h in Path(self.expected_csv_path).read_text().splitlines()[0].split(";") if not h.startswith("#")
        ]
        diff_cols = [h for h in headers if h not in expected.columns]
        if diff_cols:
            raise ValueError(
                f"Expected {len(expected.columns)} columns in the CSV file, but got {len(headers)}. {diff_cols} should not be in the CSV file."
            )
        return expected


@dataclass
class TestCases(dict):
    __test__ = False

    def __init__(self, test_cases: list[TestCase]) -> None:
        super().__init__()
        for test_case in test_cases:
            test_case_name = _get_then_name(test_case.expected_csv_path)
            self[test_case_name] = test_case

    # Overload to support type hint of return object.
    def __getitem__(self, key: str) -> TestCase:
        """Get the test.

        The key is the name of the csv-file in the `/then` folder of the scenario.
        The name is the path relative to the `/then` folder, excluding the file extension.
        """
        return super().__getitem__(key)


def _get_then_name(path: str | Path) -> str:
    """Get the path of a file relative to the `/then` folder, excluding the file extension."""
    path = Path(path)
    for parent in path.parents:
        if parent.name == "then":
            return str(path.relative_to(parent).with_suffix(""))
    raise ValueError("The path does not contain a 'then' folder")
