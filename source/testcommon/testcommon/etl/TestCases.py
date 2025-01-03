from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from testcommon.dataframes import read_csv


@dataclass
class TestCase:
    """
    Represents a test case for a specific scenario.
    The expected data is lazy-loaded from a CSV file as it may not exist in all scenarios.
    """

    __test__ = False

    def __init__(self, expected_csv_path: str, actual: DataFrame) -> None:
        if not isinstance(expected_csv_path, str):
            raise TypeError("expected_csv_path must be a string")
        self.expected_csv_path: str = expected_csv_path
        self.actual: DataFrame = actual

    @property
    def expected(self) -> DataFrame:
        """The expected DataFrame."""
        return read_csv(
            self.actual.sparkSession,
            self.expected_csv_path,
            self.actual.schema,
        )


@dataclass
class TestCases(dict):
    """
    A dictionary of test cases, where the keys are the name of the csv-files
    in the `/then` folder of the scenario. The names are the file paths relative
    to the `/then` folder, excluding the file extension.
    """

    __test__ = False

    def __init__(self, test_cases: list[TestCase]) -> None:
        super().__init__()
        for test_case in test_cases:
            test_case_name = _get_then_name(test_case.expected_csv_path)
            self[test_case_name] = test_case

    # Overload to support type hint of return object.
    def __getitem__(self, key: str) -> TestCase:
        """
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
