from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pytest

# These imports are the parts of the ETL framework that we want to test.
from testcommon.etl import assert_dataframes, get_then_names, read_csv, TestCase, TestCases


_schema = StructType().add("a", "string").add("b", "string").add("c", "integer")

@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest):
    """Very simple fixture where all expected data matches the input data."""

    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    output_path = f"{scenario_path}/when/output.csv"
    output = read_csv(
        spark,
        output_path,
        _schema,
    )

    some_output_path = f"{scenario_path}/when/some_folder/some_output.csv"
    some_output = read_csv(
        spark,
        some_output_path,
        _schema,
    )

    # Return test cases. A test case must exist for each CSV file in the `then` folder.
    return TestCases([
        TestCase(output_path, output),
        TestCase(some_output_path, some_output)
    ])


@pytest.mark.parametrize("test_case_name", get_then_names())
def test_etl(test_case_name, test_cases: TestCases):
    """Verify that all the parts of `testcommon.etl` work together."""

    test_case = test_cases[test_case_name]
    assert_dataframes(actual=test_case.actual, expected=test_case.expected)
