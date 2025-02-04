from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pytest

# These imports are the parts of the ETL framework that we want to test.
from testcommon.dataframes import (
    read_csv,
    assert_dataframes_and_schemas,
)
from testcommon.scenario_testing import get_then_names, TestCase, TestCases

_schema = (
    StructType().add("a", "string").add("b", "string").add("c", "integer")
)


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest):
    """Very simple fixture where all expected data matches the input data."""

    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    input_df = read_csv(
        spark,
        scenario_path + "/when/input.csv",
        _schema,
    )

    # No need to transform the input to test the ETL framework.
    actual_df = input_df

    # Return test cases.
    # A test case must exist for each CSV file in the `then` folder.
    return TestCases(
        [
            TestCase(f"{scenario_path}/then/output.csv", actual_df),
            TestCase(
                f"{scenario_path}/then/some_folder/some_output.csv", actual_df
            ),
        ]
    )


@pytest.mark.parametrize("test_case_name", get_then_names())
def test_etl(test_case_name, test_cases: TestCases):
    """Verify that all the parts of `testcommon.etl` work together."""

    test_case = test_cases[test_case_name]
    assert_dataframes_and_schemas(
        actual=test_case.actual, expected=test_case.expected
    )
