from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# These imports are the parts of the Scenario Testing framework that we want to test.
from geh_common.testing.dataframes import assert_dataframes_and_schemas, read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases, get_then_names

_schema = T.StructType().add("a", "string").add("b", "string").add("c", "integer")
_schema2 = T.StructType(
    [
        T.StructField("string", T.StringType()),
        T.StructField("boolean", T.BooleanType()),
        T.StructField("integer", T.IntegerType()),
    ]
)


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest):
    """Very simple fixture where all expected data matches the input data."""

    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    actual_df = read_csv(
        spark,
        scenario_path + "/when/input.csv",
        _schema,
    )
    actual2_df = read_csv(
        spark,
        scenario_path + "/when/input2.csv",
        _schema2,
    )
    # Return test cases.
    # A test case must exist for each CSV file in the `then` folder.
    return TestCases(
        [
            TestCase(f"{scenario_path}/then/output.csv", actual_df),
            TestCase(f"{scenario_path}/then/output2.csv", actual2_df),
            TestCase(f"{scenario_path}/then/some_folder/some_output.csv", actual_df),
        ],
    )


@pytest.mark.parametrize("test_case_name", get_then_names())
def test_scenario_testing(test_case_name, test_cases: TestCases):
    """Verify that all the parts of `testcommon.scenario_testing` work together."""
    test_case = test_cases[test_case_name]
    assert_dataframes_and_schemas(actual=test_case.actual, expected=test_case.expected)
