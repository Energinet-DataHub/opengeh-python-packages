from pyspark.sql import types as T

from geh_common.testing.scenario_testing.TestCases import TestCase, TestCases
from tests.testing.unit.scenario_testing.constants import SCENARIO_TESTING_DATA


def test_test_cases(spark):
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.StringType(), True),
            T.StructField("c", T.BooleanType(), True),
        ]
    )
    df = spark.createDataFrame([(1, "a", True)], schema=schema)

    path = (SCENARIO_TESTING_DATA / "then" / "no_array.csv").as_posix()
    key = "no_array"  # The path relative to the `then` folder, excluding the file extension
    cases = TestCases([TestCase(path, df)])

    assert list(cases.keys()) == [key]
    assert cases[key].expected.schema == schema
