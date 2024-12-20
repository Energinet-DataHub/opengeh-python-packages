from testcommon.etl.TestCases import TestCase, TestCases
from pyspark.sql import types as T

from tests.etl.constants import ETL_TEST_DATA


def test_TestCases(spark):
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.StringType(), True),
            T.StructField("c", T.BooleanType(), True),
        ]
    )
    df = spark.createDataFrame([(1, "a", True)], schema=schema)

    path = (ETL_TEST_DATA / "no_array.csv").as_posix()
    cases = TestCases([TestCase(path, df)])

    assert list(cases.keys()) == [path]
    assert cases[path].expected.schema == schema
