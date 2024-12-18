from pyspark.sql import types as T
from pyspark.testing.utils import assertDataFrameEqual
from tests.constants import TEST_DATA_DIR
from testcommon.read_csv import read_csv

CSV_EXAMPLES = TEST_DATA_DIR / "csv_examples"


def test_no_array(spark):
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.StringType(), True),
            T.StructField("c", T.BooleanType(), True),
        ]
    )

    path = CSV_EXAMPLES / "no_array.csv"
    df = read_csv(spark, str(path), schema, sep=";")
    assert df.schema == schema, "Schema does not match"

    test_df = spark.createDataFrame([(1, "a", True)], schema=schema)

    assertDataFrameEqual(df, test_df)

    collected = df.collect()
    assert collected[0].a == 1, f"a should be 1, got {collected[0].a}"
    assert collected[0].b == "a", f"b should be a, got {collected[0].b}"
    assert collected[0].c is True, f"c should be True, got {collected[0].c}"


def test_with_array_string(spark):
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.StringType(), True),
            T.StructField("c", T.BooleanType(), True),
            T.StructField("d", T.ArrayType(T.StringType()), True),
            T.StructField("e", T.ArrayType(T.StringType(), containsNull=False), True),
        ]
    )

    path = CSV_EXAMPLES / "with_array_string.csv"
    df = read_csv(spark, str(path), schema, sep=";")
    assert df.schema == schema, "Schema does not match"

    test_df = spark.createDataFrame(
        [(1, "a", True, ["a", "b", None], ["a", "b", "c"])], schema=schema
    )

    assertDataFrameEqual(df, test_df)

    collected = df.collect()
    assert collected[0].a == 1, f"a should be 1, got {collected[0].a}"
    assert collected[0].b == "a", f"b should be a, got {collected[0].b}"
    assert collected[0].c is True, f"c should be True, got {collected[0].c}"
    assert collected[0].d == [
        "a",
        "b",
        None,
    ], collected
    assert collected[0].e == [
        "a",
        "b",
        "c",
    ], f"e should be ['a', 'b', 'c'], got {collected[0].e}"
