from pyspark.sql import SparkSession

from geh_common.alerts.comparison import find_records_with_mismatched_values


def test_find_records_with_mismatched_values__returns_one_row_per_difference(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            (1, 10, "valid", None),
            (2, 20, "changed", "same"),
        ],
        ["id", "quantity", "quality", "nullable_value"],
    )
    target = spark.createDataFrame(
        [
            (1, 10, "valid", None),
            (2, 25, "expected", "same"),
        ],
        ["id", "quantity", "quality", "nullable_value"],
    )

    actual = find_records_with_mismatched_values(
        source,
        target,
        "id",
        "id",
        (("quantity", "quantity"), ("quality", "quality"), ("nullable_value", "nullable_value")),
    )

    assert actual.columns == ["record_key", "source_column", "target_column", "source_value", "target_value"]
    assert set(actual.collect()) == {
        ("2", "quantity", "quantity", "20", "25"),
        ("2", "quality", "quality", "changed", "expected"),
    }
