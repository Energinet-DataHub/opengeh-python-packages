from pyspark.sql import SparkSession

from geh_common.alerts.comparison import find_records_missing_from_target, find_records_with_mismatched_values


def test_find_records_missing_from_target__with_single_key__returns_missing_records(spark: SparkSession) -> None:
    source = spark.createDataFrame([(1, "first"), (2, "second")], ["id", "value"])
    target = spark.createDataFrame([(1,)], ["id"])

    actual = find_records_missing_from_target(source, target, ["id"], ["id"])

    assert actual.collect() == [(2, "second")]


def test_find_records_missing_from_target__with_multiple_keys__matches_all_key_columns(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [(1, "DK1", "first"), (1, "DK2", "second"), (2, "DK1", "third")],
        ["id", "source_area", "value"],
    )
    target = spark.createDataFrame([(1, "DK1"), (2, "DK2")], ["target_id", "target_area"])

    actual = find_records_missing_from_target(
        source,
        target,
        ["id", "source_area"],
        ["target_id", "target_area"],
    )

    assert set(actual.collect()) == {(1, "DK2", "second"), (2, "DK1", "third")}


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
        ["id"],
        ["id"],
        (("quantity", "quantity"), ("quality", "quality"), ("nullable_value", "nullable_value")),
    )

    assert actual.columns == ["record_key", "source_column", "target_column", "source_value", "target_value"]
    assert set(actual.collect()) == {
        ("2", "quantity", "quantity", "20", "25"),
        ("2", "quality", "quality", "changed", "expected"),
    }


def test_find_records_with_mismatched_values__with_multiple_keys__matches_all_key_columns(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [(1, "DK1", 10), (1, "DK2", 20)],
        ["id", "source_area", "quantity"],
    )
    target = spark.createDataFrame(
        [(1, "DK1", 15), (1, "DK2", 20)],
        ["target_id", "target_area", "quantity"],
    )

    actual = find_records_with_mismatched_values(
        source,
        target,
        ["id", "source_area"],
        ["target_id", "target_area"],
        (("quantity", "quantity"),),
    )

    assert actual.collect() == [
        ('{"id":1,"source_area":"DK1"}', "quantity", "quantity", "10", "15"),
    ]
