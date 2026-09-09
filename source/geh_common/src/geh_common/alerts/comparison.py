from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def find_records_missing_from_target(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    source_key_columns: list[str],
    target_key_columns: list[str],
) -> DataFrame:
    """Find records in the source dataframe that are missing from the target dataframe based on the specified key columns."""
    _validate_key_columns(source_key_columns, target_key_columns)
    source = source_dataframe.alias("source")
    target_keys = target_dataframe.select(*target_key_columns).distinct().alias("target")
    matching_key = _matching_key(source_key_columns, target_key_columns)
    return source.join(target_keys, matching_key, "left_anti").select("source.*")


def find_records_with_mismatched_values(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    source_key_columns: list[str],
    target_key_columns: list[str],
    comparison_columns: tuple[tuple[str, str], ...],
) -> DataFrame:
    """Return one compact row for each value that differs between matching records."""
    _validate_key_columns(source_key_columns, target_key_columns)
    if not comparison_columns:
        raise ValueError("comparison_columns must contain at least one (source_column, target_column) pair")

    source = source_dataframe.alias("source")
    target = target_dataframe.alias("target")
    matching_key = _matching_key(source_key_columns, target_key_columns)
    differences = [
        F.when(
            ~F.col(f"source.{source_column}").eqNullSafe(F.col(f"target.{target_column}")),
            F.struct(
                F.lit(source_column).alias("source_column"),
                F.lit(target_column).alias("target_column"),
                F.col(f"source.{source_column}").cast("string").alias("source_value"),
                F.col(f"target.{target_column}").cast("string").alias("target_value"),
            ),
        )
        for source_column, target_column in comparison_columns
    ]

    return (
        source.join(target, matching_key, "inner")
        .select(
            _record_key(source_key_columns),
            F.explode(F.array(*differences)).alias("difference"),
        )
        .where(F.col("difference").isNotNull())
        .select("record_key", "difference.*")
    )


def _validate_key_columns(source_key_columns: list[str], target_key_columns: list[str]) -> None:
    if not source_key_columns or not target_key_columns:
        raise ValueError("source_key_columns and target_key_columns must contain at least one column")
    if len(source_key_columns) != len(target_key_columns):
        raise ValueError("source_key_columns and target_key_columns must contain the same number of columns")


def _matching_key(source_key_columns: list[str], target_key_columns: list[str]) -> F.Column:
    comparisons = [
        F.col(f"source.{source_column}") == F.col(f"target.{target_column}")
        for source_column, target_column in zip(source_key_columns, target_key_columns, strict=True)
    ]
    return reduce(lambda left, right: left & right, comparisons)


def _record_key(source_key_columns: list[str]) -> F.Column:
    if len(source_key_columns) == 1:
        return F.col(f"source.{source_key_columns[0]}").cast("string").alias("record_key")

    return F.to_json(
        F.struct(*(F.col(f"source.{column}").alias(column) for column in source_key_columns)),
        {"ignoreNullFields": "false"},
    ).alias("record_key")
