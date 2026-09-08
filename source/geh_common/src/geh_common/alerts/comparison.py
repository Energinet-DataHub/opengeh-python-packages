import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def find_records_missing_from_target(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    source_key_column: str,
    target_key_column: str,
) -> DataFrame:
    """Find records in the source dataframe that are missing from the target dataframe based on the specified key columns."""
    target_keys = target_dataframe.select(target_key_column).distinct()
    return source_dataframe.join(
        target_keys,
        source_dataframe[source_key_column] == target_keys[target_key_column],
        "left_anti",
    )


def find_records_with_mismatched_values(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    source_key_column: str,
    target_key_column: str,
    comparison_columns: tuple[tuple[str, str], ...],
) -> DataFrame:
    """Return one compact row for each value that differs between matching records."""
    source = source_dataframe.alias("source")
    target = target_dataframe.alias("target")
    matching_key = F.col(f"source.{source_key_column}") == F.col(f"target.{target_key_column}")
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
            F.col(f"source.{source_key_column}").cast("string").alias("record_key"),
            F.explode(F.array(*differences)).alias("difference"),
        )
        .where(F.col("difference").isNotNull())
        .select("record_key", "difference.*")
    )
