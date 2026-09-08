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
    """Find records in the source dataframe that have mismatched values in the target dataframe based on the specified key columns and comparison columns."""
    source = source_dataframe.alias("source")
    target = target_dataframe.alias("target")
    matching_key = F.col(f"source.{source_key_column}") == F.col(f"target.{target_key_column}")
    matching_values = F.lit(True)
    for source_column, target_column in comparison_columns:
        matching_values &= F.col(f"source.{source_column}").eqNullSafe(F.col(f"target.{target_column}"))

    return source.join(target, matching_key, "inner").where(~matching_values).select("source.*")
