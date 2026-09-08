from dataclasses import dataclass
from enum import StrEnum
from typing import Sequence

from pyspark.sql import DataFrame

from .comparison import find_records_missing_from_target, find_records_with_mismatched_values
from .email_sender import send_comparison_alert


class ComparisonType(StrEnum):
    MISSING_RECORDS = "missing_records"
    VALUES = "values"


@dataclass(frozen=True)
class ComparisonResult:
    source_dataframe: DataFrame
    source_key_column: str
    target_dataframe: DataFrame
    target_key_column: str
    comparison_type: ComparisonType
    heading: str
    summary: str
    comparison_columns: tuple[tuple[str, str], ...] | None = None

    def __post_init__(self) -> None:
        """Validate configuration required by the selected comparison type."""
        if self.comparison_type == ComparisonType.VALUES and not self.comparison_columns:
            raise ValueError("comparison_columns must be provided for a values comparison")


def compare_and_report(
    subject: str,
    comparisons: Sequence[ComparisonResult],
    *,
    print_results: bool = True,
    send_email: bool = True,
    max_displayed_rows: int = 20,
) -> None:
    """Run comparisons and print or email a summary of non-empty results."""
    if max_displayed_rows < 1:
        raise ValueError("max_displayed_rows must be at least 1")

    result_counts = []
    for comparison in comparisons:
        dataframe = _compare(comparison)
        dataframe.persist()
        try:
            count = dataframe.count()
            result_counts.append(count)
            if count == 0:
                continue

            if print_results:
                print(f"\n{comparison.heading}")  # noqa: T201
                print(f"{count} {comparison.summary}")  # noqa: T201
                dataframe.show(n=max_displayed_rows, truncate=50)
        finally:
            dataframe.unpersist()

    if not any(result_counts):
        return

    body = "\n".join(
        f"{count} {comparison.summary}" for comparison, count in zip(comparisons, result_counts, strict=True)
    )
    if send_email:
        send_comparison_alert(subject, body)


def _compare(comparison: ComparisonResult) -> DataFrame:
    if comparison.comparison_type == ComparisonType.MISSING_RECORDS:
        return find_records_missing_from_target(
            comparison.source_dataframe,
            comparison.target_dataframe,
            comparison.source_key_column,
            comparison.target_key_column,
        )

    if comparison.comparison_type == ComparisonType.VALUES:
        assert comparison.comparison_columns is not None
        return find_records_with_mismatched_values(
            comparison.source_dataframe,
            comparison.target_dataframe,
            comparison.source_key_column,
            comparison.target_key_column,
            comparison.comparison_columns,
        )

    raise ValueError(f"Unsupported comparison type: {comparison.comparison_type}")
