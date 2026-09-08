from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql import DataFrame

from geh_common.alerts import ComparisonResult, ComparisonType, compare_and_report


@patch("geh_common.alerts.comparison_report.send_comparison_alert")
@patch("geh_common.alerts.comparison_report.find_records_with_mismatched_values")
def test_compare_and_report__with_values__compares_and_sends_summary_without_printing(
    find_mismatched: MagicMock,
    send_alert: MagicMock,
) -> None:
    source = MagicMock(spec=DataFrame)
    target = MagicMock(spec=DataFrame)
    dataframe = MagicMock(spec=DataFrame)
    dataframe.count.return_value = 2
    find_mismatched.return_value = dataframe
    comparison = ComparisonResult(
        source_dataframe=source,
        source_key_column="source_id",
        target_dataframe=target,
        target_key_column="target_id",
        comparison_type=ComparisonType.VALUES,
        comparison_columns=(("source_value", "target_value"),),
        heading="Mismatched records",
        summary="field values differ",
    )

    compare_and_report("Comparison failed", [comparison], print_results=False)

    find_mismatched.assert_called_once_with(
        source,
        target,
        "source_id",
        "target_id",
        (("source_value", "target_value"),),
    )
    dataframe.persist.assert_called_once_with()
    dataframe.show.assert_not_called()
    dataframe.unpersist.assert_called_once_with()
    send_alert.assert_called_once_with("Comparison failed", "2 field values differ")


@patch("geh_common.alerts.comparison_report.send_comparison_alert")
@patch("geh_common.alerts.comparison_report.find_records_missing_from_target")
def test_compare_and_report__with_multiple_missing_record_comparisons__combines_summaries(
    find_missing: MagicMock,
    send_alert: MagicMock,
) -> None:
    first_source = MagicMock(spec=DataFrame)
    first_target = MagicMock(spec=DataFrame)
    missing_from_target = MagicMock(spec=DataFrame)
    missing_from_target.count.return_value = 2
    missing_from_source = MagicMock(spec=DataFrame)
    missing_from_source.count.return_value = 1
    find_missing.side_effect = [missing_from_target, missing_from_source]
    comparisons = [
        ComparisonResult(
            first_source,
            "id",
            first_target,
            "id",
            ComparisonType.MISSING_RECORDS,
            "Missing from target",
            "records are missing from target",
        ),
        ComparisonResult(
            first_target,
            "id",
            first_source,
            "id",
            ComparisonType.MISSING_RECORDS,
            "Missing from source",
            "records are missing from source",
        ),
    ]

    compare_and_report("Completeness check failed", comparisons, print_results=False)

    assert find_missing.call_count == 2
    send_alert.assert_called_once_with(
        "Completeness check failed",
        "2 records are missing from target\n1 records are missing from source",
    )


@patch("geh_common.alerts.comparison_report.send_comparison_alert")
@patch("geh_common.alerts.comparison_report.find_records_missing_from_target")
def test_compare_and_report__with_empty_results__does_not_send_alert(
    find_missing: MagicMock,
    send_alert: MagicMock,
) -> None:
    dataframe = MagicMock(spec=DataFrame)
    dataframe.count.return_value = 0
    find_missing.return_value = dataframe
    comparison = ComparisonResult(
        MagicMock(spec=DataFrame),
        "id",
        MagicMock(spec=DataFrame),
        "id",
        ComparisonType.MISSING_RECORDS,
        "Missing records",
        "records are missing",
    )

    compare_and_report("Comparison failed", [comparison])

    dataframe.show.assert_not_called()
    dataframe.unpersist.assert_called_once_with()
    send_alert.assert_not_called()


@patch("builtins.print")
@patch("geh_common.alerts.comparison_report.send_comparison_alert")
@patch("geh_common.alerts.comparison_report.find_records_missing_from_target")
def test_compare_and_report__when_printing__shows_bounded_preview(
    find_missing: MagicMock,
    send_alert: MagicMock,
    print_output: MagicMock,
) -> None:
    dataframe = MagicMock(spec=DataFrame)
    dataframe.count.return_value = 25
    find_missing.return_value = dataframe
    comparison = ComparisonResult(
        MagicMock(spec=DataFrame),
        "id",
        MagicMock(spec=DataFrame),
        "id",
        ComparisonType.MISSING_RECORDS,
        "Missing records",
        "records are missing",
    )

    compare_and_report(
        "Comparison failed",
        [comparison],
        send_email=False,
        max_displayed_rows=10,
    )

    assert print_output.call_args_list == [
        call("\nMissing records"),
        call("25 records are missing"),
    ]
    dataframe.show.assert_called_once_with(n=10, truncate=50)
    send_alert.assert_not_called()


def test_comparison_result__with_values_and_no_comparison_columns__raises_error() -> None:
    with pytest.raises(ValueError, match="comparison_columns must be provided"):
        ComparisonResult(
            MagicMock(spec=DataFrame),
            "id",
            MagicMock(spec=DataFrame),
            "id",
            ComparisonType.VALUES,
            "Mismatched records",
            "records have mismatched values",
        )


def test_compare_and_report__with_invalid_display_limit__raises_error() -> None:
    with pytest.raises(ValueError, match="max_displayed_rows must be at least 1"):
        compare_and_report("Comparison failed", [], max_displayed_rows=0)
