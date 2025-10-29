from collections.abc import Set
from pathlib import Path

from geh_common.testing.covernator.commands import run_covernator
from geh_common.testing.covernator.models import CovernatorResults


def run_and_load_stats(base_path: Path, tmp_path: Path) -> tuple[CovernatorResults, dict]:
    """
    Runs covernator and returns the in-memory results and stats summary.
    Replaces legacy file-based JSON and CSV loading.
    """
    results: CovernatorResults = run_covernator(
        base_path=base_path,
        folder_to_save_files_in=tmp_path,
    )

    stats = {
        "total_cases": results.stats.total_cases,
        "total_scenarios": results.stats.total_scenarios,
        "total_groups": results.stats.total_groups,
        "logs": {
            "error": [{"message": e.message} for e in results.error_logs],
            "info": [{"message": i.message} for i in results.info_logs],
        },
    }
    return results, stats


def assert_log_messages(
    logs: dict,
    expected_errors: Set[str] | None = None,
    expected_infos: Set[str] | None = None,
) -> None:
    """Asserts the correctness of log messages."""
    if expected_errors is not None:
        actual_errors = {entry["message"] for entry in logs.get("error", [])}
        assert actual_errors == expected_errors, (
            f"Mismatch in ERROR logs.\n"
            f"Expected ({len(expected_errors)}):\n{expected_errors}\n\n"
            f"Actual ({len(actual_errors)}):\n{actual_errors}"
        )

    if expected_infos is not None:
        actual_infos = {entry["message"] for entry in logs.get("info", [])}
        assert actual_infos == expected_infos, (
            f"Mismatch in INFO logs.\n"
            f"Expected ({len(expected_infos)}):\n{expected_infos}\n\n"
            f"Actual ({len(actual_infos)}):\n{actual_infos}"
        )
