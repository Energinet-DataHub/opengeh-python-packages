from pathlib import Path
from geh_common.testing.covernator.commands import run_covernator
import json


def run_and_load_stats(base_path: Path, tmp_path: Path) -> tuple[Path, dict]:
    """
    Runs the covernator process and loads the resulting stats.json file.

    Returns:
        - output_dir (Path): Path to output directory (for checking generated CSVs)
        - stats (dict): Parsed stats.json containing total_cases, total_scenarios, etc.
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    run_covernator(output_dir, base_path)

    stats_path = output_dir / "stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8"))

    return output_dir, stats

def assert_log_messages(
    logs: dict,
    expected_errors: set[str] = None,
    expected_infos: set[str] = None,
):
    """
    Asserts the presence and correctness of error and info log messages.

    Args:
        logs (dict): The logs section from stats.json (i.e., stats["logs"])
        expected_errors (set[str]): Set of expected error message strings
        expected_infos (set[str]): Set of expected info message strings
    """
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
