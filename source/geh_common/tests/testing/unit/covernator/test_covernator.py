import json
from pathlib import Path
from geh_common.testing.covernator.commands import run_covernator


def test_missing_master_list_raises_error(tmp_path):
    """
    Verify that run_covernator logs an error when a group has no all_cases.yml file.
    This mirrors the logic at line ~270 of commands.py.
    """

    # Arrange — create expected folder structure
    base_path = tmp_path / "repo"
    subsystem_dir = base_path / "subsystem_a" / "tests" / "group_1" / "coverage"
    subsystem_dir.mkdir(parents=True)

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Act — run the covernator logic
    run_covernator(output_dir, base_path)

    # Assert — stats.json is created
    stats_path = output_dir / "stats.json"
    assert stats_path.exists(), "Expected stats.json to be created even when master list is missing"

    # Load and inspect JSON
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    error_logs = stats.get("logs", {}).get("error", [])

    # Expect an error mentioning the missing master file
    assert any("missing all_cases" in e["message"].lower() for e in error_logs), (
        f"Expected an error mentioning missing all_cases, got: {error_logs}"
    )

    # Ensure totals exist (even if 0)
    for key in ["total_cases", "total_scenarios", "total_groups"]:
        assert key in stats, f"Missing {key} in stats.json"
