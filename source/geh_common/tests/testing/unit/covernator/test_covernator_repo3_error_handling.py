import json
from pathlib import Path
import polars as pl
from geh_common.testing.covernator.commands import run_covernator


def _assert_frames_equal(df1: pl.DataFrame, df2: pl.DataFrame, sort_cols: list[str]):
    """Fallback assertion for Polars DataFrames without pl.testing."""
    df1_sorted = df1.sort(sort_cols)
    df2_sorted = df2.sort(sort_cols)
    assert df1_sorted.shape == df2_sorted.shape, (
        f"Shape mismatch: {df1_sorted.shape} != {df2_sorted.shape}"
    )
    for col in df1_sorted.columns:
        vals1, vals2 = df1_sorted[col].to_list(), df2_sorted[col].to_list()
        assert vals1 == vals2, f"Mismatch in column '{col}':\n{vals1}\nvs\n{vals2}"


def test_happy_path_repo2_generates_expected_outputs(tmp_path: Path):
    base_path = Path(__file__).parent / "test_files" / "repo3" / "geh_repo3"
    assert (base_path / "tests" / "group_x" / "coverage").exists(), "Missing coverage folder"

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    run_covernator(output_dir, base_path)

    # --- Assert stats
    stats = json.loads((output_dir / "stats.json").read_text(encoding="utf-8"))
    assert stats["total_cases"] == 13
    assert stats["total_scenarios"] == 3
    assert stats["total_groups"] == 3
    error_logs = stats.get("logs", {}).get("error", [])
    assert len(error_logs) == 3, f"Expected one error log, got {len(error_logs)}"
    assert error_logs[0]["message"] == "[ERROR] [group_y] Duplicate items in all cases: Case 2"
    assert error_logs[1]["message"] == "[ERROR] [geh_repo3/group_x] Duplicate items in scenario [given_group_x_scenario2]: Case AB1"

    # --- all_cases.csv
    df_all_cases = pl.read_csv(output_dir / "all_cases.csv")

    # Include 'Repo 2 Tests' prefix in all expected paths
    expected_all_cases_rows = [
        {"Group": "geh_repo3/group_x", "TestCase": "Case 1", "Path": "Repo 3 group_x Tests", "Implemented": True},
        {"Group": "geh_repo3/group_x", "TestCase": "Case 2", "Path": "Repo 3 group_x Tests", "Implemented": True},
        {"Group": "geh_repo3/group_x", "TestCase": "Case A1", "Path": "Repo 3 group_x Tests / Sub heading A", "Implemented": True},
        {"Group": "geh_repo3/group_x", "TestCase": "Case A2", "Path": "Repo 3 group_x Tests / Sub heading A", "Implemented": True},
        {"Group": "geh_repo3/group_x", "TestCase": "Case AA1", "Path": "Repo 3 group_x Tests / Sub heading A / Sub heading AA", "Implemented": True},
        {"Group": "geh_repo3/group_x", "TestCase": "Case AA2", "Path": "Repo 3 group_x Tests / Sub heading A / Sub heading AA", "Implemented": True},
        {"Group": "geh_repo3/group_x", "TestCase": "Case AB1", "Path": "Repo 3 group_x Tests / Sub heading A / Sub heading AB", "Implemented": True},
        {"Group": "geh_repo3/group_y", "TestCase": "Case 1", "Path": "Repo 3 group_y Tests", "Implemented": True},
        {"Group": "geh_repo3/group_y", "TestCase": "Case 2", "Path": "Repo 3 group_y Tests", "Implemented": True},
        {"Group": "geh_repo3/group_y", "TestCase": "Case A1", "Path": "Repo 3 group_y Tests / Sub heading A", "Implemented": True},
        {"Group": "geh_repo3/group_y", "TestCase": "Case A2", "Path": "Repo 3 group_y Tests / Sub heading A", "Implemented": True},
        {"Group": "geh_repo3/group_z", "TestCase": "Case 1", "Path": "Repo 3 group_z Tests", "Implemented": True},
        {"Group": "geh_repo3/group_z", "TestCase": "Case 2", "Path": "Repo 3 group_z Tests", "Implemented": True},

    ]
    df_expected_all_cases = pl.DataFrame(expected_all_cases_rows)
    _assert_frames_equal(df_all_cases, df_expected_all_cases, ["Group", "Path", "TestCase"])


    # --- case_coverage.csv
    df_case_cov = pl.read_csv(output_dir / "case_coverage.csv")

    # Vectorized normalization
    df_case_cov = df_case_cov.with_columns(
        pl.col("Scenario").str.replace_all("\\\\", "/")
    )

    scen_x1 = "given_group_x_scenario1"
    scen_x2 = "given_group_x_scenario2"
    scen_y1 = "group_y_subfolder_1/given_group_y_subfolder_1_scenario1"

    expected_cov_rows = [
        {"Group": "geh_repo3/group_x", "Scenario": scen_x1, "CaseCoverage": "Case 1"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x1, "CaseCoverage": "Case 2"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x1, "CaseCoverage": "Case A1"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x1, "CaseCoverage": "Case A2"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x2, "CaseCoverage": "Case AA1"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x2, "CaseCoverage": "Case AA2"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x2, "CaseCoverage": "Case AB1"},
        {"Group": "geh_repo3/group_x", "Scenario": scen_x2, "CaseCoverage": "Case AB1"},
        {"Group": "geh_repo3/group_y", "Scenario": scen_y1, "CaseCoverage": "Case 1"},
        {"Group": "geh_repo3/group_y", "Scenario": scen_y1, "CaseCoverage": "Case 2"},
        {"Group": "geh_repo3/group_y", "Scenario": scen_y1, "CaseCoverage": "Case A1"},
        {"Group": "geh_repo3/group_y", "Scenario": scen_y1, "CaseCoverage": "Case A2"},


    ]
    df_expected_cov = pl.DataFrame(expected_cov_rows)
    _assert_frames_equal(df_case_cov, df_expected_cov, ["Group", "Scenario", "CaseCoverage"])
