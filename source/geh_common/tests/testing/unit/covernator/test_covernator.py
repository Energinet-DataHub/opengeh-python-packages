import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Tuple
from unittest import TestCase

import polars as pl

from geh_common.testing.covernator.commands import (
    find_all_cases,
    find_all_scenarios,
    run_covernator,
)
from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow

covernator_testing_folder = Path(os.path.dirname(os.path.abspath(__file__)))


def test_covernator_all_scenarios():
    result = sorted(
        find_all_scenarios(covernator_testing_folder / "test_files" / "scenario_tests"),
        key=lambda scenario: scenario.source,
    )

    assert len(result) == 2
    assert result == [
        ScenarioRow(
            source="first_layer_folder1/sub_folder",
            cases_tested=["Case A1", "Case AA1", "Case BB1"],
        ),
        ScenarioRow(
            source="first_layer_folder2",
            cases_tested=["Case AB1"],
        ),
    ]


def test_covernator_all_cases_given_invalid_file_path():
    try:
        find_all_cases(covernator_testing_folder / "test_files" / "coverage" / "invalid_file.yml")
        did_raise = False
    except FileNotFoundError:
        did_raise = True

    assert did_raise


def test_covernator_given_invalid_file():
    result = find_all_cases(
        covernator_testing_folder / "test_files" / "scenario_tests" / "wrong_yml" / "coverage_mapping.yml"
    )
    assert len(result) == 0


def test_covernator_all_cases_from_yaml():
    result = sorted(
        find_all_cases(covernator_testing_folder / "test_files" / "coverage" / "all_cases_test.yml"),
        key=lambda case: f"{case.path}/{case.case}",
    )

    assert len(result) == 7
    assert result == [
        CaseRow(
            path="Case Group A / Sub Case Group AA",
            case="Case AA1",
            implemented=True,
        ),
        CaseRow(
            path="Case Group A / Sub Case Group AA",
            case="Case AA2",
            implemented=False,
        ),
        CaseRow(
            path="Case Group A / Sub Case Group AB",
            case="Case AB1",
            implemented=True,
        ),
        CaseRow(
            path="Case Group A",
            case="Case A1",
            implemented=True,
        ),
        CaseRow(
            path="Case Group A",
            case="Case A2",
            implemented=False,
        ),
        CaseRow(
            path="Case Group B / Sub Case Group BA",
            case="Case BA1",
            implemented=False,
        ),
        CaseRow(
            path="Case Group B / Sub Case Group BB",
            case="Case BB1",
            implemented=True,
        ),
    ]


class CovernatorFileWritingTestCase(TestCase):
    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        if self.tmp_dir.exists() and self.tmp_dir.is_dir():
            shutil.rmtree(self.tmp_dir)

    def _assert_files_exists_and_get_content(self) -> Tuple[pl.DataFrame, pl.DataFrame]:
        case_coverage_file = self.tmp_dir / "case_coverage.csv"
        self.assertTrue(case_coverage_file.exists())
        case_coverage = pl.read_csv(case_coverage_file, has_header=True)
        self.assertEqual(case_coverage.columns, ["Group", "Scenario", "CaseCoverage"])
        all_cases_file = self.tmp_dir / "all_cases.csv"
        self.assertTrue(all_cases_file.exists())
        all_cases = pl.read_csv(all_cases_file, has_header=True).sort(["Group", "Path", "TestCase"])
        self.assertEqual(all_cases.columns, ["Group", "Path", "TestCase"])
        return case_coverage, all_cases

    def test_run_with_non_existing_folder(self):
        non_existing_path = Path("/non/existing/folder")

        run_covernator(self.tmp_dir, non_existing_path)

        self.assertFalse(non_existing_path.exists())
        case_coverage, all_cases = self._assert_files_exists_and_get_content()
        self.assertEqual(0, len(case_coverage))
        self.assertEqual(0, len(all_cases))

    def test_run_with_existing_folder_without_cases(self):
        existing_path = Path(tempfile.mkdtemp())

        run_covernator(self.tmp_dir, existing_path)

        self.assertTrue(existing_path.exists())
        case_coverage, all_cases = self._assert_files_exists_and_get_content()
        self.assertEqual(0, len(case_coverage))
        self.assertEqual(0, len(all_cases))

    def test_write_file_for_single_root_folder(self):
        testing_folder = covernator_testing_folder / "test_files"

        run_covernator(self.tmp_dir, testing_folder)

        case_coverage_df, all_cases_df = self._assert_files_exists_and_get_content()
        actual_case_coverage_rows = sorted(
            case_coverage_df.to_dicts(), key=lambda x: (x["Group"], x["Scenario"], x["CaseCoverage"])
        )
        expected_case_coverage_rows = [
            {
                "Group": None,
                "Scenario": "first_layer_folder1/sub_folder",
                "CaseCoverage": case_coverage,
            }
            for case_coverage in ["Case A1", "Case AA1", "Case BB1"]
        ] + [
            {
                "Group": None,
                "Scenario": "first_layer_folder2",
                "CaseCoverage": "Case AB1",
            }
        ]
        self.assertEqual(actual_case_coverage_rows, expected_case_coverage_rows)

        actual_cases_rows = sorted(all_cases_df.to_dicts(), key=lambda x: (x["Group"], x["Path"], x["TestCase"]))
        self.assertEqual([row["Group"] for row in actual_cases_rows], [None] * 7)
        self.assertEqual(
            [row["Path"] for row in actual_cases_rows],
            [
                "Case Group A",
                "Case Group A",
                "Case Group A / Sub Case Group AA",
                "Case Group A / Sub Case Group AA",
                "Case Group A / Sub Case Group AB",
                "Case Group B / Sub Case Group BA",
                "Case Group B / Sub Case Group BB",
            ],
        )
        self.assertEqual(
            [row["TestCase"] for row in actual_cases_rows],
            [
                "Case A1",
                "Case A2",
                "Case AA1",
                "Case AA2",
                "Case AB1",
                "Case BA1",
                "Case BB1",
            ],
        )

        self.assertTrue((self.tmp_dir / "stats.json").exists())
        statistics = json.loads((self.tmp_dir / "stats.json").read_text())
        self.assertEqual(statistics, {"total_cases": 7, "total_scenarios": 2, "total_groups": 1})

    def test_write_file_for_multiple_root_folders(self):
        run_covernator(self.tmp_dir, covernator_testing_folder)

        case_coverage_df, all_cases_df = self._assert_files_exists_and_get_content()
        actual_case_coverage_rows = sorted(
            case_coverage_df.to_dicts(), key=lambda x: (x["Group"], x["Scenario"], x["CaseCoverage"])
        )
        expected_case_coverage_rows = (
            [
                {
                    "Group": "second_scenario_folder",
                    "Scenario": "some_folder",
                    "CaseCoverage": "Some Case",
                }
            ]
            + [
                {
                    "Group": "test_files",
                    "Scenario": "first_layer_folder1/sub_folder",
                    "CaseCoverage": case_coverage,
                }
                for case_coverage in ["Case A1", "Case AA1", "Case BB1"]
            ]
            + [
                {
                    "Group": "test_files",
                    "Scenario": "first_layer_folder2",
                    "CaseCoverage": "Case AB1",
                }
            ]
        )
        self.assertEqual(actual_case_coverage_rows, expected_case_coverage_rows)

        actual_cases_rows = sorted(all_cases_df.to_dicts(), key=lambda x: (x["Group"], x["Path"], x["TestCase"]))
        self.assertEqual(
            [row["Group"] for row in actual_cases_rows],
            ["second_scenario_folder"] * 2 + ["test_files"] * 7 + ["z_missing_scenarios_group"],
        )
        self.assertEqual(
            [row["Path"] for row in actual_cases_rows],
            [
                "Some Group / Some Sub Group",
                "Some Group / Some Sub Group",
                "Case Group A",
                "Case Group A",
                "Case Group A / Sub Case Group AA",
                "Case Group A / Sub Case Group AA",
                "Case Group A / Sub Case Group AB",
                "Case Group B / Sub Case Group BA",
                "Case Group B / Sub Case Group BB",
                "New Group",
            ],
        )
        self.assertEqual(
            [row["TestCase"] for row in actual_cases_rows],
            [
                "Not implemented yet",
                "Some Case",
                "Case A1",
                "Case A2",
                "Case AA1",
                "Case AA2",
                "Case AB1",
                "Case BA1",
                "Case BB1",
                "New Scenario",
            ],
        )

        self.assertTrue((self.tmp_dir / "stats.json").exists())
        statistics = json.loads((self.tmp_dir / "stats.json").read_text())
        self.assertEqual(statistics, {"total_cases": 10, "total_scenarios": 3, "total_groups": 3})
