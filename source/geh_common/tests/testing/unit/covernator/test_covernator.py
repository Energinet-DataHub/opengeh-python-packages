import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

import polars as pl

from geh_common.testing.covernator.commands import (
    create_all_cases_file,
    create_result_and_all_scenario_files,
    find_all_cases,
    find_all_scenarios,
    run_covernator,
)
from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow


def test_covernator_all_scenarios():
    result = find_all_scenarios(
        Path("/workspace/source/geh_common/tests/testing/unit/covernator/test_files/scenario_tests")
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


def test_covernator_all_cases_from_yaml():
    result = find_all_cases(
        Path("/workspace/source/geh_common/tests/testing/unit/covernator/test_files/coverage/all_cases_test.yml")
    )

    assert len(result) == 7
    assert result == [
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

    def test_write_all_cases(self):
        create_all_cases_file(
            self.tmp_dir,
            Path("/workspace/source/geh_common/tests/testing/unit/covernator/test_files/coverage/all_cases_test.yml"),
        )

        all_cases_file = self.tmp_dir / "all_cases.csv"
        self.assertTrue(all_cases_file.exists())
        all_cases = pl.read_csv(all_cases_file, has_header=True)
        self.assertEqual(all_cases.columns, ["Path", "TestCase"])

    def test_write_scenario_files(self):
        create_result_and_all_scenario_files(
            self.tmp_dir, Path("/workspace/source/geh_common/tests/testing/unit/covernator/test_files/scenario_tests")
        )

        case_coverage_file = self.tmp_dir / "case_coverage.csv"
        self.assertTrue(case_coverage_file.exists())
        case_coverage = pl.read_csv(case_coverage_file, has_header=True)
        self.assertEqual(case_coverage.columns, ["Scenario", "CaseCoverage"])
        case_coverage_rows = case_coverage.sort(["Scenario", "CaseCoverage"]).to_dicts()
        expected_case_coverage_rows = [
            {
                "Scenario": "first_layer_folder1/sub_folder",
                "CaseCoverage": case_coverage,
            }
            for case_coverage in ["Case A1", "Case AA1", "Case BB1"]
        ] + [
            {
                "Scenario": "first_layer_folder2",
                "CaseCoverage": "Case AB1",
            }
        ]
        self.assertEqual(case_coverage_rows, expected_case_coverage_rows)

    def test_write_file_for_multiple_root_folders(self):
        run_covernator(self.tmp_dir, Path("/workspace/source/geh_common/tests/testing/unit/covernator"))

        case_coverage_file = self.tmp_dir / "case_coverage.csv"
        self.assertTrue(case_coverage_file.exists())
        case_coverage = pl.read_csv(case_coverage_file, has_header=True)
        self.assertEqual(case_coverage.columns, ["Group", "Scenario", "CaseCoverage"])
        case_coverage_rows = case_coverage.sort(["Group", "Scenario", "CaseCoverage"]).to_dicts()
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
        self.assertEqual(case_coverage_rows, expected_case_coverage_rows)
        all_cases_file = self.tmp_dir / "all_cases.csv"
        self.assertTrue(all_cases_file.exists())
        all_cases = pl.read_csv(all_cases_file, has_header=True)
        self.assertEqual(all_cases.columns, ["Group", "Path", "TestCase"])
        all_cases_rows = all_cases.sort(["Group", "Path", "TestCase"])
        self.assertEqual(all_cases_rows["Group"].to_list(), ["second_scenario_folder"] * 2 + ["test_files"] * 7)
        self.assertEqual(
            all_cases_rows["Path"].to_list(),
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
            ],
        )
        self.assertEqual(
            all_cases_rows["TestCase"].to_list(),
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
            ],
        )
