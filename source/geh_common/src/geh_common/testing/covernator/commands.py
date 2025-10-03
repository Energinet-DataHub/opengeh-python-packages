import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import polars as pl
import yaml

from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow


# --- Custom loader to catch duplicate keys in YAML ---
# --- Custom loader to catch duplicate keys in YAML with context ---
class DuplicateKeyLoader(yaml.SafeLoader):
    def __init__(self, stream, group=None, scenario=None):
        super().__init__(stream)
        self.group = group
        self.scenario = scenario

    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                if self.group and self.scenario:
                    logging.debug(
                        f"[{self.group}] Duplicate items in scenario [{self.scenario}]: {key}"
                    )
                elif self.group:
                    logging.debug(
                        f"[{self.group}] Duplicate items in all cases: {key}"
                    )
                else:
                    logging.debug(f"Duplicate key found in YAML: {key}")
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def load_yaml_with_duplicates(path: Path, group=None, scenario=None):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.load(f, Loader=lambda stream: DuplicateKeyLoader(stream, group, scenario))



def _get_case_rows_from_main_yaml(
    main_yaml_content: Dict[str, Dict | bool], prefix: str = "", group: str | None = None
) -> List[CaseRow]:
    """Transform the content of yaml file containing all scenarios from it's hierarchical structure into a list of cases recursively."""
    case_rows: List[CaseRow] = []
    for scenario, content in main_yaml_content.items():
        if isinstance(content, bool):
            case_rows.append(CaseRow(path=f"{prefix}", case=scenario, implemented=content, group=group))
        elif isinstance(content, dict):
            new_prefix = f"{prefix} / {scenario}" if len(prefix) > 0 else scenario
            case_rows.extend(_get_case_rows_from_main_yaml(content, prefix=new_prefix, group=group))
    return case_rows


def find_all_cases(main_yaml_path: Path, group: str | None = None) -> List[CaseRow]:
    """Parse a yaml file containing all scenarios and transform it into a list of cases."""
    if not main_yaml_path.exists():
        raise FileNotFoundError(f"File {main_yaml_path} does not exist.")

    main_yaml_content = load_yaml_with_duplicates(main_yaml_path, group=group)

    if not isinstance(main_yaml_content, dict):
        return []

    coverage_by_case: List[CaseRow] = _get_case_rows_from_main_yaml(main_yaml_content, group=group)
    return coverage_by_case


def _get_scenario_source_name_from_path(path: Path, feature_folder_name: Path) -> str:
    return str(path.relative_to(feature_folder_name).parent)


def _get_scenarios_cases_tested(content, parents=None):
    if parents is None:
        parents = []

    if isinstance(content, dict):
        all_cases_from_dict = []
        for key, value in content.items():
            if isinstance(value, list) and all(isinstance(v, str) for v in value):
                for v in value:
                    all_cases_from_dict.append((parents + [key], v))
            else:
                all_cases_from_dict.extend(_get_scenarios_cases_tested(value, parents + [key]))
        return all_cases_from_dict
    elif isinstance(content, list):
        all_cases_from_list = []
        for case in content:
            all_cases_from_list.extend(_get_scenarios_cases_tested(case, parents))
        return all_cases_from_list
    elif isinstance(content, str):
        return [(parents, content)]
    else:
        return []


def find_all_scenarios(base_path: Path) -> List[ScenarioRow]:
    coverage_by_scenario: List[ScenarioRow] = []
    errors = []

    for path in base_path.rglob("coverage_mapping.yml"):
        try:
            try:
                coverage_mapping = load_yaml_with_duplicates(path, group=base_path.name, scenario=path.stem)
            except yaml.YAMLError:
                logging.warning(f"Invalid yaml file '{path}' (YAMLError)")
                continue

            cases_tested_content = coverage_mapping.get("cases_tested") if coverage_mapping is not None else None
            if cases_tested_content is None:
                logging.warning(f"Invalid yaml file '{path}': 'cases_tested' key not found.")
                continue

            cases_tested = _get_scenarios_cases_tested(cases_tested_content)
            coverage_by_scenario.append(
                ScenarioRow(
                    source=_get_scenario_source_name_from_path(path, base_path),
                    cases_tested=[case for _, case in cases_tested],
                )
            )
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    if errors:
        logging.warning(f"Errors while parsing scenarios: {errors}")

    return coverage_by_scenario


def run_covernator(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    folder_to_save_files_in.mkdir(parents=True, exist_ok=True)

    all_scenarios = []
    all_cases = []

    for path in base_path.rglob("coverage/all_cases*.yml"):
        group = path.parent.parent.relative_to(base_path)

        group_cases = find_all_cases(path, group=str(group))
        group_scenarios = find_all_scenarios(base_path / group / "scenario_tests")

        all_cases.extend(
            [
                {
                    "Group": str(group),
                    "TestCase": case_row.case,
                    "Path": str(case_row.path),
                    "Implemented": case_row.implemented,
                }
                for case_row in group_cases
            ]
        )

        all_scenarios.extend(
            [
                {
                    "Group": str(group),
                    "Scenario": scenario_row.source,
                    "CaseCoverage": case,
                }
                for scenario_row in group_scenarios
                for case in scenario_row.cases_tested
            ]
        )

    df_all_cases = (
        pl.DataFrame(all_cases)
        if all_cases
        else pl.DataFrame(schema={"Group": str, "TestCase": str, "Path": str, "Implemented": bool})
    )
    df_all_scenarios = (
        pl.DataFrame(all_scenarios)
        if all_scenarios
        else pl.DataFrame(schema={"Group": str, "Scenario": str, "CaseCoverage": str})
    )

    expected_cases = set(df_all_cases["TestCase"].to_list()) if df_all_cases.height > 0 else set()
    scenario_cases = set(df_all_scenarios["CaseCoverage"].to_list()) if df_all_scenarios.height > 0 else set()

    # --- Detect duplicates in all_cases
    seen = set()
    for case_row in all_cases:
        key = (case_row["Group"], case_row["TestCase"])
        if key in seen:
            logging.debug(f"[{case_row['Group']}] Duplicate items in all cases: {case_row['TestCase']}")
        else:
            seen.add(key)

    # --- Detect duplicates in scenarios
    seen = set()
    for scenario_row in all_scenarios:
        key = (scenario_row["Group"], scenario_row["Scenario"], scenario_row["CaseCoverage"])
        if key in seen:
            logging.debug(
                f"[{scenario_row['Group']}] Duplicate items in scenario [{scenario_row['Scenario']}]: {scenario_row['CaseCoverage']}"
            )
        else:
            seen.add(key)

    # --- Log cases missing or unexpected ---
    for case_row in all_cases:
        case = case_row["TestCase"]
        group = case_row["Group"]
        if case not in scenario_cases:
            logging.debug(f"[{group}] Case not covered in any scenario: {case}")

    for scenario_row in all_scenarios:
        case = scenario_row["CaseCoverage"]
        group = scenario_row["Group"]
        scenario = scenario_row["Scenario"]
        if case not in expected_cases:
            logging.debug(f"[{group}] Case found in scenario [{scenario}] not included in master list: {case}")

    # --- Write outputs ---
    df_all_cases.write_csv(folder_to_save_files_in / "all_cases.csv")
    df_all_scenarios.write_csv(folder_to_save_files_in / "case_coverage.csv")

    stats = {
        "total_cases": df_all_cases.height,
        "total_scenarios": df_all_scenarios.height,
        "total_groups": len(df_all_cases["Group"].unique()) if df_all_cases.height > 0 else 0,
    }
    with open(folder_to_save_files_in / "stats.json", "w", encoding="utf-8") as stats_file:
        json.dump(stats, stats_file, indent=4)
