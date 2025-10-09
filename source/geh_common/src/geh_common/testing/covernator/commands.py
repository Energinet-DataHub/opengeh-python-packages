import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl
import yaml

from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow


# --- DuplicateKeyLoader to detect duplicates ---
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
                    logging.debug(f"[ERROR][{self.group}] Duplicate items in scenario [{self.scenario}]: {key}")
                elif self.group:
                    logging.debug(f"[ERROR][{self.group}] Duplicate items in all cases: {key}")
                else:
                    logging.debug(f"[ERROR] Duplicate key found in YAML: {key}")
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def load_yaml_with_duplicates(path: Path, group=None, scenario=None):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.load(f, Loader=lambda stream: DuplicateKeyLoader(stream, group, scenario))


# --- Case extraction ---
def _get_case_rows_from_main_yaml(
    main_yaml_content: Dict[str, Dict | bool], prefix: str = "", group: str | None = None
) -> List[CaseRow]:
    case_rows: List[CaseRow] = []
    for scenario, content in main_yaml_content.items():
        if isinstance(content, bool):
            case_rows.append(CaseRow(path=f"{prefix}", case=scenario, implemented=content, group=group))
        elif isinstance(content, dict):
            new_prefix = f"{prefix} / {scenario}" if len(prefix) > 0 else scenario
            case_rows.extend(_get_case_rows_from_main_yaml(content, prefix=new_prefix, group=group))
    return case_rows


def find_all_cases(main_yaml_path: Path, group: str | None = None) -> List[CaseRow]:
    if not main_yaml_path.exists():
        raise FileNotFoundError(f"File {main_yaml_path} does not exist.")

    main_yaml_content = load_yaml_with_duplicates(main_yaml_path, group=group)

    if not isinstance(main_yaml_content, dict):
        return []
    return _get_case_rows_from_main_yaml(main_yaml_content, group=group)


# --- Scenario extraction ---
def _get_scenario_source_name_from_path(path: Path, feature_folder_name: Path) -> str:
    return str(path.relative_to(feature_folder_name).parent)


def _get_scenarios_cases_tested(content, parents=None) -> List[Tuple[List[str], str]]:
    if parents is None:
        parents = []
    if isinstance(content, dict):
        results = []
        for key, value in content.items():
            results.extend(_get_scenarios_cases_tested(value, parents + [key]))
        return results
    elif isinstance(content, list):
        results = []
        for case in content:
            results.extend(_get_scenarios_cases_tested(case, parents))
        return results
    else:
        return [(parents, content)]


def find_all_scenarios(base_path: Path) -> List[ScenarioRow]:
    coverage_by_scenario: List[ScenarioRow] = []
    errors = []

    for path in base_path.rglob("coverage_mapping.yml"):
        try:
            coverage_mapping = load_yaml_with_duplicates(path, group=base_path.name, scenario=path.stem)
            cases_tested_content = coverage_mapping.get("cases_tested") if coverage_mapping is not None else None
            if cases_tested_content is None:
                logging.warning(f"[ERROR] Invalid yaml file '{path}': 'cases_tested' key not found.")
                continue

            cases_tested = _get_scenarios_cases_tested(cases_tested_content)
            coverage_by_scenario.append(
                ScenarioRow(
                    source=_get_scenario_source_name_from_path(path, base_path),
                    cases_tested=[case for _, case in cases_tested],
                )
            )
        except yaml.YAMLError:
            logging.warning(f"[ERROR] Invalid yaml file '{path}' (YAMLError)")
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    if errors:
        logging.warning(f"[ERROR] Errors while parsing scenarios: {errors}")

    return coverage_by_scenario


# --- Main ---
def run_covernator(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    logging.debug("📣 run_covernator() started!")
    folder_to_save_files_in.mkdir(parents=True, exist_ok=True)

    all_cases, all_scenarios = [], []

    def is_subsystem_folder(path: Path) -> bool:
        return (path / "tests").exists()

    subsystems = [base_path] if is_subsystem_folder(base_path) else [
        p for p in base_path.iterdir() if p.is_dir() and is_subsystem_folder(p)
    ]

    for subsystem_dir in subsystems:
        subsystem = subsystem_dir.name
        tests_dir = subsystem_dir / "tests"

        for group_dir in [d for d in tests_dir.iterdir() if d.is_dir()] + [tests_dir]:
            coverage_dir = group_dir / "coverage"
            if not coverage_dir.exists():
                continue

            group = group_dir.name if group_dir != tests_dir else None
            key = f"{subsystem}/{group}" if group else subsystem
            logging.info(f"[INFO][{subsystem}]{f'[{group}]' if group else ''} Processing {'group' if group else 'subsystem'}: {group or subsystem}")

            all_cases_path = next(coverage_dir.glob("all_cases*.yml"), None)
            if not all_cases_path:
                logging.warning(f"[ERROR][{subsystem}]{f'[{group}]' if group else ''} Missing all_cases YAML file — scenario_test(s) exist but no all_cases*.yml found.")
                continue

            group_cases = find_all_cases(all_cases_path, group=group)
            scenarios_path = next((p for p in group_dir.glob("scenario_test*") if p.is_dir()), None)
            group_scenarios = find_all_scenarios(scenarios_path) if scenarios_path else []

            if not scenarios_path:
                logging.warning(f"[ERROR][{subsystem}]{f'[{group}]' if group else ''} Could not find 'scenario_test(s)' folder.")

            all_cases.extend({
                "Group": key,
                "TestCase": case_row.case,
                "Path": str(case_row.path),
                "Implemented": case_row.implemented,
            } for case_row in group_cases)

            all_scenarios.extend({
                "Group": key,
                "Scenario": scenario_row.source,
                "CaseCoverage": case,
            } for scenario_row in group_scenarios for case in scenario_row.cases_tested)

    # --- DataFrames
    df_all_cases = (
        pl.DataFrame(all_cases)
        if all_cases else
        pl.DataFrame(schema={"Group": str, "TestCase": str, "Path": str, "Implemented": bool})
    )
    df_all_scenarios = (
        pl.DataFrame(all_scenarios)
        if all_scenarios else
        pl.DataFrame(schema={"Group": str, "Scenario": str, "CaseCoverage": str})
    )

    expected_cases = (
        set(df_all_cases.filter(pl.col("Implemented") == True)["TestCase"].to_list())
        if df_all_cases.height > 0 else set()
    )
    false_cases = (
        set(df_all_cases.filter(pl.col("Implemented") == False)["TestCase"].to_list())
        if df_all_cases.height > 0 else set()
    )
    scenario_cases = set(df_all_scenarios["CaseCoverage"].to_list()) if df_all_scenarios.height > 0 else set()

    # --- Duplicates
    seen = set()
    for case_row in df_all_cases.iter_rows(named=True):
        key = (case_row["Group"], case_row["TestCase"])
        if key in seen:
            logging.debug(f"[ERROR][{case_row['Group']}] Duplicate items in all cases: {case_row['TestCase']}")
        else:
            seen.add(key)

    seen = set()
    for scenario_row in df_all_scenarios.iter_rows(named=True):
        key = (scenario_row["Group"], scenario_row["Scenario"], scenario_row["CaseCoverage"])
        if key in seen:
            logging.debug(f"[ERROR][{scenario_row['Group']}] Duplicate items in scenario [{scenario_row['Scenario']}]: {scenario_row['CaseCoverage']}")
        else:
            seen.add(key)

    # --- Coverage missing
    for case_row in df_all_cases.iter_rows(named=True):
        if not case_row["Implemented"]:
            logging.info(f"[INFO][{case_row['Group']}] Case is marked as false in master list: {case_row['TestCase']}")
            continue
        if case_row["TestCase"] not in scenario_cases:
            logging.debug(f"[ERROR][{case_row['Group']}] Case not covered in any scenario: {case_row['TestCase']}")

    # --- Unexpected or false coverage
    logged_false = set()
    for scenario_row in df_all_scenarios.iter_rows(named=True):
        case = scenario_row["CaseCoverage"]
        group = scenario_row["Group"]
        scenario = scenario_row["Scenario"]

        if case in false_cases:
            if (group, case) not in logged_false:
                logging.error(f"[ERROR][{group}] Case found in scenario [{scenario}] is marked as false in master list: {case}")
                logged_false.add((group, case))
        elif case not in expected_cases:
            logging.debug(f"[ERROR][{group}] Case found in scenario [{scenario}] not included in master list: {case}")

    # --- Outputs
    df_all_cases.write_csv(folder_to_save_files_in / "all_cases.csv")
    df_all_scenarios.write_csv(folder_to_save_files_in / "case_coverage.csv")
    stats = {
        "total_cases": df_all_cases.height,
        "total_scenarios": df_all_scenarios.height,
        "total_groups": len(df_all_cases["Group"].unique()) if df_all_cases.height > 0 else 0,
    }
    with open(folder_to_save_files_in / "stats.json", "w", encoding="utf-8") as stats_file:
        json.dump(stats, stats_file, indent=4)
