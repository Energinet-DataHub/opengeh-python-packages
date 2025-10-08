import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl
import yaml

from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow


def _get_case_rows_from_main_yaml(
    main_yaml_content: Dict[str, Dict | bool], prefix: str = "", group: str | None = None
) -> List[CaseRow]:
    """Transform the content of yaml file containing all scenarios from it's hirarchical structure into a list of cases recursively."""
    case_rows: List[CaseRow] = []
    for scenario, content in main_yaml_content.items():
        if isinstance(content, bool):
            case_rows.append(CaseRow(path=f"{prefix}", case=scenario, implemented=content, group=group))
        elif isinstance(content, dict):
            new_prefix = f"{prefix} / {scenario}" if len(prefix) > 0 else scenario
            case_rows.extend(_get_case_rows_from_main_yaml(content, prefix=new_prefix, group=group))
    return case_rows


def find_all_cases(main_yaml_path: Path, group: str | None = None) -> List[CaseRow]:
    """Parse a yaml file containing all scenarios and transform it into a list of cases.

    If yaml path is invalid or content in an invalid format, raise an exception.

    Args:
        main_yaml_path (Path): Path to the yaml file containing all scenarios.
        group (str | None): Group name to be added to the case rows if present.
    """
    if not main_yaml_path.exists():
        raise FileNotFoundError(f"File {main_yaml_path} does not exist.")

    with open(main_yaml_path) as main_file:
        main_yaml_content = yaml.safe_load(main_file)

    if not isinstance(main_yaml_content, dict):
        return []

    coverage_by_case: List[CaseRow] = _get_case_rows_from_main_yaml(main_yaml_content, group=group)
    return coverage_by_case


def _get_scenario_source_name_from_path(path: Path, feature_folder_name: Path) -> str:
    return str(path.relative_to(feature_folder_name).parent)


def _get_scenarios_cases_tested(content, parents=None) -> List[Tuple[List[str], str]]:
    """Find cases for the content of a given scenario from a coverage_mapping.yml file."""
    if parents is None:
        parents = []
    if isinstance(content, dict):
        all_cases_from_dict = []
        for key, value in content.items():
            all_cases_from_dict.extend(_get_scenarios_cases_tested(value, parents + [key]))
        return all_cases_from_dict
    elif isinstance(content, list):
        all_cases_from_list = []
        for case in content:
            all_cases_from_list.extend(_get_scenarios_cases_tested(case, parents))
        return all_cases_from_list
    else:
        return [(parents, content)]


def find_all_scenarios(base_path: Path) -> List[ScenarioRow]:
    """Find all implemented scenarios for the given path.

    Searches for all files named 'coverage_mapping.yml' in the given path and its subdirectories.
    For each file, it loads the content and extracts the cases tested for each scenario.
    Ignores the file if it doesn't contain the 'cases_tested' key or if there are no cases.

    Args:
        base_path (Path): The path to search for scenarios with the name 'coverage_mapping.yml'.
    """
    coverage_by_scenario: List[ScenarioRow] = []
    errors = []

    for path in base_path.rglob("coverage_mapping.yml"):
        with open(path) as coverage_mapping_file:
            try:
                try:
                    coverage_mapping = yaml.safe_load(coverage_mapping_file)
                except yaml.YAMLError:
                    logging.warning(f"Invalid yaml file '{path}': {coverage_mapping_file}")
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
            except (yaml.YAMLError, KeyError) as exc:
                errors.append(f"Error loading {path}: {exc}")

    if len(errors) > 0:
        raise Exception("\n".join(errors))

    return coverage_by_scenario


def run_covernator(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    folder_to_save_files_in.mkdir(parents=True, exist_ok=True)
    logging.debug("📣 run_covernator() started!")

    all_cases, all_scenarios = [], []

    def is_subsystem_folder(path: Path) -> bool:
        return (path / "tests").exists()

    subsystems = []
    if is_subsystem_folder(base_path):
        subsystems = [base_path]
    else:
        subsystems = [p for p in base_path.iterdir() if p.is_dir() and is_subsystem_folder(p)]

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

    # --- DataFrames + Logging
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

    for case_row in df_all_cases.iter_rows(named=True):
        if case_row["TestCase"] not in scenario_cases:
            logging.debug(f"[ERROR][{case_row['Group']}] Case not covered in any scenario: {case_row['TestCase']}")

    for scenario_row in df_all_scenarios.iter_rows(named=True):
        if scenario_row["CaseCoverage"] not in expected_cases:
            logging.debug(f"[ERROR][{scenario_row['Group']}] Case found in scenario [{scenario_row['Scenario']}] not included in master list: {scenario_row['CaseCoverage']}")

    df_all_cases.write_csv(folder_to_save_files_in / "all_cases.csv")
    df_all_scenarios.write_csv(folder_to_save_files_in / "case_coverage.csv")
    stats = {
        "total_cases": df_all_cases.height,
        "total_scenarios": df_all_scenarios.height,
        "total_groups": len(df_all_cases["Group"].unique()) if df_all_cases.height > 0 else 0,
    }
    with open(folder_to_save_files_in / "stats.json", "w", encoding="utf-8") as stats_file:
        json.dump(stats, stats_file, indent=4)

