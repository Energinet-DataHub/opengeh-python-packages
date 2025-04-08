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

    Args:
        main_yaml_path (Path): Path to the yaml file containing all scenarios.
        group (str | None): Group name to be added to the case rows if present.
    """
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

    Args:
        base_path (Path): The path to search for scenarios with the name 'coverage_mapping.yml'.
    """
    coverage_by_scenario: List[ScenarioRow] = []
    errors = []

    for path in base_path.rglob("coverage_mapping.yml"):
        with open(path) as coverage_mapping_file:
            try:
                coverage_mapping = yaml.safe_load(coverage_mapping_file)
                if "cases_tested" not in coverage_mapping:
                    logging.warning(f"Invalid yaml file '{path}': 'cases_tested' key not found.")
                    continue

                cases_tested = _get_scenarios_cases_tested(coverage_mapping["cases_tested"])
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
    """Generate the coverage files for all scenarios and all cases.

    This is the entrypoint for the covernator command.
    Yaml files that start with 'all_cases' are expected to be in the 'coverage' folder,
      containing the summary of all expected scenarios (implemented and not yet implemented ones).
    The parent folder of the 'coverage' folder is considered to be a group.
    In the same folder, a folder named 'scenario_tests' is expected to be present,
      in which the implemented scenarios should be found.
    The functions saves 2 files ('all_cases.csv' and 'case_coverage.csv') in the specified folder.

    Args:
        folder_to_save_files_in (Path): The folder where the coverage files will be saved.
        base_path (Path): The base path to search for scenarios and cases. Defaults to the current directory.
    """
    folder_to_save_files_in.mkdir(parents=True, exist_ok=True)

    all_scenarios = []
    all_cases = []
    for path in base_path.rglob("coverage/all_cases*.yml"):
        group = str(path.relative_to(base_path)).split("/coverage/")[0]
        group_name = group.split(os.sep)[-1]
        group_cases = find_all_cases(path)
        if len(group_cases) == 0:
            logging.warning(f"No cases found in {path}")
            continue
        all_cases.append(pl.DataFrame(group_cases).with_columns(pl.lit(group_name).alias("Group")))
        group_scenarios = find_all_scenarios(base_path / group / "scenario_tests")
        group_scenarios_df = pl.DataFrame(group_scenarios)
        if len(group_scenarios_df) > 0:
            group_scenarios_df = group_scenarios_df.with_columns(pl.lit(group_name).alias("Group"))
        all_scenarios.append(group_scenarios_df)

    df_all_scenarios = (
        pl.concat(all_scenarios)
        .explode("cases_tested")
        .select(
            pl.col("Group"),
            pl.col("source").alias("Scenario"),
            pl.col("cases_tested").alias("CaseCoverage"),
        )
    )
    df_all_scenarios.write_csv(folder_to_save_files_in / "case_coverage.csv", include_header=True)

    df_all_cases = pl.concat(all_cases).select(
        pl.col("Group"), pl.col("path").alias("Path"), pl.col("case").alias("TestCase")
    )
    df_all_cases.write_csv(folder_to_save_files_in / "all_cases.csv", include_header=True)
