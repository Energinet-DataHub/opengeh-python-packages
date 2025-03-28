from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl
import yaml

from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow


def get_case_rows_from_main_yaml(main_yaml_content: Dict[str, Dict], prefix: str = "") -> List[CaseRow]:
    case_rows: List[CaseRow] = []
    for scenario, content in main_yaml_content.items():
        if isinstance(content, bool):
            case_rows.append(
                CaseRow(
                    path=f"{prefix}",
                    case=scenario,
                    implemented=content,
                )
            )
        elif isinstance(content, dict):
            new_prefix = f"{prefix} / {scenario}" if len(prefix) > 0 else scenario
            case_rows.extend(
                get_case_rows_from_main_yaml(
                    content,
                    prefix=new_prefix,
                )
            )
    return case_rows


def find_all_cases(main_yaml_path: Path) -> List[CaseRow]:
    with open(main_yaml_path) as main_file:
        main_yaml_content = yaml.safe_load(main_file)

    coverage_by_case: List[CaseRow] = get_case_rows_from_main_yaml(main_yaml_content)
    return coverage_by_case


def _get_scenario_source_name_from_path(path: Path, feature_folder_name: Path) -> str:
    return str(path.relative_to(feature_folder_name).parent)


def _get_scenarios_cases_tested(content, parents=None) -> List[Tuple[List[str], str]]:
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
    coverage_by_scenario: List[ScenarioRow] = []
    errors = []

    for path in base_path.rglob("coverage_mapping.yml"):
        with open(path) as coverage_mapping_file:
            try:
                coverage_mapping = yaml.safe_load(coverage_mapping_file)
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


def create_all_cases_file(folder_to_save_files_in: Path, master_yaml_path: Path):
    folder_to_save_files_in.mkdir(parents=True, exist_ok=True)
    all_cases = find_all_cases(master_yaml_path)
    df = pl.DataFrame(all_cases).select(pl.col("path").alias("Path"), pl.col("case").alias("TestCase"))
    df.write_csv(folder_to_save_files_in / "all_cases.csv", include_header=True)


def create_result_and_all_scenario_files(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    all_scenarios = find_all_scenarios(base_path)
    df_all_scenarios = pl.DataFrame(all_scenarios)

    case_coverage = df_all_scenarios.explode("cases_tested").select(
        pl.col("source").alias("Scenario"),
        pl.col("cases_tested").alias("CaseCoverage"),
    )
    case_coverage.write_csv(folder_to_save_files_in / "case_coverage.csv", include_header=True)


def run_covernator(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    all_scenarios = []
    all_cases = []
    for path in base_path.rglob("coverage/all_cases*.yml"):
        group = str(path.relative_to(base_path)).split("/coverage/")[0]
        all_scenarios.append(
            pl.DataFrame(find_all_scenarios(base_path / group / "scenario_tests")).with_columns(
                pl.lit(group).alias("Group")
            )
        )
        all_cases.append(pl.DataFrame(find_all_cases(path)).with_columns(pl.lit(group).alias("Group")))

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
