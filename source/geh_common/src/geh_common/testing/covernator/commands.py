import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl
import yaml
from enum import Enum, auto

from geh_common.testing.covernator.row_types import CaseRow, ScenarioRow


# =====================================================================
# === Log levels ======================================================
# =====================================================================
class LogLevel(Enum):
    INFO = auto()
    ERROR = auto()


# =====================================================================
# === Output Manager (kept, CI-friendly logs + stats.json structure) ==
# =====================================================================
class OutputManager:
    """
    Centralized output manager for Covernator.
    Handles console output, grouped logging, CSV, and stats.json writing.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.logs_info: list[dict] = []
        self.logs_error: list[dict] = []
        self.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()])

    def log(self, message: str, level: LogLevel = LogLevel.INFO):
        """
        Record a message with a specified log level.
        Automatically prepends [INFO]/[ERROR] tags to messages.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_tag = "[INFO]" if level == LogLevel.INFO else "[ERROR]"
        formatted_message = f"[{timestamp}] {level_tag} {message}"

        if level == LogLevel.ERROR:
            logging.error(formatted_message)
            self.logs_error.append({"timestamp": timestamp, "message": f"{level_tag} {message}"})
        else:
            logging.info(formatted_message)
            self.logs_info.append({"timestamp": timestamp, "message": f"{level_tag} {message}"})

    def write_csv(self, filename: str, data: pl.DataFrame):
        """
        Write CSV files to the output directory.
        """
        data.write_csv(self.output_dir / filename)

    def finalize(self, stats: dict):
        """
        Write final stats.json containing:
          - total counts
          - info/error message groups
        """
        output = {
            "total_cases": stats.get("total_cases", 0),
            "total_scenarios": stats.get("total_scenarios", 0),
            "total_groups": stats.get("total_groups", 0),
            "logs": {
                "info": self.logs_info,
                "error": self.logs_error
            },
            "generated_at": self.start_time,
        }
        with open(self.output_dir / "stats.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4)


# =====================================================================
# === DuplicateKeyLoader to detect duplicates =========================
# =====================================================================
class DuplicateKeyLoader(yaml.SafeLoader):
    def __init__(self, stream, group=None, scenario=None, output: OutputManager | None = None):
        super().__init__(stream)
        self.group = group
        self.scenario = scenario
        self.output = output

    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping and self.output:
                if self.group and self.scenario:
                    self.output.log(f"[{self.group}] Duplicate items in scenario [{self.scenario}]: {key}", level=LogLevel.ERROR)
                elif self.group:
                    self.output.log(f"[{self.group}] Duplicate items in all cases: {key}", level=LogLevel.ERROR)
                else:
                    self.output.log(f"[ERROR] Duplicate key found in YAML: {key}", level=LogLevel.ERROR)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def load_yaml_with_duplicates(path: Path, output: OutputManager, group=None, scenario=None):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.load(f, Loader=lambda stream: DuplicateKeyLoader(stream, group, scenario, output))


# =====================================================================
# === Case Extraction =================================================
# =====================================================================
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


def find_all_cases(main_yaml_path: Path, output: OutputManager | None = None, group: str | None = None) -> List[CaseRow]:
    if not main_yaml_path.exists():
        raise FileNotFoundError(f"File {main_yaml_path} does not exist.")

    if output:
        main_yaml_content = load_yaml_with_duplicates(main_yaml_path, output=output, group=group)
    else:
        with open(main_yaml_path, "r", encoding="utf-8") as f:
            main_yaml_content = yaml.safe_load(f)

    if not isinstance(main_yaml_content, dict):
        return []
    return _get_case_rows_from_main_yaml(main_yaml_content, group=group)


# =====================================================================
# === Scenario Extraction =============================================
# =====================================================================
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


def find_all_scenarios(base_path: Path, output: OutputManager | None = None) -> List[ScenarioRow]:
    coverage_by_scenario: List[ScenarioRow] = []
    errors = []

    for path in base_path.rglob("coverage_mapping.yml"):
        try:
            if output:
                coverage_mapping = load_yaml_with_duplicates(path, output=output, group=base_path.name, scenario=path.stem)
            else:
                with open(path, "r", encoding="utf-8") as f:
                    coverage_mapping = yaml.safe_load(f)
            cases_tested_content = coverage_mapping.get("cases_tested") if coverage_mapping is not None else None
            if cases_tested_content is None:
                if output:
                    output.log(f"[ERROR] Invalid yaml file '{path}': 'cases_tested' key not found.", level=LogLevel.ERROR)
                continue

            cases_tested = _get_scenarios_cases_tested(cases_tested_content)
            coverage_by_scenario.append(
                ScenarioRow(
                    source=_get_scenario_source_name_from_path(path, base_path),
                    cases_tested=[case for _, case in cases_tested],
                )
            )
        except yaml.YAMLError:
            if output:
                output.log(f"[ERROR] Invalid yaml file '{path}' (YAMLError)", level=LogLevel.ERROR)
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    if errors and output:
        output.log(f"[ERROR] Errors while parsing scenarios: {errors}")

    return coverage_by_scenario


# =====================================================================
# === Main Runner (Windows-safe, robust group/scenario discovery) =====
# =====================================================================
def run_covernator(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    output = OutputManager(folder_to_save_files_in)
    output.log("📣 run_covernator() started!", level=LogLevel.INFO)

    stats = {
        "total_cases": 0,
        "total_scenarios": 0,
        "total_groups": 0,
    }

    all_cases, all_scenarios = [], []
    scenario_roots_seen = set()
    seen_groups = set()
    logged_messages = set()

    # --- Detect subsystem folders (a "subsystem" is any folder with /tests) ---
    def is_subsystem_folder(path: Path) -> bool:
        return (path / "tests").exists()

    subsystems = [base_path] if is_subsystem_folder(base_path) else [
        p for p in base_path.iterdir() if p.is_dir() and is_subsystem_folder(p)
    ]

    for subsystem_dir in subsystems:
        subsystem = subsystem_dir.name
        tests_dir = subsystem_dir / "tests"

        # --- Identify potential group directories ---
        excluded_dirs = {"coverage", "scenario_tests"}
        group_dirs = [d for d in tests_dir.iterdir() if d.is_dir() and d.name not in excluded_dirs]

        # Flat repo fallback: if /tests itself has coverage, treat /tests as a single pseudo-group
        if (tests_dir / "coverage").exists():
            group_dirs.append(tests_dir)

        for group_dir in group_dirs:
            coverage_dir = group_dir / "coverage"
            group = group_dir.name if group_dir != tests_dir else None
            key = f"{subsystem}/{group}" if group else subsystem

            seen_groups.add(key)
            output.log(
                f"[{subsystem}]{f'[{group}]' if group else ''} Processing {'group' if group else 'subsystem'}: {group or subsystem}",
                level=LogLevel.INFO,
            )

            # --- Identify coverage and scenario folders
            all_cases_path = next((p for p in coverage_dir.glob("all_cases*.yml")), None)
            scenarios_path = next((p for p in group_dir.glob("scenario_test*") if p.is_dir()), None)

            # --- Coverage folder validation (presence + all_cases)
            if not coverage_dir.exists():
                output.log(f"[{subsystem}][{group}] Missing coverage folder", level=LogLevel.ERROR)

            if not all_cases_path:
                output.log(
                    f"[{subsystem}]{f'[{group}]' if group else ''} Missing all_cases YAML file — scenario_test(s) exist but no all_cases*.yml found.",
                    level=LogLevel.ERROR,
                )
                group_cases = []
            else:
                group_cases = find_all_cases(all_cases_path, output=output, group=group)

            # --- Scenario detection (list roots with tests/*.py and coverage yaml)
            # --- Scenario detection (robust: search recursively for any coverage_mapping.yml)
            group_scenarios = find_all_scenarios(group_dir, output=output)

            if not group_scenarios:
                output.log(
                    f"[{subsystem}]{f'[{group}]' if group else ''} No scenarios found under {group_dir} (no coverage_mapping.yml files).",
                    level=LogLevel.ERROR,
                )
            else:
                output.log(
                    f"[{subsystem}]{f'[{group}]' if group else ''} Found {len(group_scenarios)} scenario(s).",
                    level=LogLevel.INFO,
                )

                # Parse all scenario mappings under this scenarios root
                found_scenarios = find_all_scenarios(scenarios_path, output=output)
                group_scenarios.extend(found_scenarios)

            # --- Aggregate cases and scenarios for CSVs ---
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

    # --- Build DataFrames ---
    df_all_cases = (
        pl.DataFrame(all_cases)
        if all_cases else pl.DataFrame(schema={"Group": str, "TestCase": str, "Path": str, "Implemented": bool})
    )
    df_all_scenarios = (
        pl.DataFrame(all_scenarios)
        if all_scenarios else pl.DataFrame(schema={"Group": str, "Scenario": str, "CaseCoverage": str})
    )

    # --- Expected vs. Scenario sets ---
    expected_cases = (
        set(df_all_cases.filter(pl.col("Implemented") == True)["TestCase"].to_list())
        if df_all_cases.height > 0 else set()
    )
    false_cases = (
        set(df_all_cases.filter(pl.col("Implemented") == False)["TestCase"].to_list())
        if df_all_cases.height > 0 else set()
    )
    scenario_cases = set(df_all_scenarios["CaseCoverage"].to_list()) if df_all_scenarios.height > 0 else set()

    # --- Deduplicate error messages (cases) ---
    seen = set()
    for case_row in df_all_cases.iter_rows(named=True):
        key = (case_row["Group"], case_row["TestCase"])
        if key in seen:
            msg = f"[{case_row['Group']}] Duplicate items in all cases: {case_row['TestCase']}"
            if msg not in logged_messages:
                output.log(msg, level=LogLevel.ERROR)
                logged_messages.add(msg)
        else:
            seen.add(key)

    # --- Deduplicate error messages (scenarios) ---
    seen = set()
    for scenario_row in df_all_scenarios.iter_rows(named=True):
        key = (scenario_row["Group"], scenario_row["Scenario"], scenario_row["CaseCoverage"])
        if key in seen:
            msg = f"[{scenario_row['Group']}] Duplicate items in scenario [{scenario_row['Scenario']}]: {scenario_row['CaseCoverage']}"
            if msg not in logged_messages:
                output.log(msg, level=LogLevel.ERROR)
                logged_messages.add(msg)
        else:
            seen.add(key)

    # --- Coverage validation ---
    for case_row in df_all_cases.iter_rows(named=True):
        if not case_row["Implemented"]:
            output.log(f"[{case_row['Group']}] Case is marked as false in master list: {case_row['TestCase']}", level=LogLevel.INFO)
            continue
        if case_row["TestCase"] not in scenario_cases:
            msg = f"[{case_row['Group']}] Case not covered in any scenario: {case_row['TestCase']}"
            if msg not in logged_messages:
                output.log(msg, level=LogLevel.ERROR)
                logged_messages.add(msg)

    # --- Unexpected or false coverage ---
    logged_false = set()
    for scenario_row in df_all_scenarios.iter_rows(named=True):
        case = scenario_row["CaseCoverage"]
        group = scenario_row["Group"]
        scenario = scenario_row["Scenario"]

        if case in false_cases:
            if (group, case) not in logged_false:
                msg = f"[{group}] Case found in scenario [{scenario}] is marked as false in master list: {case}"
                if msg not in logged_messages:
                    output.log(msg, level=LogLevel.ERROR)
                    logged_messages.add(msg)
                logged_false.add((group, case))
        elif case not in expected_cases:
            msg = f"[{group}] Case found in scenario [{scenario}] not included in master list: {case}"
            if msg not in logged_messages:
                output.log(msg, level=LogLevel.ERROR)
                logged_messages.add(msg)

    # --- Output writing ---
    # NOTE: tests read these exact files/schemas
    output.write_csv("all_cases.csv",
        df_all_cases.select(["Group", "Path", "TestCase", "Implemented"])
        if df_all_cases.height > 0
        else pl.DataFrame(schema={"Group": str, "Path": str, "TestCase": str, "Implemented": bool})
    )
    output.write_csv("case_coverage.csv",
        df_all_scenarios.select(["Group", "Scenario", "CaseCoverage"])
        if df_all_scenarios.height > 0
        else pl.DataFrame(schema={"Group": str, "Scenario": str, "CaseCoverage": str})
    )

    # --- Final stats (tests assert these) ---
    stats = {
        "total_cases": df_all_cases.height,
        "total_scenarios": len(df_all_scenarios.select("Scenario").unique()) if df_all_scenarios.height > 0 else 0,
        "total_groups": len(seen_groups),
    }

    output.finalize(stats)
