import logging
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl
import yaml

from geh_common.testing.covernator.models import CaseInfo, CoverageMapping, CoverageStats, CovernatorResults, LogEntry


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
    """Centralized output manager for Covernator.

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
        """Record a message with a specified log level."""
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
        """Write CSV files to the output directory."""
        data.write_csv(self.output_dir / filename)

    def finalize(self, stats: dict):
        """Write final stats.

        stats.json containing:
        - total counts
        - info/error message groups.
        """
        import json

        output = {
            "total_cases": stats.get("total_cases", 0),
            "total_scenarios": stats.get("total_scenarios", 0),
            "total_groups": stats.get("total_groups", 0),
            "logs": {
                "info": self.logs_info,
                "error": self.logs_error,
            },
            "generated_at": self.start_time,
        }

        # ✅ FIX: Must use "w" for write mode
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
                    self.output.log(
                        f"[{self.group}] Duplicate items in scenario [{self.scenario}]: {key}",
                        level=LogLevel.ERROR,
                    )
                elif self.group:
                    self.output.log(
                        f"[{self.group}] Duplicate items in all cases: {key}",
                        level=LogLevel.ERROR,
                    )
                else:
                    # 🔧 FIX: Include domain/group name even for root-level YAML duplicates
                    domain = self.group or "unknown_domain"
                    self.output.log(
                        f"[{domain}] Duplicate key found in YAML: {key}",
                        level=LogLevel.ERROR,
                    )
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def load_yaml_with_duplicates(path: Path, output: OutputManager, group: str | None = None, scenario: str | None = None):
    """Load YAML while detecting duplicate keys, logging them via OutputManager."""

    class _DuplicateKeyLoader(DuplicateKeyLoader):  # type: ignore[misc]
        def __init__(self, stream):
            super().__init__(stream, group=group, scenario=scenario, output=output)

    with open(path, encoding="utf-8") as f:
        # Pass the Loader *type*, not a callable
        return yaml.load(f, Loader=_DuplicateKeyLoader)


# =====================================================================
# === Case Extraction =================================================
# =====================================================================


def _get_case_rows_from_main_yaml(
    main_yaml_content: Dict[str, Dict | bool],
    prefix: str = "",
    group: str | None = None,
) -> List[CaseInfo]:
    """Recursively parse the master all_cases.yml structure and produce a flat list of CaseInfo entries.

    Each CaseInfo represents a single testable case (leaf node).
    """
    case_rows: List[CaseInfo] = []

    # Ensure group is always a string (for CaseInfo typing)
    safe_group = group or ""

    for scenario, content in main_yaml_content.items():
        # If the node value is a boolean, it's a leaf case definition
        if isinstance(content, bool):
            case_rows.append(
                CaseInfo(
                    path=prefix.strip(),
                    case=scenario.strip(),
                    implemented=content,
                    group=safe_group,  # ✅ always a string now
                )
            )
        elif isinstance(content, dict):
            new_prefix = f"{prefix} / {scenario}" if prefix else scenario
            case_rows.extend(
                _get_case_rows_from_main_yaml(
                    main_yaml_content=content,
                    prefix=new_prefix,
                    group=group,  # still allowed as None; inner call normalizes too
                )
            )

    return case_rows


def find_all_cases(
    main_yaml_path: Path,
    output: "OutputManager | None" = None,
    group: str | None = None,
) -> List[CaseInfo]:
    """Load all test case entries from a YAML file into a list of CaseInfo objects."""
    if not main_yaml_path.exists():
        raise FileNotFoundError(f"File {main_yaml_path} does not exist.")

    # Load YAML content (with or without duplicate-handling)
    if output:
        main_yaml_content = load_yaml_with_duplicates(main_yaml_path, output=output, group=group)
    else:
        with open(main_yaml_path, encoding="utf-8") as f:
            main_yaml_content = yaml.safe_load(f)

    if not isinstance(main_yaml_content, dict):
        return []

    return _get_case_rows_from_main_yaml(main_yaml_content, group=group)


# =====================================================================
# === Scenario Extraction =============================================
# =====================================================================


def _get_scenario_source_name_from_path(path: Path, feature_folder_name: Path) -> str:
    """Return the relative source name of a scenario file within its feature folder."""
    return str(path.relative_to(feature_folder_name).parent)


def _get_scenarios_cases_tested(content, parents=None) -> List[Tuple[List[str], str]]:
    """Recursively extract case names from a nested 'cases_tested' structure."""
    if parents is None:
        parents = []

    if isinstance(content, dict):
        results = []
        for key, value in content.items():
            results.extend(_get_scenarios_cases_tested(value, parents + [key]))
        return results

    if isinstance(content, list):
        results = []
        for case in content:
            results.extend(_get_scenarios_cases_tested(case, parents))
        return results

    # Base case: content is a leaf string
    return [(parents, str(content))]


def find_all_scenarios(
    base_path: Path,
    output: "OutputManager | None" = None,
) -> List[CoverageMapping]:
    """Find all scenario test coverage mappings recursively under base_path.

    Each 'coverage_mapping.yml' file contributes one or more CoverageMapping entries,
    linking a test group and a case to a scenario path.
    """
    coverage_by_scenario: List[CoverageMapping] = []
    errors: List[str] = []

    for path in base_path.rglob("coverage_mapping.yml"):
        try:
            if output:
                coverage_mapping = load_yaml_with_duplicates(
                    path, output=output, group=base_path.name, scenario=path.stem
                )
            else:
                with open(path, encoding="utf-8") as f:
                    coverage_mapping = yaml.safe_load(f)

            cases_tested_content = coverage_mapping.get("cases_tested") if coverage_mapping else None
            if cases_tested_content is None:
                if output:
                    output.log(
                        f"[ERROR] Invalid yaml file '{path}': 'cases_tested' key not found.",
                        level=LogLevel.ERROR,
                    )
                continue

            cases_tested = _get_scenarios_cases_tested(cases_tested_content)
            scenario_source = _get_scenario_source_name_from_path(path, base_path)
            group_name = base_path.name.strip().lower()

            # Convert cases to CoverageMapping entries
            for _, case_name in cases_tested:
                coverage_by_scenario.append(
                    CoverageMapping(
                        group=group_name,
                        case=case_name.strip(),
                        scenario=scenario_source,
                    )
                )

        except yaml.YAMLError:
            if output:
                output.log(f"[ERROR] Invalid yaml file '{path}' (YAMLError)", level=LogLevel.ERROR)
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    if errors and output:
        output.log(f"[ERROR] Errors while parsing scenarios: {errors}", level=LogLevel.ERROR)

    return coverage_by_scenario


# =====================================================================
# === Main Runner =====================================================
# =====================================================================
def run_covernator(folder_to_save_files_in: Path, base_path: Path = Path(".")):
    output = OutputManager(folder_to_save_files_in)
    output.log("📣 run_covernator() started!", level=LogLevel.INFO)

    all_cases, all_scenarios = [], []
    seen_groups, seen_scenarios, logged_messages = set(), set(), set()

    def is_subsystem_folder(path: Path) -> bool:
        return (path / "tests").exists()

    subsystems = (
        [base_path]
        if is_subsystem_folder(base_path)
        else [p for p in base_path.iterdir() if p.is_dir() and is_subsystem_folder(p)]
    )

    for subsystem_dir in subsystems:
        subsystem = subsystem_dir.name
        tests_dir = subsystem_dir / "tests"
        excluded_dirs = {"coverage", "scenario_tests"}
        group_dirs = [d for d in tests_dir.iterdir() if d.is_dir() and d.name not in excluded_dirs]
        if (tests_dir / "coverage").exists():
            group_dirs.append(tests_dir)

        for group_dir in group_dirs:
            coverage_dir = group_dir / "coverage"
            group = group_dir.name if group_dir != tests_dir else None
            key = f"{subsystem}/{group}" if group else subsystem
            seen_groups.add(key)
            output.log(
                f"[{subsystem}]{f'[{group}]' if group else ''} Processing {'group' if group else 'subsystem'}: {group or subsystem}"
            )

            all_cases_path = next((p for p in coverage_dir.glob("all_cases*.yml")), None)
            scenarios_path = next((p for p in group_dir.glob("scenario_test*") if p.is_dir()), None)

            if not coverage_dir.exists():
                output.log(f"[{subsystem}][{group}] Missing coverage folder", level=LogLevel.ERROR)

            if not all_cases_path:
                output.log(
                    f"[{subsystem}]{f'[{group}]' if group else ''} Missing all_cases YAML file — scenario_test(s) exist but no all_cases*.yml found.",
                    level=LogLevel.ERROR,
                )
                group_cases = []
            else:
                group_cases = find_all_cases(all_cases_path, output=output, group=group or subsystem)

            group_scenarios = []

            if not scenarios_path:
                output.log(
                    f"[{subsystem}]{f'[{group}]' if group else ''} Could not find 'scenario_test(s)' folder.",
                    level=LogLevel.ERROR,
                )
            else:
                for scenario_folder in scenarios_path.rglob("*"):
                    if not scenario_folder.is_dir():
                        continue

                    has_test_file = any(
                        f.name.startswith("test_") and f.suffix == ".py" for f in scenario_folder.iterdir()
                    )
                    has_coverage_yaml = (scenario_folder / "coverage_mapping.yml").exists()

                    if has_test_file:
                        rel = scenario_folder.relative_to(scenarios_path)
                        scenario_rel_path = str(rel)
                        if (key, scenario_rel_path) not in seen_scenarios:
                            seen_scenarios.add((key, scenario_rel_path))
                        if not has_coverage_yaml:
                            msg = f"[{key}] Scenario folder '{scenario_folder.name}' is missing coverage_mapping.yml"
                            if msg not in logged_messages:
                                output.log(msg, level=LogLevel.ERROR)
                                logged_messages.add(msg)

                found_scenarios = find_all_scenarios(scenarios_path, output=output)
                group_scenarios.extend(found_scenarios)

            all_cases.extend(
                {
                    "Group": key,
                    "TestCase": c.case,
                    "Path": str(c.path),
                    "Implemented": c.implemented,
                }
                for c in group_cases
            )

            all_scenarios.extend(
                {
                    "Group": key,
                    "Scenario": s.scenario,
                    "CaseCoverage": s.case,
                }
                for s in group_scenarios
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

    expected_cases = (
        set(df_all_cases.filter(pl.col("Implemented"))["TestCase"].to_list()) if df_all_cases.height > 0 else set()
    )
    false_cases = (
        set(df_all_cases.filter(~pl.col("Implemented"))["TestCase"].to_list()) if df_all_cases.height > 0 else set()
    )
    scenario_cases = set(df_all_scenarios["CaseCoverage"].to_list()) if df_all_scenarios.height > 0 else set()

    # Duplicate checks
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

    # Coverage validation
    for case_row in df_all_cases.iter_rows(named=True):
        if not case_row["Implemented"]:
            output.log(f"[{case_row['Group']}] Case is marked as false in master list: {case_row['TestCase']}")
            continue
        if case_row["TestCase"] not in scenario_cases:
            msg = f"[{case_row['Group']}] Case not covered in any scenario: {case_row['TestCase']}"
            if msg not in logged_messages:
                output.log(msg, level=LogLevel.ERROR)
                logged_messages.add(msg)

    logged_false = set()
    for s_row in df_all_scenarios.iter_rows(named=True):
        case, group, scenario = s_row["CaseCoverage"], s_row["Group"], s_row["Scenario"]
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

        results = CovernatorResults(
            stats=CoverageStats(
                total_cases=df_all_cases.height,
                total_scenarios=len(seen_scenarios),
                total_groups=len(seen_groups),
            ),
            info_logs=[
                LogEntry(timestamp=log_entry["timestamp"], message=log_entry["message"])
                for log_entry in output.logs_info
            ],
            error_logs=[
                LogEntry(timestamp=log_entry["timestamp"], message=log_entry["message"])
                for log_entry in output.logs_error
            ],
            all_cases=[
                CaseInfo(
                    group=row["Group"],
                    path=row["Path"],
                    case=row["TestCase"],
                    implemented=row["Implemented"],
                )
                for row in df_all_cases.iter_rows(named=True)
            ],
            coverage_map=[
                CoverageMapping(
                    group=row["Group"],
                    case=row["CaseCoverage"],
                    scenario=row["Scenario"],  # ✅ Add per-scenario
                )
                for row in df_all_scenarios.iter_rows(named=True)
            ],
        )

    return results
