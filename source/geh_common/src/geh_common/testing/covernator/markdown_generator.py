import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from geh_common.testing.covernator.models import (
    CaseInfo,
    CoverageMapping,
    CovernatorResults,
)


def normalize_group_name(raw: str, prefix: str = "geh_") -> str:
    if prefix and raw.startswith(prefix):
        raw = raw[len(prefix) :]
    raw = raw.replace("_", " ").strip()
    return " ".join(word.capitalize() for word in raw.split())


def group_cases_by_group(cases: List[CaseInfo]) -> Dict[str, List[CaseInfo]]:
    grouped = {}
    for case in cases:
        group = case.group.strip()
        grouped.setdefault(group, []).append(case)
    return grouped


def get_coverage_dict(coverage_map: List[CoverageMapping]) -> Dict[tuple, int]:
    return {(entry.group.lower().strip(), entry.case.lower().strip()): entry.scenario_count for entry in coverage_map}


def generate_markdown_from_results(
    results: CovernatorResults,
    output_path: Path,
    group_prefix: str = "geh_",
):
    output = []

    # H1 Header
    normalized_prefix = normalize_group_name(group_prefix.rstrip("/"))
    output.append(f"# 🔩 Covernator Coverage Overview for {normalized_prefix}\n")

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    output.append(f"Generated: {now}\n")

    # Summary Section
    total_cases = len(results.all_cases)
    implemented_cases = sum(1 for case in results.all_cases if case.implemented)
    coverage_pct = f"{(implemented_cases / total_cases * 100):.1f}" if total_cases > 0 else "0.0"

    output.extend(
        [
            "\n## 📊 Summary",
            "| Metric | Value |",
            "|--------|--------|",
            f"| 📟 Total Cases | {results.stats.total_cases} |",
            f"| 🧠 Total Scenarios | {results.stats.total_scenarios} |",
            f"| 💂️ Total Groups | {results.stats.total_groups} |",
            f"| ⚙️ Implemented Cases | {implemented_cases} / {total_cases} ({coverage_pct}%) |",
            "",
        ]
    )

    # Mapping: (group, case) -> scenario count
    coverage_dict = get_coverage_dict(results.coverage_map)

    grouped_cases = group_cases_by_group(results.all_cases)

    for group, cases in grouped_cases.items():
        group_title = normalize_group_name(group, group_prefix)
        group_key = group[len(group_prefix) :] if group.startswith(group_prefix) else group

        # Emoji based on errors
        header_emoji = "🚨" if any(group_key in e.message for e in results.error_logs) else "📁"
        output.append(f"## {header_emoji} {group_title}")

        output.append("### Case overview")
        output.append("| Path | Case | Implemented | Covered by # scenarios |")
        output.append("|----------|-----------|-------------|-------------|")

        for case in cases:
            case_key = (case.group.strip().lower(), case.case.strip().lower())
            covered = coverage_dict.get(case_key, 0)
            covered_icon = "✅" if covered > 0 else "⚠️"
            impl_icon = "🧩" if case.implemented else "⚠️"
            output.append(
                f"| {case.path.strip()} | {case.case.strip()} | {impl_icon} {str(case.implemented)} | {covered_icon} {covered} |"
            )

        output.append("")

        # Group-specific errors
        # Match logs like:
        # [geh_calculated_measurements/net_consumption_group_6]
        # or [net_consumption_group_6]
        group_pattern = re.compile(
            rf"\[(?:[a-z_]+/)?{re.escape(group)}\]|\[{re.escape(group_key)}\]",
            re.IGNORECASE,
        )

        errors = [e.message for e in results.error_logs if group_pattern.search(e.message)]

        if errors:
            output.append(f"### ❌ {group_title} Coverage Errors")
            for err in errors:
                output.append(f"- {err}")
            output.append("")

    # Global Logs
    output.extend(
        [
            "# 📟 Logs",
            "",
            "## 📣 Info Logs",
        ]
    )
    if results.info_logs:
        for log in results.info_logs:
            output.append(f"- {log.message}")
    else:
        output.append("_No info logs_")

    output.append("")
    output.append("## ❌ Other Errors (not linked to specific groups)")
    other_errors = [e.message for e in results.error_logs if "[geh_" not in e.message and "]" not in e.message]
    if other_errors:
        for err in other_errors:
            output.append(f"- {err}")
    else:
        output.append("_No other errors_")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(output), encoding="utf-8")
