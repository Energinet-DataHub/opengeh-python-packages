import re
from collections import defaultdict
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
    """Group CoverageMapping entries by (group, case), and counts unique scenarios."""
    coverage_dict = defaultdict(set)
    for entry in coverage_map:
        key = (entry.group.lower().strip(), entry.case.lower().strip())
        coverage_dict[key].add(entry.scenario.strip().lower())
    return {k: len(v) for k, v in coverage_dict.items()}


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

    # Group CaseInfo entries by group
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

        # Group-specific errors using bracket tag extraction
        def extract_bracket_tags(msg: str) -> list[str]:
            return [m.group(1).strip().lower() for m in re.finditer(r"\[([^\[\]]+)\]", msg)]

        group_normalized = group.strip().lower()
        group_key_normalized = group_key.strip().lower()

        errors = [
            e.message
            for e in results.error_logs
            if any(
                tag == group_normalized or tag == group_key_normalized or tag.endswith(f"/{group_key_normalized}")
                for tag in extract_bracket_tags(e.message)
            )
        ]

        if errors:
            output.append(f"### ❌ {group_title} Coverage Errors")
            for err in errors:
                output.append(f"- {err}")
            output.append("")

    # Global Logs (only once, after all groups)
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

    # Build set of known groups
    known_groups = {case.group.strip() for case in results.all_cases}

    # Compile patterns for all handled groups
    known_group_patterns = [
        re.compile(
            rf"\[(?:[a-z_]+/)?{re.escape(group)}\]|\[{re.escape(group.split('/')[-1])}\]",
            re.IGNORECASE,
        )
        for group in known_groups
    ]

    # Collect unmatched errors
    other_errors = []
    for err in results.error_logs:
        if not any(p.search(err.message) for p in known_group_patterns):
            other_errors.append(err.message)

    if other_errors:
        for err in other_errors:
            output.append(f"- {err}")
    else:
        output.append("_No other errors_")

    # Final write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(output), encoding="utf-8")
