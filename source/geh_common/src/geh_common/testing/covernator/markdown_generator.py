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


# ==========================================================
# === Helpers ==============================================
# ==========================================================
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
    """Group CoverageMapping entries by (group, case), and count unique scenarios."""
    coverage_dict = defaultdict(set)
    for entry in coverage_map:
        key = (entry.group.lower().strip(), entry.case.lower().strip())
        coverage_dict[key].add(entry.scenario.strip().lower())
    return {k: len(v) for k, v in coverage_dict.items()}


def extract_bracket_tags(msg: str) -> list[str]:
    """Extract all bracketed tags from log messages."""
    return [m.group(1).strip().lower() for m in re.finditer(r"\[([^\[\]]+)\]", msg)]


# ==========================================================
# === Markdown Generator ===================================
# ==========================================================
def generate_markdown_from_results(
    results: CovernatorResults,
    output_path: Path,
    group_prefix: str = "geh_",
):
    output = []

    # --- Header ---
    normalized_prefix = normalize_group_name(group_prefix.rstrip("/"))
    output.append(f"# 🔩 Covernator Coverage Overview for {normalized_prefix}\n")

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    output.append(f"Generated: {now}\n")

    # --- Summary Section ---
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

    # --- Mapping: (group, case) -> scenario count ---
    coverage_dict = get_coverage_dict(results.coverage_map)
    grouped_cases = group_cases_by_group(results.all_cases)

    # ==========================================================
    # === Per-group section ====================================
    # ==========================================================
    for group, cases in grouped_cases.items():
        group_title = normalize_group_name(group, group_prefix)
        group_key = group[len(group_prefix) :] if group.startswith(group_prefix) else group
        group_normalized = group.strip().lower()
        group_key_normalized = group_key.strip().lower()

        # Determine if group has any errors
        header_emoji = "📁"
        for e in results.error_logs:
            tags = extract_bracket_tags(e.message)
            if any(
                g in tags
                for g in [group_normalized, group_key_normalized, f"geh_calculated_measurements/{group_key_normalized}"]
            ):
                header_emoji = "🚨"
                break

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

        # --- Group-specific errors (robust, tag-baseret) ---
        errors = []
        for e in results.error_logs:
            tags = extract_bracket_tags(e.message)  # fx ["error", "net_consumption_group_6"]
            if any(
                t in tags
                for t in (
                    group_normalized,  # "geh_calculated_measurements/net_consumption_group_6"
                    group_key_normalized,  # "net_consumption_group_6"
                    f"geh_calculated_measurements/{group_key_normalized}",
                )
            ):
                errors.append(e.message)

    # ==========================================================
    # === Global logs ==========================================
    # ==========================================================
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

    # --- Collect errors not matched to any known group ---
    known_groups_full = {case.group.strip().lower() for case in results.all_cases}
    known_groups_short = {g.split("/", 1)[-1] for g in known_groups_full}  # fx "net_consumption_group_6"

    other_errors = []
    for err in results.error_logs:
        tags = extract_bracket_tags(err.message)
        is_group_tag = any(
            t in tags or f"geh_calculated_measurements/{t}" in tags for t in (known_groups_full | known_groups_short)
        )
        if not is_group_tag:
            other_errors.append(err.message)

    if other_errors:
        for err in other_errors:
            output.append(f"- {err}")
    else:
        output.append("_No other errors_")

    # --- Write file ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(output), encoding="utf-8")
