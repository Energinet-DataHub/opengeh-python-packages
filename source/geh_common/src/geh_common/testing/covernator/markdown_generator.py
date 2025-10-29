import logging
import os
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

# ----------------------------------------------------------
# Debug helper (quiet by default; enable with COVERNATOR_DEBUG=1)
# ----------------------------------------------------------
logger = logging.getLogger(__name__)
if os.getenv("COVERNATOR_DEBUG"):
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
else:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


# ==========================================================
# === Helpers ==============================================
# ==========================================================
def normalize_group_name(raw: str, prefix: str = "geh_") -> str:
    if prefix and raw.startswith(prefix):
        raw = raw[len(prefix) :]
    raw = raw.replace("_", " ").strip()
    return " ".join(word.capitalize() for word in raw.split())


def group_cases_by_group(cases: List[CaseInfo]) -> Dict[str, List[CaseInfo]]:
    grouped: Dict[str, List[CaseInfo]] = {}
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
) -> None:
    output: List[str] = []
    logger.debug("🔧 [DEBUG] Markdown generation started")

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

        logger.debug("🧩 [DEBUG] Processing group: %s (%s)", group, group_key_normalized)

        # Determine if group has any errors (for header emoji)
        header_emoji = "📁"
        for e in results.error_logs:
            tags = extract_bracket_tags(e.message)
            # Normalize tags and candidate group names for robust comparison
            normalized_tags = {t.replace(" ", "_") for t in tags} | {t.replace("_", " ") for t in tags}
            candidate_names = {
                group_normalized,
                group_normalized.replace("_", " "),
                group_normalized.replace(" ", "_"),
                group_key_normalized,
                group_key_normalized.replace("_", " "),
                group_key_normalized.replace(" ", "_"),
                f"geh_calculated_measurements/{group_key_normalized}",
                f"calculated_measurements/{group_key_normalized}",
            }
            if normalized_tags & candidate_names:
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
                f"| {case.path.strip()} | {case.case.strip()} | {impl_icon} {case.implemented} | {covered_icon} {covered} |"
            )

        output.append("")

        # --- Group-specific errors ---
        errors: List[str] = []

        # Define equivalent name forms for matching (full, partial, short)
        all_group_aliases = {
            group_normalized,
            group_key_normalized,
            f"geh_calculated_measurements/{group_key_normalized}",
            f"calculated_measurements/{group_key_normalized}",
            group_key_normalized.split("/")[-1],
        }
        logger.debug("   [DEBUG] Alias set for %s: %s", group, all_group_aliases)

        for e in results.error_logs:
            tags = extract_bracket_tags(e.message)
            if tags:
                logger.debug("   [DEBUG] Tags in '%s...': %s", e.message[:60], tags)
            if any(tag in all_group_aliases for tag in tags):
                errors.append(e.message)
                logger.debug("✅ [DEBUG] Matched error for %s: %s", group, e.message)

        if errors:
            output.append(f"### ❌ {group_title} Coverage Errors")
            for err in errors:
                output.append(f"- {err}")
            output.append("")
        else:
            logger.debug("⚠️ [DEBUG] No errors found for %s", group)

    # ==========================================================
    # === Global logs (bottom of markdown) =====================
    # ==========================================================
    output.append("# 📟 Logs\n")

    # --- Info Logs ---
    output.append("## 📣 Info Logs")
    if results.info_logs:
        for log in results.info_logs:
            output.append(f"- {log.message}")
    else:
        output.append("_No info logs_")

    output.append("")
    output.append("## ❌ Other Errors (not linked to specific groups)")

    # --- Build alias set for all known groups (used to exclude assigned errors) ---
    known_groups_full = {case.group.strip().lower() for case in results.all_cases}
    known_aliases = set()
    for g in known_groups_full:
        short = g.split("/", 1)[-1]
        known_aliases |= {
            g,
            short,
            f"geh_calculated_measurements/{short}",
            f"calculated_measurements/{short}",
        }

    # --- Identify unassigned errors ---
    other_errors: List[str] = [
        err.message
        for err in results.error_logs
        if not any(t in known_aliases for t in extract_bracket_tags(err.message))
    ]

    if other_errors:
        for err in other_errors:
            output.append(f"- {err}")
    else:
        output.append("_No other errors_")

    # --- Write file ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(output), encoding="utf-8")
    logger.debug("✅ [DEBUG] Markdown generation completed successfully.")
