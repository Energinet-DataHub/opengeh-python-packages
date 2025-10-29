import zoneinfo
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from geh_common.testing.covernator.models import CovernatorResults


def normalize_group_name(raw: str, prefix: str = "") -> str:
    clean = raw
    if prefix and raw.startswith(prefix):
        clean = raw[len(prefix) :]
    clean = clean.replace("_", " ")
    return " ".join(word.capitalize() for word in clean.split())


def generate_markdown_from_results(results: CovernatorResults, output_path: Path, group_prefix: str = "geh_"):
    output_lines = []
    tz = zoneinfo.ZoneInfo("Europe/Copenhagen")
    now_cet = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    subsystem_display = normalize_group_name(group_prefix.rstrip("/"), "")

    # Header
    output_lines.append(f"# 🧩 Covernator Coverage Overview for {subsystem_display}\n")
    output_lines.append(f"Generated: {now_cet}\n")

    # Summary
    output_lines.append("## 📊 Summary")
    output_lines.append("| Metric | Value |")
    output_lines.append("|--------|--------|")
    output_lines.append(f"| 🧾 Total Cases | {results.stats.total_cases} |")
    output_lines.append(f"| 🧠 Total Scenarios | {results.stats.total_scenarios} |")
    output_lines.append(f"| 🗂️ Total Groups | {results.stats.total_groups} |")

    implemented = sum(1 for case in results.all_cases if case.implemented)
    total_csv_cases = len(results.all_cases)
    if total_csv_cases > 0:
        coverage_pct = round((implemented / total_csv_cases) * 100, 1)
    else:
        coverage_pct = 0.0
    output_lines.append(f"| ⚙️ Implemented Cases | {implemented} / {total_csv_cases} ({coverage_pct}%) |")
    output_lines.append("")

    # Coverage mapping: (group, case) -> scenario count
    coverage_counts = defaultdict(int)
    for m in results.coverage_map:
        coverage_counts[(m.group.lower(), m.case.lower())] += m.scenario_count

    # Errors per group
    group_errors = defaultdict(list)
    other_errors = []
    known_groups = {case.group for case in results.all_cases}

    for err in results.error_logs:
        matched = False
        for group in known_groups:
            if f"[{group}]" in err.message:
                short_group = group[len(group_prefix) :] if group.startswith(group_prefix) else group
                group_errors[short_group].append(f"- {err.message}")
                matched = True
                break
        if not matched:
            other_errors.append(f"- {err.message}")

    # Sort by group name
    groups_sorted = sorted({case.group for case in results.all_cases})
    for grp in groups_sorted:
        group_display = normalize_group_name(grp, group_prefix)
        short_group = grp[len(group_prefix) :] if grp.startswith(group_prefix) else grp

        if group_errors.get(short_group):
            output_lines.append(f"## 🚨 {group_display}")
        else:
            output_lines.append(f"## 📁 {group_display}")

        output_lines.append("### Case overview")
        output_lines.append("| Path | Case | Implemented | Covered by # scenarios |")
        output_lines.append("|----------|-----------|-------------|-------------|")

        for case in results.all_cases:
            if case.group != grp:
                continue
            key = (case.group.lower(), case.case.lower())
            covered = coverage_counts.get(key, 0)

            emoji = "⚠️" if covered == 0 else "✅"
            impl_icon = "🧩" if case.implemented else "⚠️"
            output_lines.append(f"| {case.path} | {case.case} | {impl_icon} {case.implemented} | {emoji} {covered} |")

        output_lines.append("")  # spacing

        if group_errors.get(short_group):
            output_lines.append(f"### ❌ {group_display} Coverage Errors")
            output_lines.extend(group_errors[short_group])
            output_lines.append("")

    # Logs section
    output_lines.append("# 🧾 Logs")
    output_lines.append("")
    output_lines.append("## 📣 Info Logs")
    if results.info_logs:
        output_lines.extend(f"- {log.message}" for log in results.info_logs)
    else:
        output_lines.append("_No info logs_")

    output_lines.append("")
    output_lines.append("## ❌ Other Errors (not linked to specific groups)")
    if other_errors:
        output_lines.extend(other_errors)
    else:
        output_lines.append("_No other errors_")

    output_path.write_text("\n".join(output_lines), encoding="utf-8")
    print(f"✅ Markdown coverage overview written to: {output_path}")
