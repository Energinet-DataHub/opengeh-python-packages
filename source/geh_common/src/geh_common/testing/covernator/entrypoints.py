import logging
from pathlib import Path

from geh_common.testing.covernator.commands import run_covernator
from geh_common.testing.covernator.markdown_generator import generate_markdown_from_results

# Configure root logger for CI visibility
logging.basicConfig(level=logging.INFO, format="%(message)s")


def run_and_generate_markdown(project_path: str, output_folder: str = "docs/covernator") -> None:
    """Run Covernator and generate markdown summary."""
    logging.info(f"🔍 Running Covernator analysis on: {project_path}")
    results = run_covernator(
        base_path=Path(project_path),
        folder_to_save_files_in=Path(output_folder),
    )

    # ==========================================
    # 🔍 DIAGNOSTIC: Log raw run_covernator output
    # ==========================================
    stats = results.stats
    unique_scenarios = {(cm.group, cm.scenario) for cm in results.coverage_map}
    logging.info("📊 --- RAW COVERNATOR RESULTS ---")
    logging.info(f"🧩 Total Groups: {stats.total_groups}")
    logging.info(f"📁 Total Cases: {stats.total_cases}")
    logging.info(f"🧠 Total Scenarios (in stats): {stats.total_scenarios}")
    logging.info(f"🔍 Unique Scenarios found in coverage_map: {len(unique_scenarios)}")
    logging.info(f"🧾 Example Scenarios: {list(sorted(unique_scenarios))[:5]}")
    logging.info(f"ℹ️ Info logs: {len(results.info_logs)} | ❌ Error logs: {len(results.error_logs)}")
    logging.info("📊 --- END OF RAW RESULTS ---")

    output_path = Path(output_folder) / "coverage_overview.md"
    logging.info(f"🧾 Writing markdown to: {output_path}")

    generate_markdown_from_results(results, output_path=output_path)
    logging.info("✅ Markdown coverage overview generated successfully.")
