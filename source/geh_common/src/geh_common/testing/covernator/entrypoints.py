import logging
from pathlib import Path

from geh_common.testing.covernator.commands import run_covernator
from geh_common.testing.covernator.markdown_generator import generate_markdown_from_results

# Configure root logger for CI visibility
logging.basicConfig(level=logging.INFO, format="%(message)s")


def run_and_generate_markdown(project_path: str, output_folder: str = "docs/covernator") -> None:
    """Run Covernator and generate markdown summary.

    This function executes the Covernator coverage analysis and writes
    a Markdown coverage overview file to the specified output folder.

    Args:
        project_path: Path to the project source folder.
        output_folder: Path where the Markdown file will be written.
    """
    logging.info(f"🔍 Running Covernator analysis on: {project_path}")
    results = run_covernator(
        base_path=Path(project_path),
        folder_to_save_files_in=Path(output_folder),
    )

    output_path = Path(output_folder) / "coverage_overview.md"
    logging.info(f"🧾 Writing markdown to: {output_path}")

    generate_markdown_from_results(results, output_path=output_path)
    logging.info("✅ Markdown coverage overview generated successfully.")
