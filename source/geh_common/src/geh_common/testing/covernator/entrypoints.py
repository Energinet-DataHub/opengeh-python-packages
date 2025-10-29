import os
from pathlib import Path

from geh_common.covernator_streamlit.server import run_covernator_return
from geh_common.testing.covernator.markdown_generator import generate_markdown_from_results


def run_and_generate_markdown(project_path: str, output_folder: str = "docs/covernator") -> None:
    """Runs Covernator and writes the markdown summary to disk.
    - project_path: directory with test suites (e.g. source/geh_calculated_measurements)
    - output_folder: where to write the markdown output (default: docs/covernator)
    """
    print(f"🔍 Running Covernator analysis on: {project_path}")
    results = run_covernator_return(project_path)

    print(f"📁 Ensuring output folder exists: {output_folder}")
    os.makedirs(output_folder, exist_ok=True)

    output_path = Path(output_folder) / "coverage_overview.md"
    print(f"🧾 Writing markdown to: {output_path}")

    # ✅ Pass the output_path explicitly
    generate_markdown_from_results(results, output_path=output_path, group_prefix="geh_")

    print("✅ Markdown coverage overview generated successfully.")
