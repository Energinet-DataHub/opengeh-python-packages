from pathlib import Path

from geh_common.testing.covernator.commands import run_covernator
from geh_common.testing.covernator.markdown_generator import generate_markdown_from_results


def run_and_generate_markdown(project_path: str, output_folder: str = "docs/covernator") -> None:
    print(f"🔍 Running Covernator analysis on: {project_path}")
    results = run_covernator(base_path=Path(project_path), folder_to_save_files_in=Path(output_folder))

    output_path = Path(output_folder) / "coverage_overview.md"
    print(f"🧾 Writing markdown to: {output_path}")

    generate_markdown_from_results(results, output_path=output_path)
    print("✅ Markdown coverage overview generated successfully.")
