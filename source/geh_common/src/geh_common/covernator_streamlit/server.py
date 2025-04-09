import os
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings

from geh_common.testing.covernator import run_covernator


class CovernatorCliSettings(BaseSettings, cli_parse_args=True, cli_kebab_case=True, cli_implicit_flags=True):
    """CLI-Tool to generate covernator files and run the streamlit app."""

    test_folder_path: str = Field(
        description="base path to search for test scenarios",
        default="./tests",
        validation_alias=AliasChoices("t", "p", "test-folder-path"),
    )
    output_dir: Optional[str] = Field(
        description="output directory to store the files. If not set, will create a temporary directory",
        default=None,
        validation_alias=AliasChoices("o", "output-dir"),
    )
    generate_only: bool = Field(
        description="Do not run the streamlit app, only generate the files. Requires --output_dir",
        default=False,
        validation_alias=AliasChoices("g", "generate-only"),
    )
    serve_only: bool = Field(
        description="Do not generate the files, only run the streamlit app. Requires --output_dir",
        default=False,
        validation_alias=AliasChoices("s", "serve-only"),
    )


def _validate_cli_args(args: CovernatorCliSettings):
    generate_str = "--generate-only (-g)"
    serve_str = "--serve-only (-s)"
    if args.generate_only and args.serve_only:
        raise ValueError(f"Covernator failed, as both {generate_str} and {serve_str} are set.")

    if args.output_dir is None and (args.serve_only is True or args.generate_only is True):
        raise ValueError(
            f"Covernator failed, as --output_dir (-o) is required when {generate_str} or {serve_str} are set."
        )


def _create_and_run_streamlit_app(output_dir: Path):
    streamlit_script = os.path.join(os.path.dirname(__file__), "streamlit_app.py")
    with open(streamlit_script) as f:
        content = f.read()
        content = content.replace("{SUBSTITUTTED_OUTPUT_PATH}", output_dir.as_posix())
    with open(f"{output_dir}/script.py", "w") as f:
        f.write(content)
    command = ["streamlit", "run", f"{output_dir}/script.py"]
    res = subprocess.run(command, check=True)
    if res.returncode != 0:
        raise RuntimeError("Error running streamlit app")


def main():
    cli_args = CovernatorCliSettings()

    _validate_cli_args(cli_args)

    base_path = Path(cli_args.test_folder_path)
    output_dir = Path(cli_args.output_dir or tempfile.mkdtemp())

    if not cli_args.serve_only:
        run_covernator(output_dir, base_path)

    if not cli_args.generate_only:
        _create_and_run_streamlit_app(output_dir)


if __name__ == "__main__":
    main()
