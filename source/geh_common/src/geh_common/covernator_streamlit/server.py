import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from geh_common.testing.covernator import run_covernator


class CovernatorCliSettings(BaseSettings, cli_parse_args=True, cli_kebab_case=True, cli_implicit_flags=True):
    """CLI-Tool to generate covernator files and run the streamlit app."""

    path: Path = Field(
        description="Base path to search for test scenarios",
        default=Path("./tests"),
        validation_alias=AliasChoices("p", "path"),
    )
    output_dir: Path = Field(
        description="Output directory to store the files. If not set, will create a temporary directory",
        default=Path(tempfile.mkdtemp()),
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
    github_output_key: str | None = Field(
        description="Key to write github output to. If not set, will not write to github output",
        default=None,
        validation_alias=AliasChoices("k", "github-output-key"),
    )

    @model_validator(mode="before")
    @classmethod
    def validate_arguments_combined(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Validate allowed combinations of arguments.

        Checks that at max one of --generate-only or --serve-only is set.
        If one of them is set, requires --output_dir to be set.
        """
        generate_str = "--generate-only (-g)"
        serve_str = "--serve-only (-s)"
        generate_only = data.get("g", False)
        serve_only = data.get("s", False)

        if generate_only and serve_only:
            raise ValueError(f"Covernator failed, as both {generate_str} and {serve_str} are set.")

        if data.get("o", None) is None and (serve_only or generate_only):
            raise ValueError(
                f"Covernator failed, as --output_dir (-o) is required when {generate_str} or {serve_str} are set."
            )

        return data

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Need to overwrite the priority, otherwise path with be overwritten by the env var."""
        return (init_settings,)


def _create_and_run_streamlit_app(output_dir: Path):
    """Create a copy of the streamlit file, modify the path and run it."""
    streamlit_script = os.path.join(os.path.dirname(__file__), "streamlit_app.py")
    with open(streamlit_script) as f:
        content = f.read()
        content = content.replace("{SUBSTITUTTED_OUTPUT_PATH}", output_dir.as_posix())
    with open(f"{output_dir}/script.py", "w") as f:
        f.write(content)
    command = ["uv", "run", f"{output_dir}/script.py"]
    res = subprocess.run(command, check=True)
    if res.returncode != 0:
        raise RuntimeError("Error running streamlit app")


def _write_github_output(covernator_cli_settings: CovernatorCliSettings):
    value = """"""
    try:
        with open(covernator_cli_settings.output_dir / "stats.json") as stats_file:
            stats = json.load(stats_file)
        value = (
            f"<details><summary>Stats</summary>"
            f"<h3>Total Cases</h3>{stats['total_cases']}"
            f"<h3>Unique Scenarios</h3>{stats['total_scenarios']}"
            f"<h3>Unique Groups</h3>{stats['total_groups']}</details>"
        )
    except FileNotFoundError | KeyError:
        raise Exception(
            f"Could not find stats.json with the correct content in {covernator_cli_settings.output_dir}. "
            "Please update to the latest covernator version."
        )

    try:
        with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
            print(f"{covernator_cli_settings.github_output_key}={value}", file=fh)
    except KeyError:
        raise Exception(
            "GITHUB_OUTPUT environment variable not set. Please run this script in a GitHub Actions workflow."
        )


def main():
    cli_args = CovernatorCliSettings()

    if not cli_args.serve_only:
        run_covernator(cli_args.output_dir, cli_args.path)

    if not cli_args.generate_only:
        _create_and_run_streamlit_app(cli_args.output_dir)

    if cli_args.github_output_key is not None:
        _write_github_output(cli_args)


if __name__ == "__main__":
    main()
