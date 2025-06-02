from pathlib import Path

from geh_common.testing.job.project_script import assert_pyproject_toml_project_script_exists

root_path = Path(__file__).parent.parent.parent.parent.parent


def test_project_scripts(caplog):
    assert_pyproject_toml_project_script_exists(root_path / "pyproject.toml")
