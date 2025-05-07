import logging
from pathlib import Path

import pytest

from geh_common.testing.job.project_script import assert_pyproject_toml_project_script_exists

test_cases = Path(__file__).parent / "test_cases"


def test_missing_function(caplog):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(AssertionError, match="Missing script entry points detected"):
            assert_pyproject_toml_project_script_exists(test_cases / "case_missing")

    assert "- func: Function 'func_not_there' not found" in caplog.text


def test_success(caplog):
    with caplog.at_level(logging.INFO):
        assert_pyproject_toml_project_script_exists(test_cases / "case_succeed")

    assert "All script entry points exist!" in caplog.text
