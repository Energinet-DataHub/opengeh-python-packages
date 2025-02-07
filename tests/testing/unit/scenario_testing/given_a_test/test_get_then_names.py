from pathlib import Path

import pytest

from geh_common.testing.scenario_testing import get_then_names


@pytest.mark.parametrize("test_case_name", get_then_names())
def test_get_then_names(test_case_name):
    assert test_case_name in ["output", "output2", "some_folder/some_output"]


@pytest.mark.parametrize("test_case_name", get_then_names(scenario_path=Path(__file__).parent))
def test_get_then_names_from_provided_scenario_path(test_case_name):
    assert test_case_name in ["output", "output2", "some_folder/some_output"]
