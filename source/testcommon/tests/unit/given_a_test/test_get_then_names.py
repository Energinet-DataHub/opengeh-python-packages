import pytest

from testcommon.get_then_names import get_then_names


@pytest.mark.parametrize("test_case_name", get_then_names())
def test_get_then_names(test_case_name):
    assert test_case_name in ["test", "another_test"]
