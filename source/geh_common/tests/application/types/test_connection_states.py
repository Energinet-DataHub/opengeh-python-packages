import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import ConnectionStates


class ModelWithConnectionStates(BaseModel):
    connection_states: ConnectionStates


class ModelOptionalConnectionStates(BaseModel):
    connection_states: ConnectionStates | None = None


def _assert_connection_states(model, expected_states):
    """Helper function to assert the connection states."""
    if expected_states is None:
        assert model.connection_states is None
        return
    if isinstance(expected_states, str):
        if expected_states.startswith("[") and expected_states.endswith("]"):
            expected_states = expected_states[1:-1]
        expected_states = expected_states.split(",")
    assert model.connection_states == [str(s).strip() for s in expected_states]


@pytest.mark.parametrize(
    "testcase, match",
    [
        # Empty or invalid inputs
        ([], "Input should be a valid list"),
        ("", "Input should be a valid list"),
        ("[]", "Input should be a valid list"),
        # Invalid connection states
        (["not_a_connection_state", "not_a_connection_state_2"], "Must be one of"),
        ("not_a_connection_state,not_a_connection_state_2", "Must be one of"),
        ("not_a_connection_state, not_a_connection_state_2", "Must be one of"),
        ("[not_a_connection_state,not_a_connection_state_2]", "Must be one of"),
        # Valid inputs
        (["new", "connected"], None),
        ("connected,disconnected,closed_down", None),
        ("connected, disconnected, closed_down", None),
        ("[connected,disconnected,closed_down]", None),
    ],
)
def test__required_connection_states(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelWithConnectionStates(connection_states=testcase)
    else:
        model = ModelWithConnectionStates(connection_states=testcase)
        _assert_connection_states(model, testcase)


@pytest.mark.parametrize(
    "testcase, match",
    [
        # Empty or invalid inputs
        ([], "Input should be a valid list"),
        ("", "Input should be a valid list"),
        ("[]", "Input should be a valid list"),
        # Invalid connection states
        (["not_a_connection_state", "not_a_connection_state_2"], "Must be one of"),
        ("not_a_connection_state,not_a_connection_state_2", "Must be one of"),
        ("not_a_connection_state, not_a_connection_state_2", "Must be one of"),
        ("[not_a_connection_state,not_a_connection_state_2]", "Must be one of"),
        # Valid inputs
        (["new", "connected"], None),
        ("connected,disconnected,closed_down", None),
        ("connected, disconnected, closed_down", None),
        ("[connected,disconnected,closed_down]", None),
        ("[connected, disconnected, closed_down]", None),
    ],
)
def test__optional_connection_states(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelOptionalConnectionStates(connection_states=testcase)
    else:
        model = ModelOptionalConnectionStates(connection_states=testcase)
        _assert_connection_states(model, testcase)
