from typing import Annotated

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode

from geh_common.application.converters.str_to_list import str_to_list
from geh_common.domain.types import ConnectionState


def _validate_connection_states(v: list[str] | None) -> list[str] | None:
    """Validate the list of connection states."""
    if v is None:
        return v
    for state in v:
        if not isinstance(state, str):
            raise TypeError(f"Connection states must be strings, not {type(state)}")
        if state not in [e.value for e in ConnectionState]:
            raise ValueError(
                f"Unexpected connection state: '{state}'. Must be one of: {', '.join([e.value for e in ConnectionState])}"
            )
    return v


ConnectionStates = Annotated[
    list[str],
    BeforeValidator(str_to_list),
    AfterValidator(_validate_connection_states),
    NoDecode(),
]
"""
Annotated type for a list of connection states

This type ensures that the input is converted to a list of strings representing connection states,
and that each state is validated to be a string that is represented in ConnectionState.

Validators:
- BeforeValidator: Converts the input value to a list of connection states.
- AfterValidator: Validates that each connection state is valid.
- NoDecode: Prevents decoding of the input value.

Example:
    >>> ConnectionStates(["connected", "disconnected"])
    ["connected", "disconnected"]
"""
