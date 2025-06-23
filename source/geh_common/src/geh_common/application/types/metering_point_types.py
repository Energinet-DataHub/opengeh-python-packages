from typing import Annotated

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode

from geh_common.application.converters.str_to_list import str_to_list
from geh_common.domain.types import MeteringPointType


def _validate_metering_point_types(v: list[str]) -> list[str]:
    """Validate the list of metering point types."""
    if v is None:
        return v
    for point in v:
        if not isinstance(point, str):
            raise TypeError(f"Metering point types must be strings, not {type(point)}")
        if point not in [e.value for e in MeteringPointType]:
            raise ValueError(
                f"Unexpected metering point type: '{point}'. Must be one of: {', '.join([e.value for e in MeteringPointType])}"
            )
    return v


MeteringPointTypes = Annotated[
    list[str],
    BeforeValidator(str_to_list),
    AfterValidator(_validate_metering_point_types),
    NoDecode(),
]
"""
Annotated type for a list of metering point types

This type ensures that the input is converted to a list of strings representing metering point types,
and that each type is validated to be a string that is represented in MeteringPointType.

Validators:
- BeforeValidator: Converts the input value to a list of metering point types.
- AfterValidator: Validates that each metering point type is valid.
- NoDecode: Prevents decoding of the input value.

Example:
"""
