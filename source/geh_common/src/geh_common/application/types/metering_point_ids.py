from typing import Annotated

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode

from geh_common.application.converters.str_to_list import str_to_list


def _validate_metering_point_ids(ids: list[str]) -> list[str]:
    """Validate the list of metering point IDs."""
    if not ids:
        return ids
    for id_ in ids:
        if not isinstance(id_, str):
            raise TypeError(f"Metering point IDs must be strings, not {type(id_)}")
        if len(id_) != 18:
            raise ValueError(f"Metering point ID '{id_}' must be 18 characters")
        if not id_.isdigit():
            raise ValueError(f"Metering point ID '{id_}' must only consist of digits (0-9)")

    return ids


MeteringPointIds = Annotated[
    list[str],
    BeforeValidator(str_to_list),
    AfterValidator(_validate_metering_point_ids),
    NoDecode(),
]
"""
Annotated type for a list of metering point ids.

This type ensures that the input is converted to a list of strings representing metering point IDs,
and that each ID is validated to be a string of 18 digits.

Validators:
- BeforeValidator: Converts the input value to a list of metering point IDs.
- AfterValidator: Validates the list of metering point IDs.
- NoDecode: Prevents decoding of the input value.

Example:
```python
class MySettings(BaseSettings):
    metering_point_ids: MeteringPointIds

args = MySettings(metering_point_ids="800000000000000000, 123456789123456789, 9876543210987654321")
print(args.metering_point_ids)
# Output: ['800000000000000000', '123456789123456789', '987654321098765432']
```
"""
