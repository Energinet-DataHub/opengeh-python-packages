from typing import Annotated

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode

from geh_common.application.converters.str_to_list import str_to_list


def _validate_grid_area_ids(v: list[str] | None) -> list[str] | None:
    """Validate the list of grid area ids.

    Args:
        v (Optional[List[str]]): The list of grid area ids to validate.

    Returns:
        Optional[List[str]]: The validated list of grid area ids.

    Raises:
        ValueError: If any grid area id is not a string of 3 digits.
    """
    if v is None:
        return v
    for code in v:
        if not isinstance(code, str):
            raise TypeError(f"Grid area ids must be strings, not {type(code)}")
        if len(code) != 3 or not code.isdigit():
            raise ValueError(f"Unexpected grid area id: '{code}'. Grid area ids must consist of 3 digits (000-999).")
    return v


GridAreaIds = Annotated[
    list[str],
    BeforeValidator(str_to_list),
    AfterValidator(_validate_grid_area_ids),
    NoDecode(),
]
"""
Annotated type for a list of grid area ids.

This type ensures that the input is converted to a list of strings representing grid area ids,
and that each id is validated to be a string of exactly 3 digits (000-999).

Validators:
- BeforeValidator: Converts the input value to a list of grid area ids.
- AfterValidator: Validates the list of grid area ids.
- NoDecode: Prevents decoding of the input value.

Example:
```python
class MySettings(BaseSettings):
    grid_area_ids: GridAreaIds

args = MySettings(grid_area_ids="123,456,789")
print(args.grid_area_ids)
# Output: ['123', '456', '789']
```
"""
