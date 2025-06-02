import re
from typing import Annotated, Any

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode


def _convert_grid_area_codes(value: Any) -> list[str] | None:
    """Convert the input value to a list of grid area codes (strings).

    Args:
        value (Any): The input value to convert.

    Returns:
        Optional[List[str]]: A list of grid area codes or None if the input is empty.
    """
    if not value:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    else:
        return re.findall(r"\d+", value)


def _validate_grid_area_codes(v: list[str] | None) -> list[str] | None:
    """Validate the list of grid area codes.

    Args:
        v (Optional[List[str]]): The list of grid area codes to validate.

    Returns:
        Optional[List[str]]: The validated list of grid area codes.

    Raises:
        ValueError: If any grid area code is not a string of 3 digits.
    """
    if v is None:
        return v
    for code in v:
        if not isinstance(code, str):
            raise TypeError(f"Grid area codes must be strings, not {type(code)}")
        if len(code) != 3 or not code.isdigit():
            raise ValueError(
                f"Unexpected grid area code: '{code}'. Grid area codes must consist of 3 digits (000-999)."
            )
    return v


GridAreaCodes = Annotated[
    list[str] | str | list[int],
    BeforeValidator(_convert_grid_area_codes),
    AfterValidator(_validate_grid_area_codes),
    NoDecode(),
]
"""
Annotated type for a list of grid area codes.

This type ensures that the input is converted to a list of strings representing grid area codes,
and that each code is validated to be a string of exactly 3 digits (000-999).

Validators:
- BeforeValidator: Converts the input value to a list of grid area codes.
- AfterValidator: Validates the list of grid area codes.
- NoDecode: Prevents decoding of the input value.

Example:
```python
class MySettings(BaseSettings):
    grid_area_codes: GridAreaCodes

args = MySettings(grid_area_codes="123,456,789")
print(args.grid_area_codes)
# Output: ['123', '456', '789']
```
"""
