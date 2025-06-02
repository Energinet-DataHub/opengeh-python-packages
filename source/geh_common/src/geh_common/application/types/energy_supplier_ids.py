import re
from typing import Annotated, Any

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode


def _str_to_list(value: Any) -> list[str] | None:
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


def _validate_energy_supplier_ids(v: list[str]) -> list[str]:
    """Validate the list of energy supplier IDs."""
    if not v:
        return v
    for id_ in v:
        if not isinstance(id_, str):
            raise TypeError(f"Energy supplier IDs must be strings, not {type(id_)}")
        if not (len(id_) == 13 or len(id_) == 16):
            raise ValueError(f"Energy supplier ID '{id_}' must be 13 or 16 characters. Not {len(id_)}.")
        if not all(c.isdigit() for c in id_):
            raise ValueError(f"Energy supplier ID '{id_}' must consist of digits only.")
    return v


EnergySupplierIds = Annotated[
    list[str] | list[int] | str,
    BeforeValidator(_str_to_list),
    AfterValidator(_validate_energy_supplier_ids),
    NoDecode(),
]
"""
Annotated type for a list of energy supplier ids.

This type ensures that the input is converted to a list of strings representing energy supplier IDs,
and that each ID is validated to be a string either 13 or 16 digits.

Validators:
- BeforeValidator: Converts the input value to a list of energy suppler IDs.
- AfterValidator: Validates the list of energy supplier IDs.
- NoDecode: Prevents decoding of the input value.

Example:
```python
class MySettings(BaseSettings):
    energy_supplier_ids: EnergySupplierIds

args = MySettings(energy_supplier_ids="8000000000000, 1234567890123456, 1234567890123")
print(args.energy_supplier_ids)
# Output: ['8000000000000', '1234567890123456', '1234567890123']
```
"""
