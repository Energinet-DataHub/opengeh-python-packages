import re
from typing import Annotated

from pydantic import AfterValidator, BeforeValidator
from pydantic_settings import NoDecode

from geh_common.application.converters.str_to_list import str_to_list


def _validate_energy_supplier_ids(v: list[str]) -> list[str]:
    """Validate the list of energy supplier IDs."""
    if not v:
        return v
    for id_ in v:
        if not isinstance(id_, str):
            raise TypeError(f"Energy supplier IDs must be strings, not {type(id_)}")
        if not (len(id_) == 13 or len(id_) == 16):
            raise ValueError(f"Energy supplier ID '{id_}' must be 13 or 16 characters")
        if len(id_) == 13:
            if not re.fullmatch(r"[0-9]{13}", id_):
                raise ValueError(f"Energy supplier ID (GLN) must be 13 digits (0-9), got {id_}")
        if len(id_) == 16:
            if not re.fullmatch(r"[A-Za-z0-9]{16}", id_):
                raise ValueError(
                    f"Energy supplier ID '{id_}' (EIC) must be 16 characters (0-9, a-z, A-Z) with no special characters."
                )

    return v


EnergySupplierIds = Annotated[
    list[str],
    BeforeValidator(str_to_list),
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
