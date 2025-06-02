import re
from typing import Any


def str_to_list(value: Any) -> list[str] | None:
    """Convert the input value to a list of strings.

    Args:
        value (Any): The input value to convert.

    Returns:
        Optional[List[str]]: A list of strings or None if the input is empty.
    """
    if not value:
        return None
    if isinstance(value, list):
        return _convert_values(value)
    elif isinstance(value, str):
        return _convert_values(re.findall(r"\d+", value))
    else:
        raise TypeError(f"Input should be a valid list or string, not {type(value)}")


def _convert_values(items) -> list[str] | None:
    valid_values = []
    for item in items:
        normalized = _normalize_value(item)
        if normalized is not None:
            valid_values.extend(normalized)
    if not valid_values:
        return None
    return valid_values


def _normalize_value(value: Any) -> list[str] | None:
    """Normalize a value to a string or None.

    Args:
        value (Any): The input value to normalize.

    Returns:
        Optional[str]: The normalized string or None if the input is empty.
    """
    if isinstance(value, int | str):
        return _normalize_scalar_value(value)
    elif isinstance(value, list):
        return _convert_values(value)


def _normalize_scalar_value(value: Any) -> list[str] | None:
    """Normalize a scalar value to a string or None.

    Args:
        value (Any): The input value to normalize.

    Returns:
        Optional[str]: The normalized string or None if the input is empty.
    """
    item = str(value)
    if item.startswith("[") and item.endswith("]"):
        item = item[1:-1]
    item_normalized = str(item).strip()
    if item_normalized and item_normalized != "[]":
        return item_normalized.split(",")
    return None
