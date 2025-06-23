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
        # Split by commas first, and then extract from brackets if present
        if "[" in value and "]" in value:
            # Extract content from within brackets
            bracket_content = re.search(r"\[(.*?)\]", value)
            if bracket_content:
                value = bracket_content.group(1)

        # Split by comma and clean up each item
        items = [item.strip() for item in value.split(",")]
        return _convert_values(items)
    else:
        raise TypeError(f"Input should be a valid list or string, not {type(value)}")


def _convert_values(items) -> list[str] | None:
    valid_values = []
    for item in items:
        normalized = None
        if isinstance(item, int | str):
            normalized = _normalize_scalar_value(item)
        elif isinstance(item, list):
            normalized = _convert_values(item)

        # Only add non-empty normalized values
        if normalized is not None:
            valid_values.extend(normalized)

    # If no valid values were found, return None
    if not valid_values:
        return None

    return valid_values


def _normalize_scalar_value(value: Any) -> list[str] | None:
    """Normalize a scalar value to a string or None.

    Args:
        value (Any): The input value to normalize.

    Returns:
        Optional[List[str]]: A list containing the normalized string or None if the input is empty.
    """
    item = str(value)
    if item.startswith("[") and item.endswith("]"):
        item = item[1:-1]
    item_normalized = str(item).strip()
    if item_normalized:
        return [str(v).strip() for v in item_normalized.split(",")]
    return None
