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
        return [str(item) for item in value]
    elif isinstance(value, str):
        return re.findall(r"\d+", value)
    else:
        raise TypeError(f"Input should be a valid list or string, not {type(value)}")
