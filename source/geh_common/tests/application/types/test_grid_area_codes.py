from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import GridAreaCodes


class ModelWithGridAreaCodes(BaseModel):
    grid_area_codes: GridAreaCodes


class ModelWithOptionalGridAreaCodes(BaseModel):
    grid_area_codes: GridAreaCodes | None = None


class ModelWithoutGridAreaCodes(BaseModel):
    grid_area_codes: list[str] | None = None


def test__when_valid_grid_area_codes__returns_expected() -> None:
    # Arrange
    valid_codes = ["123", "456", "789"]

    # Act
    model = ModelWithGridAreaCodes(grid_area_codes=valid_codes)

    # Assert
    assert model.grid_area_codes == valid_codes


def test__when_valid_grid_area_codes_from_string__returns_list_of_grid_area_codes() -> None:
    # Arrange
    valid_codes_string = "123,456,789"
    expected_codes = ["123", "456", "789"]

    # Act
    model = ModelWithGridAreaCodes(grid_area_codes=valid_codes_string)

    # Assert
    assert model.grid_area_codes == expected_codes


def test__when_none_grid_area_codes_and_optional__returns_none() -> None:
    # Arrange
    none_codes = None

    # Act
    model = ModelWithOptionalGridAreaCodes(grid_area_codes=none_codes)

    # Assert
    assert model.grid_area_codes is None


def test__when_empty_grid_area_codes__raises_exception() -> None:
    # Arrange
    empty_codes = []

    # Act
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaCodes(grid_area_codes=empty_codes)

    # Assert
    assert "Input should be a valid list" in str(exc_info.value)


def test__when_none_grid_area_codes_and_mandatory__raises_exception() -> None:
    # Arrange
    none_codes = None

    # Act
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaCodes(grid_area_codes=none_codes)  # type: ignore

    # Assert
    assert "Input should be a valid list" in str(exc_info.value)


@pytest.mark.parametrize(
    "invalid_code",
    [
        "12",  # not three characters
        "4567",  # not three characters
        "89a",  # not all digits
    ],
)
def test__when_invalid_grid_area_codes__raises_exception(invalid_code: Any) -> None:
    # Act & Assert
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaCodes(grid_area_codes=[invalid_code])

    assert "Unexpected grid area code" in str(exc_info.value) or "Grid area codes must be strings" in str(
        exc_info.value
    )


def test__when_list_of_valid_int__returns_list_of_strings():
    # Arrange
    valid_codes_string = [123, 456, 789]
    expected_codes = ["123", "456", "789"]

    # Act
    model = ModelWithGridAreaCodes(grid_area_codes=valid_codes_string)

    # Assert
    assert model.grid_area_codes == expected_codes


@pytest.mark.parametrize(
    "invalid_code",
    [
        1234,  # not three digits
        1,  # not three digits
        12,  # not three digits
    ],
)
def test__when_invalid_int__raise_exception(invalid_code: Any) -> None:
    # Act & Assert
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaCodes(grid_area_codes=[invalid_code])

    assert "Unexpected grid area code" in str(exc_info.value) or "Grid area codes must be strings" in str(
        exc_info.value
    )


def test__when_not_gridareacodes_type___returns_expected() -> None:
    # Arrange
    valid_codes = ["12", "whatever", "789"]

    # Act
    model = ModelWithoutGridAreaCodes(grid_area_codes=valid_codes)

    # Assert
    assert model.grid_area_codes == valid_codes
