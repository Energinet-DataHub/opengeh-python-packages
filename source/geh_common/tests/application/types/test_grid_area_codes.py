from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import GridAreaIds


class ModelWithGridAreaIds(BaseModel):
    grid_area_ids: GridAreaIds


class ModelWithOptionalGridAreaIds(BaseModel):
    grid_area_ids: GridAreaIds | None = None


class ModelWithoutGridAreaIds(BaseModel):
    grid_area_ids: list[str] | None = None


def test__when_valid_grid_area_ids__returns_expected() -> None:
    # Arrange
    valid_ids = ["123", "456", "789"]

    # Act
    model = ModelWithGridAreaIds(grid_area_ids=valid_ids)

    # Assert
    assert model.grid_area_ids == valid_ids


def test__when_valid_grid_area_ids_from_string__returns_list_of_grid_area_ids() -> None:
    # Arrange
    valid_ids_string = "123,456,789"
    expected_ids = ["123", "456", "789"]

    # Act
    model = ModelWithGridAreaIds(grid_area_ids=valid_ids_string)  # type: ignore

    # Assert
    assert model.grid_area_ids == expected_ids


def test__when_none_grid_area_ids_and_optional__returns_none() -> None:
    # Arrange
    none_ids = None

    # Act
    model = ModelWithOptionalGridAreaIds(grid_area_ids=none_ids)

    # Assert
    assert model.grid_area_ids is None


def test__when_empty_grid_area_ids__raises_exception() -> None:
    # Arrange
    empty_ids = []

    # Act
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaIds(grid_area_ids=empty_ids)

    # Assert
    assert "Input should be a valid list" in str(exc_info.value)


def test__when_none_grid_area_ids_and_mandatory__raises_exception() -> None:
    # Arrange
    none_ids = None

    # Act
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaIds(grid_area_ids=none_ids)  # type: ignore

    # Assert
    assert "Input should be a valid list" in str(exc_info.value)


@pytest.mark.parametrize(
    "invalid_id",
    [
        "12",  # not three characters
        "4567",  # not three characters
        "89a",  # not all digits
    ],
)
def test__when_invalid_grid_area_ids__raises_exception(invalid_id: Any) -> None:
    # Act & Assert
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaIds(grid_area_ids=[invalid_id])

    assert "Unexpected grid area id" in str(exc_info.value) or "Grid area ids must be strings" in str(exc_info.value)


def test__when_list_of_valid_int__returns_list_of_strings():
    # Arrange
    valid_ids_string = [123, 456, 789]
    expected_ids = ["123", "456", "789"]

    # Act
    model = ModelWithGridAreaIds(grid_area_ids=valid_ids_string)  # type: ignore

    # Assert
    assert model.grid_area_ids == expected_ids


@pytest.mark.parametrize(
    "invalid_id",
    [
        1234,  # not three digits
        1,  # not three digits
        12,  # not three digits
    ],
)
def test__when_invalid_int__raise_exception(invalid_id: Any) -> None:
    # Act & Assert
    with pytest.raises(ValidationError) as exc_info:
        ModelWithGridAreaIds(grid_area_ids=[invalid_id])

    assert "Unexpected grid area id" in str(exc_info.value) or "Grid area ids must be strings" in str(
        exc_info.value
    )


def test__when_not_gridareaids_type___returns_expected() -> None:
    # Arrange
    valid_ids = ["12", "whatever", "789"]

    # Act
    model = ModelWithoutGridAreaIds(grid_area_ids=valid_ids)

    # Assert
    assert model.grid_area_ids == valid_ids
