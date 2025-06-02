from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import EnergySupplierIds


class TestModel(BaseModel):
    __test__ = False  # Prevents pytest from treating this as a test case

    energy_supplier_ids: EnergySupplierIds


class TestModelWithOptionalEnergySupplierIds(BaseModel):
    __test__ = False  # Prevents pytest from treating this as a test case

    energy_supplier_ids: EnergySupplierIds | None = None


class TestModelWithoutEnergySupplierIds(BaseModel):
    __test__ = False  # Prevents pytest from treating this as a test case

    energy_supplier_ids: list[str] | None = None


def test__when_valid_energy_supplier_ids__returns_expected() -> None:
    # Arrange
    valid_ids = ["8000000000000", "1234567890123456", "1234567890123"]

    # Act
    model = TestModel(energy_supplier_ids=valid_ids)

    # Assert
    assert model.energy_supplier_ids == valid_ids


def test__when_valid_energy_supplier_ids_from_string__returns_list_of_grid_area_codes() -> None:
    # Arrange
    valid_id_string = "8000000000000,1234567890123456,1234567890123"
    expected_ids = ["8000000000000", "1234567890123456", "1234567890123"]

    # Act
    model = TestModel(energy_supplier_ids=valid_id_string)

    # Assert
    assert model.energy_supplier_ids == expected_ids


def test__when_none_energy_supplier_ids_and_optional__returns_none() -> None:
    # Arrange
    none_ids = None

    # Act
    model = TestModelWithOptionalEnergySupplierIds(energy_supplier_ids=none_ids)

    # Assert
    assert model.energy_supplier_ids is None


def test__when_empty_energy_supplier_ids__raises_exception() -> None:
    # Arrange
    empty_ids = []

    # Act
    with pytest.raises(ValidationError) as exc_info:
        TestModel(energy_supplier_ids=empty_ids)

    # Assert
    assert "Input should be a valid list" in str(exc_info.value)


def test__when_none_energy_supplier_ids_and_mandatory__raises_exception() -> None:
    # Arrange
    none_ids = None

    # Act
    with pytest.raises(ValidationError) as exc_info:
        TestModel(energy_supplier_ids=none_ids)  # type: ignore

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
def test__when_invalid_energy_supplier_ids__raises_exception(invalid_code: Any) -> None:
    # Act & Assert
    with pytest.raises(ValidationError, match="energy_supplier_ids\n\\s+Value error"):
        TestModel(energy_supplier_ids=[invalid_code])

    # assert "Unexpected energy supplier id" in str(exc_info.value) or "Energy supplier ids must be strings" in str(
    #     exc_info.value
    # )


def test__when_list_of_valid_int__returns_list_of_strings():
    # Arrange
    valid_id_ints = [8000000000000, 1234567890123456, 1234567890123]
    expected_ids = ["8000000000000", "1234567890123456", "1234567890123"]

    # Act
    model = TestModel(energy_supplier_ids=valid_id_ints)

    # Assert
    assert model.energy_supplier_ids == expected_ids


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
    with pytest.raises(ValidationError, match=f"must be 13 or 16 characters. Not {len(str(invalid_code))}."):
        TestModel(energy_supplier_ids=[invalid_code])


def test__when_not_energysupplierids_type___returns_expected() -> None:
    # Arrange
    valid_ids = ["12", "whatever", "789"]

    # Act
    model = TestModelWithoutEnergySupplierIds(energy_supplier_ids=valid_ids)

    # Assert
    assert model.energy_supplier_ids == valid_ids
