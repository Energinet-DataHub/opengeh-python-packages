import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import EnergySupplierIds


class ModelWithEnergySupplierIds(BaseModel):
    energy_supplier_ids: EnergySupplierIds


class ModelOptionalEnergySupplierIds(BaseModel):
    energy_supplier_ids: EnergySupplierIds | None = None


def _assert_energy_supplier_ids(model, expected_ids):
    """Helper function to assert the energy supplier IDs."""
    if expected_ids is None:
        assert model.energy_supplier_ids is None
        return
    if isinstance(expected_ids, str):
        if expected_ids.startswith("[") and expected_ids.endswith("]"):
            expected_ids = expected_ids[1:-1]
        expected_ids = expected_ids.split(",")
    assert model.energy_supplier_ids == expected_ids


@pytest.mark.parametrize(
    "testcase, match",
    [
        (["8000000000000", "1234567890123456", "1234567890123"], None),
        (["123456789", "1234567890123"], "must be 13 or 16 characters"),
        ([], "Input should be a valid list"),
        ("8000000000000,1234567890123456,1234567890123", None),
        ("123456789,1234567890123", "must be 13 or 16 characters"),
        ("", "Input should be a valid list"),
        ("[8000000000000,1234567890123456,1234567890123]", None),
        ("[123456789,1234567890123]", "must be 13 or 16 characters"),
        ("[]", "Input should be a valid list"),
    ],
)
def test__required_energy_supplier_ids(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelWithEnergySupplierIds(energy_supplier_ids=testcase)
    else:
        model = ModelWithEnergySupplierIds(energy_supplier_ids=testcase)
        _assert_energy_supplier_ids(model, testcase)


@pytest.mark.parametrize(
    "testcase, match",
    [
        (None, None),  # Optional field can be None
        (["8000000000000", "1234567890123456", "1234567890123"], None),
        (["123456789", "1234567890123"], "must be 13 or 16 characters"),
        ([], "Input should be a valid list"),
        ("8000000000000,1234567890123456,1234567890123", None),
        ("123456789,1234567890123", "must be 13 or 16 characters"),
        ("", "Input should be a valid list"),
        ("[8000000000000,1234567890123456,1234567890123]", None),
        ("[123456789,1234567890123]", "must be 13 or 16 characters"),
        ("[]", "Input should be a valid list"),
    ],
)
def test__optional_energy_supplier_ids(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelOptionalEnergySupplierIds(energy_supplier_ids=testcase)
    else:
        model = ModelOptionalEnergySupplierIds(energy_supplier_ids=testcase)
        _assert_energy_supplier_ids(model, testcase)
