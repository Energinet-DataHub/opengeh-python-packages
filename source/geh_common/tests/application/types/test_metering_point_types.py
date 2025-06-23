import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import MeteringPointTypes


class ModelWithMeteringPointTypes(BaseModel):
    metering_point_types: MeteringPointTypes


class ModelOptionalMeteringPointTypes(BaseModel):
    metering_point_types: MeteringPointTypes | None = None


def _assert_metering_point_types(model, expected_points):
    """Helper function to assert the energy supplier IDs."""
    if expected_points is None:
        assert model.metering_point_types is None
        return
    if isinstance(expected_points, str):
        if expected_points.startswith("[") and expected_points.endswith("]"):
            expected_points = expected_points[1:-1]
        expected_points = expected_points.split(",")
    assert model.metering_point_types == [str(id_).strip() for id_ in expected_points]


@pytest.mark.parametrize(
    "testcase, match",
    [
        # Empty or invalid inputs
        ([], "Input should be a valid list"),
        ("", "Input should be a valid list"),
        ("[]", "Input should be a valid list"),
        # Too short or too long IDs
        (["not_a_metering_point_type", "not_a_metering_point_type_2"], "Must be one of"),
        ("not_a_metering_point_type,not_a_metering_point_type_2", "Must be one of"),
        ("not_a_metering_point_type, not_a_metering_point_type_2", "Must be one of"),
        ("[not_a_metering_point_type,not_a_metering_point_type_2]", "Must be one of"),
        # Valid inputs
        (["capacity_settlement", "electrical_heating"], None),
        ("capacity_settlement,electrical_heating,net_consumption", None),
        ("capacity_settlement, electrical_heating, net_consumption", None),
        ("[capacity_settlement,electrical_heating,net_consumption]", None),
    ],
)
def test__required_energy_supplier_ids(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelWithMeteringPointTypes(metering_point_types=testcase)
    else:
        model = ModelWithMeteringPointTypes(metering_point_types=testcase)
        _assert_metering_point_types(model, testcase)


@pytest.mark.parametrize(
    "testcase, match",
    [
        # Empty or invalid inputs
        ([], "Input should be a valid list"),
        ("", "Input should be a valid list"),
        ("[]", "Input should be a valid list"),
        # Too short or too long IDs
        (["not_a_metering_point_type", "not_a_metering_point_type_2"], "Must be one of"),
        ("not_a_metering_point_type,not_a_metering_point_type_2", "Must be one of"),
        ("not_a_metering_point_type, not_a_metering_point_type_2", "Must be one of"),
        ("[not_a_metering_point_type,not_a_metering_point_type_2]", "Must be one of"),
        # Valid inputs
        (["capacity_settlement", "electrical_heating"], None),
        ("capacity_settlement,electrical_heating,net_consumption", None),
        ("capacity_settlement, electrical_heating, net_consumption", None),
        ("[capacity_settlement,electrical_heating,net_consumption]", None),
    ],
)
def test__optional_energy_supplier_ids(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelOptionalMeteringPointTypes(metering_point_types=testcase)
    else:
        model = ModelOptionalMeteringPointTypes(metering_point_types=testcase)
        _assert_metering_point_types(model, testcase)
