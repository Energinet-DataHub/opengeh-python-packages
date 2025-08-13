import pytest
from pydantic import BaseModel, ValidationError

from geh_common.application import MeteringPointIds


class ModelWithMeteringPointIds(BaseModel):
    metering_point_ids: MeteringPointIds


class ModelOptionalModelWithMeteringPointIds(BaseModel):
    metering_point_ids: MeteringPointIds | None = None


def _assert_metering_point_ids(model, expected_ids):
    """Helper function to assert the metering point IDs."""
    if expected_ids is None:
        assert model.metering_point_ids is None
        return
    if isinstance(expected_ids, str):
        if expected_ids.startswith("[") and expected_ids.endswith("]"):
            expected_ids = expected_ids[1:-1]
        expected_ids = expected_ids.split(",")
    assert model.metering_point_ids == [str(id_).strip() for id_ in expected_ids]


@pytest.mark.parametrize(
    "testcase, match",
    [
        # Empty or invalid inputs
        ([], "Input should be a valid list"),
        ("", "Input should be a valid list"),
        ("[]", "Input should be a valid list"),
        # Too short or too long IDs
        ([1234567891234567890, 123456789123456789], "must be 18 characters"),
        (["12345678912345678", "123456789123456789"], "must be 18 characters"),
        ("1234567891234567890,123456789123456789", "must be 18 characters"),
        ("[12345678912345678,123456789123456789]", "must be 18 characters"),
        # Missing closing or opening brackets
        ("123456789123456789,123456789123456789]", "must be 18 characters"),
        ("[123456789123456789,123456789123456789]", "must be 18 characters"),
        # Not only digits
        ("#23456789123456789,123456789123456789", "ust only consist of digits"),
        (["12345678W123456789", "123456789123456789"], "must only consist of digits"),
        # Valid inputs
        ([123456789123456789, 234567891234567891], None),
        (["123456789123456789", "234567891234567891"], None),
        ("[123456789123456789,234567891234567891]", None),
        ("123456789123456789,123456789123456789", None),
        ("123456789123456789", None),
    ],
)
def test__required_metering_point_ids(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelWithMeteringPointIds(metering_point_ids=testcase)
    else:
        model = ModelWithMeteringPointIds(metering_point_ids=testcase)
        _assert_metering_point_ids(model, testcase)


@pytest.mark.parametrize(
    "testcase, match",
    [
        # Empty or invalid inputs
        ([], "Input should be a valid list"),
        ("", "Input should be a valid list"),
        ("[]", "Input should be a valid list"),
        # Too short or too long IDs
        ([1234567891234567890, 123456789123456789], "must be 18 characters"),
        (["12345678912345678", "123456789123456789"], "must be 18 characters"),
        ("1234567891234567890,123456789123456789", "must be 18 characters"),
        ("[12345678912345678,123456789123456789]", "must be 18 characters"),
        # Not only digits
        ("#23456789123456789,123456789123456789", "must only consist of digits"),
        (["12345678W123456789", "1000000000000000"], "must only consist of digits"),
        # Valid inputs
        ([123456789123456789, 234567891234567891], None),
        (["123456789123456789", "234567891234567891"], None),
        ("[123456789123456789,234567891234567891]", None),
        ("123456789123456789,123456789123456789", None),
        ("123456789123456789", None),
    ],
)
def test__optional_energy_supplier_ids(testcase, match):
    # Act & Assert
    if match:
        with pytest.raises(ValidationError, match=match):
            ModelOptionalModelWithMeteringPointIds(metering_point_ids=testcase)
    else:
        model = ModelOptionalModelWithMeteringPointIds(metering_point_ids=testcase)
        _assert_metering_point_ids(model, testcase)
