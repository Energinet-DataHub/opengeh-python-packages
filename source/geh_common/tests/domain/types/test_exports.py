from geh_common.domain import types
from geh_common.domain.types.business_reason import BusinessReason
from geh_common.domain.types.relation_type import RelationType


def test_public_domain_types_are_exported() -> None:
    assert types.BusinessReason is BusinessReason
    assert types.RelationType is RelationType
    assert {"BusinessReason", "RelationType"} <= set(types.__all__)
