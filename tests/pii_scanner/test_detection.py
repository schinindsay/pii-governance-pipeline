import pytest

from data_platform.projects.pii_scanner.core.detection import (
    detect_name_candidates,
)
from data_platform.projects.pii_scanner.models import (
    ColumnMetadata,
    ObjectType,
)


@pytest.mark.parametrize(
    ("column_name", "expected_types"),
    [
        ("email", {"email"}),
        ("customer_email", {"email"}),
        ("EMAIL_ADDRESS", {"email"}),
        ("phone_number", {"phone"}),
        ("mobile", {"phone"}),
        ("ssn", {"ssn"}),
        ("social_security_number", {"ssn"}),
        ("total_amount", set()),
        ("customer_id", set()),
    ],
)
def test_detect_name_candidates(column_name, expected_types):
    column = ColumnMetadata(
        schema="raw",
        table="customers",
        column=column_name,
        data_type="VARCHAR",
        ordinal_position=1,
        object_type=ObjectType.TABLE,
    )

    candidates = detect_name_candidates(column)

    assert {candidate.pii_type for candidate in candidates} == expected_types
    assert all(candidate.detection_method == "column_name" for candidate in candidates)
