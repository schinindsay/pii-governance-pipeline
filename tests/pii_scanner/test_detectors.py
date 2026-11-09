import pytest

from data_platform.projects.pii_scanner.detectors import (
    VALUE_DETECTORS,
    ValueDetector,
    is_email,
    is_phone,
)


def test_email_detector():
    assert is_email("a@example.com")
    assert not is_email("not-an-email")


def test_detector_registry_contains_email():
    email_detectors = [
        detector for detector in VALUE_DETECTORS if detector.pii_type == "email"
    ]

    assert len(email_detectors) == 1
    assert isinstance(email_detectors[0], ValueDetector)
    assert email_detectors[0].validator("a@example.com")


def test_detector_registry_contains_phone():
    phone_detectors = [
        detector for detector in VALUE_DETECTORS if detector.pii_type == "phone"
    ]

    assert len(phone_detectors) == 1
    assert isinstance(phone_detectors[0], ValueDetector)
    assert phone_detectors[0].validator("919-555-0123")


def test_detector_registry_has_unique_pii_types():
    pii_types = [detector.pii_type for detector in VALUE_DETECTORS]

    assert len(pii_types) == len(set(pii_types))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("919-555-0123", True),
        ("(919) 555-0123", True),
        ("9195550123", True),
        ("+1 919 555 0123", True),
        ("12345", False),
        ("not a phone number", False),
        ("919-555-0123 ext 45", False),
        ("", False),
        ("1234567890123456", False),
    ],
)
def test_is_phone(value, expected):
    assert is_phone(value) is expected
