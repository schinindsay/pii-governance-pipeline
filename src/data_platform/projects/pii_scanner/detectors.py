"""Value validators and their registry for PII detection."""

import re
from collections.abc import Callable
from dataclasses import dataclass

EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
PHONE_PATTERN = re.compile(r"^\+?[\d\s().-]+$")


def is_email(value: str) -> bool:
    """Recognize plausible email addresses using a simplified pattern."""
    return bool(EMAIL_PATTERN.fullmatch(value))


def is_phone(value: str) -> bool:
    """Recognize plausible phone numbers, not verify ownership."""
    value = value.strip()

    if not PHONE_PATTERN.fullmatch(value):
        return False

    digits = re.sub(r"\D", "", value)

    return 10 <= len(digits) <= 15


@dataclass(frozen=True)
class ValueDetector:
    """Associate a PII classification with a validation function."""

    pii_type: str
    validator: Callable[[str], bool]


VALUE_DETECTORS = (
    ValueDetector(pii_type="email", validator=is_email),
    ValueDetector(pii_type="phone", validator=is_phone),
)
