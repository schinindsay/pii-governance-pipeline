"""Column-name patterns used to identify potential PII."""

import re

PII_NAME_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"(^|_)e?mail(_|$)", re.IGNORECASE),
    "phone": re.compile(
        r"(^|_)(phone|mobile|telephone|cell)(_number)?(_|$)",
        re.IGNORECASE,
    ),
    "ssn": re.compile(
        r"(^|_)(ssn|social_security_number)(_|$)",
        re.IGNORECASE,
    ),
}
