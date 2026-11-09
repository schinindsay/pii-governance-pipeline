"""Coordinate column-name and sampled-value PII detection."""

from collections.abc import Callable

import duckdb

from data_platform.projects.pii_scanner.constants import PII_NAME_PATTERNS
from data_platform.projects.pii_scanner.detectors import VALUE_DETECTORS
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnEvaluationMetrics,
    ColumnMetadata,
    DetectionCandidate,
    DetectionEvidence,
)
from data_platform.projects.pii_scanner.sampling import is_missing, sample_column


def evaluate_sample(
    values: list[object],
    pii_type: str,
    validator: Callable[[str], bool],
) -> DetectionEvidence:
    """Measure how often non-missing values match a PII validator."""
    non_missing = [value for value in values if not is_missing(value)]

    match_count = sum(
        isinstance(value, str) and validator(value) for value in non_missing
    )

    return DetectionEvidence(
        pii_type=pii_type,
        sample_size=len(values),
        non_missing_count=len(non_missing),
        match_count=match_count,
    )


def detect_name_candidates(
    column: ColumnMetadata,
) -> list[DetectionCandidate]:
    """Identify potential PII using column-name patterns."""
    return [
        DetectionCandidate(
            column=column,
            pii_type=pii_type,
            detection_method="column_name",
        )
        for pii_type, pattern in PII_NAME_PATTERNS.items()
        if pattern.search(column.column)
    ]


def detect_column(
    con: duckdb.DuckDBPyConnection,
    column: ColumnMetadata,
    sample_limit: int = 1000,
    *,
    metrics: ColumnEvaluationMetrics | None = None,
) -> ColumnDetectionResult:
    """Run name-based and value-based detection on a column."""
    name_candidates = detect_name_candidates(column)

    values = sample_column(
        con,
        schema=column.schema,
        table=column.table,
        column=column.column,
        limit=sample_limit,
        metrics=metrics,
    )

    evidence = tuple(
        evaluate_sample(
            values,
            pii_type=detector.pii_type,
            validator=detector.validator,
        )
        for detector in VALUE_DETECTORS
    )

    return ColumnDetectionResult(
        column=column,
        name_candidates=tuple(name_candidates),
        evidence=evidence,
    )
