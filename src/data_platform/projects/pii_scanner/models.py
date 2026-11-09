"""Domain models shared across PII scanner components."""

from dataclasses import dataclass
from enum import StrEnum


class ObjectType(StrEnum):
    """Warehouse object types currently supported by the scanner.

    This is intentionally limited to ordinary tables and views.
    Other warehouse object types require explicit handling.
    """

    TABLE = "BASE TABLE"
    VIEW = "VIEW"


@dataclass(frozen=True)
class ColumnMetadata:
    """Metadata describing a discovered warehouse column."""

    schema: str
    table: str
    column: str
    data_type: str
    ordinal_position: int
    object_type: ObjectType


@dataclass(frozen=True)
class DetectionCandidate:
    """A potential PII classification suggested by column metadata."""

    column: ColumnMetadata
    pii_type: str
    detection_method: str


@dataclass(frozen=True)
class DetectionEvidence:
    """Observed validator results for a sampled column."""

    pii_type: str
    sample_size: int
    non_missing_count: int
    match_count: int

    @property
    def coverage(self) -> float | None:
        """Fraction of sampled rows containing non-missing values."""
        if self.sample_size == 0:
            return None
        return self.non_missing_count / self.sample_size

    @property
    def match_ratio(self) -> float | None:
        """Fraction of non-missing values matching the validator."""
        if self.non_missing_count == 0:
            return None
        return self.match_count / self.non_missing_count


@dataclass
class ColumnEvaluationMetrics:
    """Counts and elapsed time for one attempted column evaluation."""

    sampling_queries: int = 0
    sampled_values: int = 0
    non_null_sampled_values: int = 0
    evaluation_duration_seconds: float | None = None


@dataclass(frozen=True)
class ColumnDetectionResult:
    """Combined name-based and value-based observations for a column."""

    column: ColumnMetadata
    name_candidates: tuple[DetectionCandidate, ...]
    evidence: tuple[DetectionEvidence, ...]


class DetectionStatus(StrEnum):
    """Classification outcomes before human governance review."""

    CANDIDATE = "candidate"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    NO_MATCH = "no_match"


@dataclass(frozen=True)
class DetectionAssessment:
    """Classification outcome and supporting reasons for a PII type."""

    pii_type: str
    status: DetectionStatus
    reasons: tuple[str, ...]
