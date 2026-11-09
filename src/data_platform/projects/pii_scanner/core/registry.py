"""Persistent PII registry schema and initialization.

Adapted from the original production registry schema.
The registry stores detected PII, human review decisions,
and independent masking and deletion workflow state.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb

from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    DetectionAssessment,
    DetectionStatus,
    ObjectType,
)
from data_platform.projects.pii_scanner.sampling import quote_identifier

REGISTRY_SCHEMA = "governance"
REGISTRY_TABLE = "pii_column_registry"

# Preserve the original production column order and definitions.
# DATA_TYPE is an intentional addition for type-aware masking.
REGISTRY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("CREATED_AT", "TIMESTAMP"),
    ("SOURCE_SYSTEM", "VARCHAR"),
    ("DBT_MODEL", "VARCHAR"),
    ("UPSTREAM_MODEL", "VARCHAR"),
    ("FIELD", "VARCHAR"),
    ("DESCRIPTION", "VARCHAR"),
    ("MASKING_STATUS", "VARCHAR"),
    ("LOOKUP_KEY", "VARCHAR"),
    ("ANONYMIZATION_QUERY", "VARCHAR"),
    ("NOTES", "VARCHAR"),
    ("MATERIALIZATION", "VARCHAR"),
    ("LINEAGE_GROUP", "VARCHAR"),
    ("DETECTED_BY", "VARCHAR"),
    ("PII_TYPE", "VARCHAR"),
    ("PII_CLASSIFICATION_STATUS", "VARCHAR"),
    ("CONFIDENCE_SCORE", "DECIMAL(3,2)"),
    ("DETECTION_METHOD", "VARCHAR"),
    ("REVIEWED_AT", "TIMESTAMP"),
    ("REVIEWED_BY", "VARCHAR"),
    ("PR_NUMBER", "BIGINT"),
    ("PR_URL", "VARCHAR"),
    ("DATA_TYPE", "VARCHAR"),
)


def registry_table_name(
    schema: str = REGISTRY_SCHEMA,
    table: str = REGISTRY_TABLE,
) -> str:
    """Return a safely quoted, schema-qualified table name."""
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def create_registry_sql(fq_table: str) -> str:
    """Build the canonical CREATE TABLE statement."""
    column_defs = ",\n    ".join(
        f"{name} {data_type}" for name, data_type in REGISTRY_COLUMNS
    )
    return f"CREATE TABLE IF NOT EXISTS {fq_table} (\n    {column_defs}\n)"


def ensure_columns_sql(
    fq_table: str,
    existing_columns: list[str] | None = None,
) -> list[str]:
    """Generate ALTER statements only for missing registry columns."""
    present = (
        {name.upper() for name in existing_columns}
        if existing_columns is not None
        else set()
    )

    return [
        f"ALTER TABLE {fq_table} ADD COLUMN IF NOT EXISTS {name} {data_type}"
        for name, data_type in REGISTRY_COLUMNS
        if name.upper() not in present
    ]


def get_existing_columns(
    con: duckdb.DuckDBPyConnection,
    schema: str = REGISTRY_SCHEMA,
    table: str = REGISTRY_TABLE,
) -> list[str]:
    """Read the existing registry columns from DuckDB metadata."""
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()

    return [row[0] for row in rows]


def ensure_registry(
    con: duckdb.DuckDBPyConnection,
    schema: str = REGISTRY_SCHEMA,
    table: str = REGISTRY_TABLE,
) -> None:
    """Create or migrate the registry without modifying existing rows."""
    fq_table = registry_table_name(schema, table)

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema)}")

    con.execute(create_registry_sql(fq_table))

    existing_columns = get_existing_columns(con, schema, table)

    for statement in ensure_columns_sql(fq_table, existing_columns):
        con.execute(statement)


@dataclass(frozen=True)
class RegistrationCandidate:
    """A classified detection eligible for registry comparison."""

    result: ColumnDetectionResult
    assessment: DetectionAssessment


@dataclass(frozen=True)
class RegistrationResult:
    """Outcome of comparing candidates with the existing registry."""

    net_new: tuple[RegistrationCandidate, ...]
    previously_rejected: tuple[RegistrationCandidate, ...]
    already_accepted: int
    awaiting_review: int


def registry_match_key(model: str, field: str) -> tuple[str, str]:
    """Match registry identity case-insensitively, as in production."""
    return model.casefold(), field.casefold()


def consolidate_candidates(
    candidates: list[RegistrationCandidate],
) -> list[RegistrationCandidate]:
    """Produce one registration candidate per column.

    Preserve individual PII evidence in scan observations.
    Multiple distinct positive PII types become 'mixed'.
    """
    grouped: dict[tuple[str, str], list[RegistrationCandidate]] = {}

    for candidate in candidates:
        if candidate.assessment.status != DetectionStatus.CANDIDATE:
            continue

        column = candidate.result.column
        key = registry_match_key(column.table, column.column)
        grouped.setdefault(key, []).append(candidate)

    consolidated = []

    for group in grouped.values():
        first = group[0]
        pii_types = {item.assessment.pii_type for item in group}
        reasons = {reason for item in group for reason in item.assessment.reasons}

        consolidated.append(
            RegistrationCandidate(
                result=first.result,
                assessment=DetectionAssessment(
                    pii_type=(
                        next(iter(pii_types)) if len(pii_types) == 1 else "mixed"
                    ),
                    status=DetectionStatus.CANDIDATE,
                    reasons=tuple(sorted(reasons)),
                ),
            )
        )

    return consolidated


def compare_candidates(
    con: duckdb.DuckDBPyConnection,
    candidates: list[RegistrationCandidate],
) -> RegistrationResult:
    """Separate new detections from existing reviewed or suggested rows."""
    ensure_registry(con)

    rows = con.execute(
        f"""
        SELECT DBT_MODEL, FIELD, PII_CLASSIFICATION_STATUS
        FROM {registry_table_name()}
        """
    ).fetchall()

    existing = {
        registry_match_key(model, field): status for model, field, status in rows
    }

    net_new = []
    previously_rejected = []
    already_accepted = 0
    awaiting_review = 0

    for candidate in consolidate_candidates(candidates):
        if candidate.assessment.status != DetectionStatus.CANDIDATE:
            continue

        column = candidate.result.column
        key = registry_match_key(column.table, column.column)
        status = existing.get(key)

        if status is None:
            net_new.append(candidate)
        elif status.lower() == "rejected":
            previously_rejected.append(candidate)
        elif status.lower() == "accepted":
            already_accepted += 1
        elif status.lower() == "suggested":
            awaiting_review += 1
        else:
            raise ValueError(f"Unexpected registry status {status!r} for {key}")

    return RegistrationResult(
        net_new=tuple(net_new),
        previously_rejected=tuple(previously_rejected),
        already_accepted=already_accepted,
        awaiting_review=awaiting_review,
    )


def masking_status_for(object_type: ObjectType) -> str | None:
    """Seed the original remediation status based on materialization."""
    if object_type == ObjectType.VIEW:
        return "mask_pii pending"
    if object_type == ObjectType.TABLE:
        return "DDL policy pending"
    return None


def detection_method_for(
    candidate: RegistrationCandidate,
) -> str:
    """Describe which detection signals identified the candidate."""
    reasons = candidate.assessment.reasons
    has_name = "column_name_match" in reasons
    has_value = "value_match" in reasons

    if has_name and has_value:
        return "column_name,value"
    if has_name:
        return "column_name"
    if has_value:
        return "value"
    raise ValueError("Candidate has no detection reason")


def upsert_candidates(
    con: duckdb.DuckDBPyConnection,
    candidates: list[RegistrationCandidate],
) -> int:
    """Insert new candidates without overwriting registered columns."""
    ensure_registry(con)

    inserted = 0
    table = registry_table_name()

    con.execute("BEGIN TRANSACTION")
    try:
        for candidate in consolidate_candidates(candidates):
            if candidate.assessment.status != DetectionStatus.CANDIDATE:
                continue

            column = candidate.result.column

            materialization = (
                "view" if column.object_type == ObjectType.VIEW else "table"
            )

            row = con.execute(
                f"""
                INSERT INTO {table} (
                    CREATED_AT, SOURCE_SYSTEM, DBT_MODEL, FIELD,
                    MATERIALIZATION, MASKING_STATUS, DETECTED_BY,
                    PII_TYPE, PII_CLASSIFICATION_STATUS,
                    DETECTION_METHOD, DATA_TYPE
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table}
                    WHERE lower(DBT_MODEL) = lower(?)
                      AND lower(FIELD) = lower(?)
                )
                RETURNING DBT_MODEL
                """,
                [
                    datetime.now(timezone.utc).replace(tzinfo=None),
                    column.schema,
                    column.table,
                    column.column,
                    materialization,
                    masking_status_for(column.object_type),
                    "scanner",
                    candidate.assessment.pii_type,
                    "suggested",
                    detection_method_for(candidate),
                    column.data_type,
                    column.table,
                    column.column,
                ],
            ).fetchone()

            if row is not None:
                inserted += 1

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return inserted
