"""Persistence and outcome counts for column observations."""

import json
from dataclasses import asdict

import duckdb

from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnEvaluationMetrics,
    ColumnMetadata,
    DetectionAssessment,
    DetectionStatus,
)


def record_evaluation(
    con: duckdb.DuckDBPyConnection,
    scan_id: str,
    result: ColumnDetectionResult,
    assessments: tuple[DetectionAssessment, ...],
    *,
    metrics: ColumnEvaluationMetrics | None = None,
) -> DetectionStatus:
    """Commit a completed column with evidence counts and detector outcomes."""
    statuses = {assessment.status for assessment in assessments}
    if DetectionStatus.CANDIDATE in statuses:
        outcome = DetectionStatus.CANDIDATE
    elif DetectionStatus.NO_MATCH in statuses:
        outcome = DetectionStatus.NO_MATCH
    else:
        outcome = DetectionStatus.INSUFFICIENT_EVIDENCE

    column = result.column
    updated = con.execute(
        """
        UPDATE scan_state.column_observations
        SET outcome = ?, evaluated_at = current_timestamp, evidence = ?, assessments = ?, metrics = ?
        WHERE scan_id = ? AND source_schema = ? AND relation_name = ? AND column_name = ?
          AND outcome = 'pending'
          AND EXISTS (
              SELECT 1 FROM scan_state.scan_runs
              WHERE scan_runs.scan_id = column_observations.scan_id AND status = 'running'
          )
        RETURNING scan_id
        """,
        [
            outcome,
            json.dumps([asdict(item) for item in result.evidence]),
            json.dumps([asdict(item) for item in assessments]),
            json.dumps(asdict(metrics)) if metrics is not None else None,
            scan_id,
            column.schema,
            column.table,
            column.column,
        ],
    ).fetchall()
    if len(updated) != 1:
        raise RuntimeError(f"Expected one pending observation for scan {scan_id}")
    return outcome


def record_failed_evaluation(
    con: duckdb.DuckDBPyConnection,
    scan_id: str,
    column: ColumnMetadata,
    error_type: str,
    *,
    metrics: ColumnEvaluationMetrics | None = None,
) -> None:
    """Record the error class without persisting the exception message."""
    updated = con.execute(
        """
        UPDATE scan_state.column_observations
        SET outcome = 'failed', evaluated_at = current_timestamp, error_type = ?, metrics = ?
        WHERE scan_id = ? AND source_schema = ? AND relation_name = ? AND column_name = ?
          AND outcome = 'pending'
          AND EXISTS (
              SELECT 1 FROM scan_state.scan_runs
              WHERE scan_runs.scan_id = column_observations.scan_id AND status = 'running'
          )
        RETURNING scan_id
        """,
        [
            error_type,
            json.dumps(asdict(metrics)) if metrics is not None else None,
            scan_id,
            column.schema,
            column.table,
            column.column,
        ],
    ).fetchall()
    if len(updated) != 1:
        raise RuntimeError(f"Expected one pending observation for scan {scan_id}")


def get_scan_outcome_counts(
    con: duckdb.DuckDBPyConnection,
    scan_id: str,
) -> dict[str, int]:
    """Return outcome counts across the entire persisted scan."""
    rows = con.execute(
        """
        SELECT outcome, COUNT(*)
        FROM scan_state.column_observations
        WHERE scan_id = ?
        GROUP BY outcome
        """,
        [scan_id],
    ).fetchall()

    counts = dict(rows)

    return {
        "evaluated_columns": sum(
            counts.get(status, 0)
            for status in (
                "candidate",
                "no_match",
                "insufficient_evidence",
            )
        ),
        "failed_columns": counts.get("failed", 0),
        "pending_columns": counts.get("pending", 0),
        "candidate_columns": counts.get("candidate", 0),
        "no_match_columns": counts.get("no_match", 0),
        "insufficient_evidence_columns": counts.get("insufficient_evidence", 0),
        "skipped_columns": counts.get("skipped", 0),
    }
