"""Human-review reporting from governance state and durable scan observations.

This module is read-only: detection evidence never changes review decisions.
"""

import json
from collections import Counter

import duckdb

from data_platform.projects.pii_scanner.core.registry import RegistrationResult


def _decode_json(value):
    if value is None:
        return []
    return json.loads(value) if isinstance(value, str) else value


def build_review_report(
    con: duckdb.DuckDBPyConnection,
    comparison: RegistrationResult,
    newly_registered: int,
    *,
    dagster_run_id: str,
) -> dict:
    """Build a reviewer-oriented report without exposing sampled values."""
    # Preserve the latest successful evidence independently of the latest attempt.
    # A failed reevaluation must be visible without discarding prior evidence.
    rows = con.execute(
        """
        WITH observations AS (
            SELECT o.source_schema, o.relation_name, o.column_name,
                   o.outcome, o.error_type, o.evaluated_at,
                   o.evidence, o.assessments,
                   s.dagster_run_id, s.status AS scan_status,
                   s.started_at, s.scan_id
            FROM scan_state.column_observations o
            JOIN scan_state.scan_runs s ON s.scan_id = o.scan_id
            WHERE o.evaluated_at IS NOT NULL
              AND o.outcome IN (
                  'candidate', 'no_match', 'insufficient_evidence', 'failed'
              )
        ),
        latest_attempt AS (
            SELECT *, row_number() OVER (
                PARTITION BY lower(source_schema), lower(relation_name),
                             lower(column_name)
                ORDER BY evaluated_at DESC, started_at DESC, scan_id DESC
            ) AS rn
            FROM observations
        ),
        latest_success AS (
            SELECT *, row_number() OVER (
                PARTITION BY lower(source_schema), lower(relation_name),
                             lower(column_name)
                ORDER BY evaluated_at DESC, started_at DESC, scan_id DESC
            ) AS rn
            FROM observations
            WHERE outcome != 'failed'
        )
        SELECT r.SOURCE_SYSTEM, r.DBT_MODEL, r.FIELD, r.PII_TYPE,
               r.PII_CLASSIFICATION_STATUS, r.DETECTION_METHOD,
               ok.outcome, ok.evaluated_at, ok.evidence, ok.assessments,
               ok.dagster_run_id, ok.scan_status,
               latest.outcome, latest.error_type, latest.dagster_run_id
        FROM governance.pii_column_registry r
        LEFT JOIN latest_success ok
          ON lower(r.SOURCE_SYSTEM) = lower(ok.source_schema)
         AND lower(r.DBT_MODEL) = lower(ok.relation_name)
         AND lower(r.FIELD) = lower(ok.column_name)
         AND ok.rn = 1
        LEFT JOIN latest_attempt latest
          ON lower(r.SOURCE_SYSTEM) = lower(latest.source_schema)
         AND lower(r.DBT_MODEL) = lower(latest.relation_name)
         AND lower(r.FIELD) = lower(latest.column_name)
         AND latest.rn = 1
        WHERE lower(r.PII_CLASSIFICATION_STATUS) = 'suggested'
        ORDER BY lower(r.DBT_MODEL), lower(r.FIELD)
        """
    ).fetchall()

    pending = []
    for (
        source,
        model,
        field,
        pii_type,
        status,
        method,
        outcome,
        evaluated_at,
        evidence_json,
        assessments_json,
        observation_run_id,
        scan_status,
        latest_outcome,
        latest_error_type,
        latest_run_id,
    ) in rows:
        evidence = sorted(
            (
                {
                    "pii_type": item["pii_type"],
                    "sample_size": item["sample_size"],
                    "non_missing_count": item["non_missing_count"],
                    "match_count": item["match_count"],
                }
                for item in _decode_json(evidence_json)
            ),
            key=lambda item: item["pii_type"],
        )
        assessments = _decode_json(assessments_json)
        detected_types = sorted(
            {item["pii_type"] for item in assessments if item["status"] == "candidate"}
        )
        pending.append(
            {
                "source": source,
                "model": model,
                "field": field,
                "pii_type": pii_type,
                "status": status,
                "detection_method": method,
                "detected_types": detected_types,
                "evidence": evidence,
                "observation_outcome": outcome,
                "evidence_evaluated_at": (
                    evaluated_at.isoformat() if evaluated_at else None
                ),
                "evidence_scan_status": scan_status,
                "evidence_from_current_run": observation_run_id == dagster_run_id,
                "latest_evaluation_outcome": latest_outcome,
                "latest_evaluation_error_type": latest_error_type,
                "latest_evaluation_from_current_run": latest_run_id == dagster_run_id,
                "review_command": (
                    f"uv run python scripts/review_pii.py accept "
                    f"{model} {field} --reviewed-by YOUR_ID"
                ),
            }
        )

    current_candidates = [*comparison.net_new, *comparison.previously_rejected]
    by_type = dict(
        sorted(
            Counter(
                candidate.assessment.pii_type for candidate in current_candidates
            ).items()
        )
    )
    return {
        "newly_registered": newly_registered,
        "current_run": {
            "net_new": len(comparison.net_new),
            "previously_rejected": len(comparison.previously_rejected),
            "already_accepted": comparison.already_accepted,
            "awaiting_review": comparison.awaiting_review,
            "by_type": by_type,
            "previously_rejected_findings": [
                {
                    "model": item.result.column.table,
                    "field": item.result.column.column,
                    "detected_pii_type": item.assessment.pii_type,
                    "action": "revisit existing rejection; no automatic change",
                }
                for item in comparison.previously_rejected
            ],
        },
        "pending_review": pending,
        "pending_review_count": len(pending),
        "review_instructions": (
            "Inspect the evidence, then run scripts/review_pii.py with "
            "accept or reject, model, field, and --reviewed-by. "
            "No review decision is made by this report."
        ),
        # Retain existing top-level fields for downstream consumers.
        "previously_rejected": len(comparison.previously_rejected),
        "already_accepted": comparison.already_accepted,
    }
