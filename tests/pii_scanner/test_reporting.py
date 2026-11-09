"""Reviewer report uses persisted evidence without modifying governance state."""

import duckdb

from data_platform.projects.pii_scanner.core.observations import (
    finish_scan,
    record_evaluation,
    record_failed_evaluation,
    start_scan,
)
from data_platform.projects.pii_scanner.core.registry import (
    RegistrationCandidate,
    compare_candidates,
    upsert_candidates,
)
from data_platform.projects.pii_scanner.core.reporting import build_review_report
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnMetadata,
    DetectionAssessment,
    DetectionEvidence,
    DetectionStatus,
    ObjectType,
)


def test_report_exposes_mixed_evidence_and_preserves_review_state():
    con = duckdb.connect(":memory:")
    try:
        column = ColumnMetadata(
            "raw", "customers", "contact_notes", "VARCHAR", 1, ObjectType.TABLE
        )
        result = ColumnDetectionResult(
            column=column,
            name_candidates=(),
            evidence=(
                DetectionEvidence("email", 10, 8, 3),
                DetectionEvidence("phone", 10, 8, 2),
            ),
        )
        assessments = (
            DetectionAssessment("email", DetectionStatus.CANDIDATE, ("value_match",)),
            DetectionAssessment("phone", DetectionStatus.CANDIDATE, ("value_match",)),
        )
        scan_id = start_scan(con, "run-1", [column])
        record_evaluation(con, scan_id, result, assessments)
        finish_scan(con, scan_id)
        candidates = [RegistrationCandidate(result, item) for item in assessments]
        # One governance record for mixed detection, as implemented in the
        # newer local registry. This test catches accidental first-type wins.
        comparison = compare_candidates(con, candidates)
        assert upsert_candidates(con, candidates) == 1
        report = build_review_report(con, comparison, 1, dagster_run_id="run-1")
        assert report["pending_review_count"] == 1
        finding = report["pending_review"][0]
        assert finding["pii_type"] == "mixed"
        assert finding["detected_types"] == ["email", "phone"]
        assert finding["evidence"] == [
            {
                "pii_type": "email",
                "sample_size": 10,
                "non_missing_count": 8,
                "match_count": 3,
            },
            {
                "pii_type": "phone",
                "sample_size": 10,
                "non_missing_count": 8,
                "match_count": 2,
            },
        ]
        assert finding["evidence_from_current_run"] is True
        assert con.execute(
            "SELECT PII_CLASSIFICATION_STATUS FROM governance.pii_column_registry"
        ).fetchone() == ("suggested",)
    finally:
        con.close()


def test_report_marks_old_evidence_and_latest_no_match():
    con = duckdb.connect(":memory:")
    try:
        column = ColumnMetadata(
            "raw", "customers", "email_address", "VARCHAR", 1, ObjectType.TABLE
        )
        candidate_assessment = DetectionAssessment(
            "email", DetectionStatus.CANDIDATE, ("value_match",)
        )
        positive = ColumnDetectionResult(
            column, (), (DetectionEvidence("email", 2, 2, 2),)
        )
        first = start_scan(con, "old-run", [column])
        record_evaluation(con, first, positive, (candidate_assessment,))
        finish_scan(con, first)
        candidate = RegistrationCandidate(positive, candidate_assessment)
        upsert_candidates(con, [candidate])
        second = start_scan(con, "new-run", [column])
        negative = ColumnDetectionResult(
            column, (), (DetectionEvidence("email", 2, 2, 0),)
        )
        record_evaluation(
            con,
            second,
            negative,
            (DetectionAssessment("email", DetectionStatus.NO_MATCH, ()),),
        )
        finish_scan(con, second)
        comparison = compare_candidates(con, [])
        report = build_review_report(con, comparison, 0, dagster_run_id="another-run")
        finding = report["pending_review"][0]
        assert finding["observation_outcome"] == "no_match"
        assert finding["detected_types"] == []
        assert finding["evidence_from_current_run"] is False
        assert finding["evidence"][0]["match_count"] == 0
    finally:
        con.close()


def test_report_distinguishes_latest_failure_from_prior_success():
    """A failed reevaluation must not masquerade as fresh successful evidence."""
    column = ColumnMetadata(
        "raw", "customers", "email_address", "VARCHAR", 1, ObjectType.TABLE
    )
    assessment = DetectionAssessment(
        "email", DetectionStatus.CANDIDATE, ("value_match",)
    )
    result = ColumnDetectionResult(column, (), (DetectionEvidence("email", 10, 10, 8),))

    with duckdb.connect(":memory:") as con:
        # First run successfully identifies PII and registers it for review.
        first = start_scan(con, "successful-run", [column])
        record_evaluation(con, first, result, (assessment,))
        finish_scan(con, first)
        upsert_candidates(con, [RegistrationCandidate(result, assessment)])

        # A later scan attempts the same column but evaluation fails.
        second = start_scan(con, "failed-run", [column])
        record_failed_evaluation(con, second, column, "RuntimeError")
        finish_scan(con, second, error_type="RuntimeError")

        # Explicit timestamps make the historical ordering deterministic.
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET evaluated_at = TIMESTAMPTZ '2026-11-08 10:00:00+00'
            WHERE scan_id = ?
            """,
            [first],
        )
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET evaluated_at = TIMESTAMPTZ '2026-11-09 10:00:00+00'
            WHERE scan_id = ?
            """,
            [second],
        )

        report = build_review_report(
            con,
            compare_candidates(con, []),
            0,
            dagster_run_id="failed-run",
        )

        assert report["pending_review_count"] == 1
        finding = report["pending_review"][0]

        # Preserve the earlier successful evidence for human review.
        assert finding["evidence"][0]["match_count"] == 8
        assert finding["observation_outcome"] == "candidate"
        assert finding["evidence_from_current_run"] is False

        # Separately expose the latest actual evaluation attempt.
        assert finding["latest_evaluation_outcome"] == "failed"
        assert finding["latest_evaluation_error_type"] == "RuntimeError"
        assert finding["latest_evaluation_from_current_run"] is True


def test_report_preserves_historical_evidence_after_incremental_skip():
    column = ColumnMetadata(
        "raw", "customers", "email_address", "VARCHAR", 1, ObjectType.TABLE
    )
    assessment = DetectionAssessment(
        "email", DetectionStatus.CANDIDATE, ("value_match",)
    )
    result = ColumnDetectionResult(column, (), (DetectionEvidence("email", 10, 8, 6),))

    with duckdb.connect(":memory:") as con:
        # Full scan discovers PII and registers it.
        full_scan = start_scan(con, "full-run", [column])
        record_evaluation(con, full_scan, result, (assessment,))
        finish_scan(con, full_scan)
        upsert_candidates(con, [RegistrationCandidate(result, assessment)])

        # Incremental scan skips the structurally unchanged column.
        incremental_scan = start_scan(
            con,
            "incremental-run",
            [column],
            scan_mode="incremental",
        )

        observation = con.execute(
            """
            SELECT outcome, skip_reason, evaluated_at
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [incremental_scan],
        ).fetchone()

        assert observation == ("skipped", "unchanged_schema", None)

        finish_scan(con, incremental_scan)

        report = build_review_report(
            con,
            compare_candidates(con, []),
            0,
            dagster_run_id="incremental-run",
        )

        assert report["pending_review_count"] == 1
        finding = report["pending_review"][0]

        assert finding["observation_outcome"] == "candidate"
        assert finding["latest_evaluation_outcome"] == "candidate"
        assert finding["latest_evaluation_error_type"] is None
        assert finding["evidence"][0]["match_count"] == 6
        assert finding["evidence_from_current_run"] is False
        assert finding["latest_evaluation_from_current_run"] is False
        assert finding["evidence_scan_status"] == "completed"

        # The registry's human-review state remains unchanged.
        assert finding["status"] == "suggested"
