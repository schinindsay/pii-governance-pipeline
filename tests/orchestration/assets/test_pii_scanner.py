"""Tests for Dagster PII scanner asset wiring."""

import json
from dataclasses import replace

import dagster as dg
import duckdb
import pytest

from data_platform.orchestration.assets import pii_scanner as scanner_assets
from data_platform.orchestration.definitions import defs
from data_platform.orchestration.jobs.pii_scanner import (
    pii_assets,
    pii_full_scan,
    pii_incremental_scan,
)
from data_platform.orchestration.resources import DuckDBResource


def test_scanner_collection_contains_all_five_assets():
    assert isinstance(defs.resources["warehouse"], DuckDBResource)

    asset_defs = pii_assets
    assert len(asset_defs) == 5

    names = {key.path[-1] for asset_def in asset_defs for key in asset_def.keys}

    assert names == {
        "schema_snapshot",
        "pii_detection_candidates",
        "pii_candidates_for_registration",
        "registry_upsert",
        "pii_detection_report",
    }


@pytest.mark.parametrize("review_status", ["accepted", "rejected"])
def test_end_to_end_pipeline_is_idempotent(tmp_path, review_status):
    database_path = tmp_path / "test.duckdb"

    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute(
            """
            CREATE TABLE raw.customers (
                id INTEGER,
                email_address VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO raw.customers VALUES
                (1, 'a@example.com'),
                (2, 'b@example.com'),
                (3, NULL)
            """
        )

    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}

    first = dg.materialize(pii_assets, resources=resources)
    assert first.success

    with duckdb.connect(str(database_path)) as con:
        first_rows = con.execute(
            """
            SELECT DBT_MODEL, FIELD,
                   PII_CLASSIFICATION_STATUS
            FROM governance.pii_column_registry
            """
        ).fetchall()

    assert first_rows == [("customers", "email_address", "suggested")]

    with duckdb.connect(str(database_path)) as con:
        con.execute(
            """
            UPDATE governance.pii_column_registry
            SET PII_CLASSIFICATION_STATUS = ?, REVIEWED_BY = 'reviewer',
                ANONYMIZATION_QUERY = 'curated query'
            """,
            [review_status],
        )
        reviewed_rows = con.execute(
            "SELECT * FROM governance.pii_column_registry"
        ).fetchall()
        assert len(reviewed_rows[0]) == 22

    second = dg.materialize(pii_assets, resources=resources)
    assert second.success

    with duckdb.connect(str(database_path)) as con:
        assert (
            con.execute("SELECT * FROM governance.pii_column_registry").fetchall()
            == reviewed_rows
        )
        assert con.execute(
            "SELECT count(*) FROM scan_state.scan_runs WHERE status = 'completed'"
        ).fetchone() == (2,)
        assert con.execute(
            "SELECT count(*) FROM scan_state.column_observations WHERE outcome = 'candidate'"
        ).fetchone() == (2,)


@pytest.mark.parametrize("fail_in_query", [False, True])
def test_scan_failure_persists_completed_and_unreached_columns(
    tmp_path, monkeypatch, fail_in_query
):
    database_path = tmp_path / "test.duckdb"
    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("""
            CREATE TABLE raw.items (contact VARCHAR, detail VARCHAR, quantity INTEGER)
        """)
        con.execute("INSERT INTO raw.items VALUES ('secret@example.com', NULL, 1)")

    detect_column = scanner_assets.detect_column
    attempted = []

    def fail_second_column(con, column, *, metrics):
        attempted.append(column.column)
        if column.column == "detail":
            if fail_in_query:
                column = replace(column, column="missing_column")
            else:
                raise RuntimeError("Injected failure with secret@example.com")
        return detect_column(con, column, metrics=metrics)

    monkeypatch.setattr(scanner_assets, "detect_column", fail_second_column)
    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}
    result = dg.materialize(pii_assets, resources=resources, raise_on_error=False)
    assert not result.success
    assert attempted == ["contact", "detail", "quantity", "detail"]
    error_type = "BinderException" if fail_in_query else "RuntimeError"
    failure = result.failure_data_for_node("pii_detection_candidates")
    metadata = failure.user_failure_data.metadata
    assert "secret@example.com" not in failure.error.to_string()
    expected_metrics = {
        "discovered_columns": 3,
        "attempted_columns": 1,
        "evaluated_columns": 2,
        "failed_columns": 1,
        "pending_columns": 0,
        "candidate_columns": 1,
        "no_match_columns": 1,
        "insufficient_evidence_columns": 0,
        "sampling_queries": 1 if fail_in_query else 0,
        "sampled_values": 0,
        "non_null_sampled_values": 0,
    }
    assert {key: metadata[key].value for key in expected_metrics} == expected_metrics
    assert metadata["detection_duration_seconds"].value > 0

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            """
            SELECT dagster_run_id, status, finished_at IS NOT NULL, error_type
            FROM scan_state.scan_runs
            """
        ).fetchall() == [(result.run_id, "failed", True, error_type)]
        assert con.execute(
            """
            SELECT column_name, outcome, evaluated_at IS NOT NULL, error_type
            FROM scan_state.column_observations
            ORDER BY ordinal_position
            """
        ).fetchall() == [
            ("contact", "candidate", True, None),
            ("detail", "failed", True, error_type),
            ("quantity", "no_match", True, None),
        ]
        failed_metrics, quantity_metrics = con.execute(
            """
            SELECT metrics FROM scan_state.column_observations
            WHERE column_name IN ('detail', 'quantity') ORDER BY ordinal_position
            """
        ).fetchall()

        assert json.loads(quantity_metrics[0])["sampling_queries"] == 1
        counts = json.loads(failed_metrics[0])
        elapsed = counts.pop("evaluation_duration_seconds")
        assert 0 < elapsed <= metadata["detection_duration_seconds"].value
        assert counts == {
            "sampling_queries": int(fail_in_query),
            "sampled_values": 0,
            "non_null_sampled_values": 0,
        }
        evidence, assessments = con.execute(
            """
            SELECT evidence, assessments FROM scan_state.column_observations
            WHERE column_name = 'contact'
            """
        ).fetchone()
        assert "secret@example.com" not in evidence + assessments
        assert any(
            item["pii_type"] == "email" and item["match_count"] == 1
            for item in json.loads(evidence)
        )
        assert con.execute(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = 'governance' AND table_name = 'pii_column_registry'
            """
        ).fetchone() == (0,)


def test_mixed_pii_end_to_end_preserves_review(tmp_path):
    database_path = tmp_path / "test.duckdb"

    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("""
            CREATE TABLE raw.contacts (
                id INTEGER,
                contact_notes VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO raw.contacts VALUES
                (1, 'person@example.com'),
                (2, '919-555-1234'),
                (3, NULL)
        """)

    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}

    first = dg.materialize(pii_assets, resources=resources)
    assert first.success

    with duckdb.connect(str(database_path)) as con:
        registry_rows = con.execute("""
            SELECT DBT_MODEL, FIELD, PII_TYPE,
                   PII_CLASSIFICATION_STATUS, DETECTION_METHOD
            FROM governance.pii_column_registry
        """).fetchall()

        assert registry_rows == [
            ("contacts", "contact_notes", "mixed", "suggested", "value")
        ]

        evidence_json, assessments_json = con.execute("""
            SELECT evidence, assessments
            FROM scan_state.column_observations
            WHERE column_name = 'contact_notes'
        """).fetchone()

        evidence = {item["pii_type"]: item for item in json.loads(evidence_json)}
        assessments = {item["pii_type"]: item for item in json.loads(assessments_json)}

        assert evidence["email"]["match_count"] == 1
        assert evidence["phone"]["match_count"] == 1
        assert assessments["email"]["status"] == "candidate"
        assert assessments["phone"]["status"] == "candidate"

        con.execute("""
            UPDATE governance.pii_column_registry
            SET PII_CLASSIFICATION_STATUS = 'accepted',
                REVIEWED_BY = 'reviewer',
                ANONYMIZATION_QUERY = 'curated query',
                LOOKUP_KEY = 'id'
            WHERE FIELD = 'contact_notes'
        """)

        reviewed_rows = con.execute(
            "SELECT * FROM governance.pii_column_registry"
        ).fetchall()

    second = dg.materialize(pii_assets, resources=resources)
    assert second.success

    with duckdb.connect(str(database_path)) as con:
        assert (
            con.execute("SELECT * FROM governance.pii_column_registry").fetchall()
            == reviewed_rows
        )

        assert con.execute("""
            SELECT COUNT(*)
            FROM scan_state.column_observations
            WHERE column_name = 'contact_notes'
              AND outcome = 'candidate'
        """).fetchone() == (2,)


def test_zero_finding_scan_records_evaluation_outcomes(tmp_path, monkeypatch):
    database_path = tmp_path / "test.duckdb"
    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.items (quantity INTEGER, detail VARCHAR)")
        con.execute("INSERT INTO raw.items VALUES (1, NULL), (2, ' ')")

    times = iter([10.0, 11.0, 13.0, 14.0, 17.0, 19.0])
    monkeypatch.setattr(scanner_assets, "perf_counter", lambda: next(times))

    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}
    result = dg.materialize(pii_assets, resources=resources)
    assert result.success

    [materialization] = result.asset_materializations_for_node(
        "pii_detection_candidates"
    )
    expected_metrics = {
        "discovered_columns": 2,
        "attempted_columns": 2,
        "evaluated_columns": 2,
        "failed_columns": 0,
        "pending_columns": 0,
        "candidate_columns": 0,
        "no_match_columns": 1,
        "insufficient_evidence_columns": 1,
        "sampling_queries": 2,
        "sampled_values": 4,
        "non_null_sampled_values": 3,
        "detection_duration_seconds": 9.0,
    }
    assert {
        key: materialization.metadata[key].value for key in expected_metrics
    } == expected_metrics

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            """
            SELECT dagster_run_id, status, finished_at IS NOT NULL, error_type
            FROM scan_state.scan_runs
            """
        ).fetchall() == [(result.run_id, "completed", True, None)]
        assert con.execute(
            """
            SELECT column_name, outcome, evaluated_at IS NOT NULL
            FROM scan_state.column_observations
            ORDER BY ordinal_position
            """
        ).fetchall() == [
            ("quantity", "no_match", True),
            ("detail", "insufficient_evidence", True),
        ]
        metrics = con.execute(
            "SELECT metrics FROM scan_state.column_observations ORDER BY ordinal_position"
        ).fetchall()
        assert [json.loads(row[0]) for row in metrics] == [
            {
                "sampling_queries": 1,
                "sampled_values": 2,
                "non_null_sampled_values": 2,
                "evaluation_duration_seconds": 2.0,
            },
            {
                "sampling_queries": 1,
                "sampled_values": 2,
                "non_null_sampled_values": 1,
                "evaluation_duration_seconds": 3.0,
            },
        ]
        assert con.execute(
            "SELECT count(*) FROM governance.pii_column_registry"
        ).fetchone() == (0,)


def test_observation_write_failure_does_not_complete_scan(tmp_path, monkeypatch):
    database_path = tmp_path / "test.duckdb"
    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.items (contact VARCHAR, detail VARCHAR)")
        con.execute("INSERT INTO raw.items VALUES ('person@example.com', NULL)")

    record_evaluation = scanner_assets.record_evaluation

    def fail_second_write(con, scan_id, result, assessments, *, metrics):
        if result.column.column == "detail":
            raise duckdb.IOException("Injected observation write failure")
        return record_evaluation(con, scan_id, result, assessments, metrics=metrics)

    monkeypatch.setattr(scanner_assets, "record_evaluation", fail_second_write)
    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}
    result = dg.materialize(pii_assets, resources=resources, raise_on_error=False)
    assert not result.success
    failure = result.failure_data_for_node("pii_detection_candidates")
    metadata = failure.user_failure_data.metadata
    assert metadata["attempted_columns"].value == 1
    assert metadata["evaluated_columns"].value == 1
    assert metadata["failed_columns"].value == 0
    assert metadata["pending_columns"].value == 1
    assert metadata["sampling_queries"].value == 1

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            """
            SELECT status, error_type, finished_at IS NOT NULL
            FROM scan_state.scan_runs WHERE dagster_run_id = ?
            """,
            [result.run_id],
        ).fetchone() == ("failed", "IOException", True)
        assert con.execute(
            """
            SELECT column_name, outcome, evaluated_at IS NOT NULL
            FROM scan_state.column_observations ORDER BY ordinal_position
            """
        ).fetchall() == [("contact", "candidate", True), ("detail", "pending", False)]


def test_empty_inventory_completes_without_observations(tmp_path):
    database_path = tmp_path / "test.duckdb"
    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")

    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}
    result = dg.materialize(pii_assets, resources=resources)
    assert result.success

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            """
            SELECT dagster_run_id, status, finished_at IS NOT NULL
            FROM scan_state.scan_runs
            """
        ).fetchall() == [(result.run_id, "completed", True)]
        assert con.execute(
            "SELECT count(*) FROM scan_state.column_observations"
        ).fetchone() == (0,)


def test_detection_retry_resumes_checkpoint_without_resampling(tmp_path, monkeypatch):
    database_path = tmp_path / "test.duckdb"

    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("""
            CREATE TABLE raw.items (
                contact VARCHAR,
                detail VARCHAR,
                quantity INTEGER
            )
        """)
        con.execute("INSERT INTO raw.items VALUES ('secret@example.com', NULL, 1)")

    original_detect_column = scanner_assets.detect_column
    attempted = []
    detail_attempts = 0

    def fail_detail_once(con, column, *, metrics):
        nonlocal detail_attempts
        attempted.append(column.column)

        if column.column == "detail":
            detail_attempts += 1
            if detail_attempts == 1:
                raise RuntimeError("Injected transient detection failure")

        return original_detect_column(con, column, metrics=metrics)

    monkeypatch.setattr(scanner_assets, "detect_column", fail_detail_once)

    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}

    result = dg.materialize(
        pii_assets,
        resources=resources,
        raise_on_error=False,
    )

    assert result.success
    assert attempted == ["contact", "detail", "quantity", "detail"]

    with duckdb.connect(str(database_path)) as con:
        scans = con.execute("""
            SELECT scan_id, dagster_run_id, status
            FROM scan_state.scan_runs
        """).fetchall()

        assert len(scans) == 1
        assert scans[0][1] == result.run_id
        assert scans[0][2] == "completed"

        observations = con.execute("""
            SELECT column_name, outcome
            FROM scan_state.column_observations
            ORDER BY ordinal_position
        """).fetchall()

        assert observations == [
            ("contact", "candidate"),
            ("detail", "insufficient_evidence"),
            ("quantity", "no_match"),
        ]

        assert con.execute("""
            SELECT DBT_MODEL, FIELD
            FROM governance.pii_column_registry
        """).fetchall() == [("items", "contact")]


def test_full_then_incremental_job_preserves_review_and_skips_unchanged(tmp_path):
    database_path = tmp_path / "test.duckdb"

    with duckdb.connect(str(database_path)) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("""
            CREATE TABLE raw.customers (
                id INTEGER,
                email_address VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO raw.customers VALUES
                (1, 'a@example.com'),
                (2, 'b@example.com'),
                (3, NULL)
        """)

    resources = {"warehouse": DuckDBResource(database_path=str(database_path))}

    test_defs = dg.Definitions(
        assets=pii_assets,
        jobs=[pii_full_scan, pii_incremental_scan],
        resources=resources,
    )

    full_job = test_defs.resolve_job_def("pii_full_scan")
    incremental_job = test_defs.resolve_job_def("pii_incremental_scan")

    full_result = full_job.execute_in_process()
    assert full_result.success

    with duckdb.connect(str(database_path)) as con:
        assert con.execute("""
            SELECT scan_mode, status
            FROM scan_state.scan_runs
        """).fetchall() == [("full", "completed")]

        con.execute("""
            UPDATE governance.pii_column_registry
            SET PII_CLASSIFICATION_STATUS = 'accepted',
                REVIEWED_BY = 'reviewer',
                ANONYMIZATION_QUERY = 'curated query'
            WHERE FIELD = 'email_address'
        """)

        reviewed_rows = con.execute(
            "SELECT * FROM governance.pii_column_registry"
        ).fetchall()

    incremental_result = incremental_job.execute_in_process()
    assert incremental_result.success

    with duckdb.connect(str(database_path)) as con:
        assert con.execute("""
            SELECT scan_mode, status
            FROM scan_state.scan_runs
            ORDER BY started_at
        """).fetchall() == [
            ("full", "completed"),
            ("incremental", "completed"),
        ]

        assert con.execute("""
            SELECT column_name, outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = (
                SELECT scan_id
                FROM scan_state.scan_runs
                WHERE scan_mode = 'incremental'
            )
            ORDER BY ordinal_position
        """).fetchall() == [
            ("id", "skipped", "unchanged_schema"),
            ("email_address", "skipped", "unchanged_schema"),
        ]

        assert (
            con.execute("SELECT * FROM governance.pii_column_registry").fetchall()
            == reviewed_rows
        )
