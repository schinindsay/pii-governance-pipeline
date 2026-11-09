"""Tests for durable scan state transitions."""

import json
from dataclasses import replace
from uuid import uuid4

import duckdb
import pytest

from data_platform.projects.pii_scanner.core.observations import (
    finish_scan,
    load_previous_evaluations,
    prepare_scan_retry,
    record_evaluation,
    record_failed_evaluation,
    select_incremental_columns,
    start_scan,
)
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnMetadata,
    ObjectType,
)


@pytest.fixture
def column():
    return ColumnMetadata("raw", "items", "detail", "VARCHAR", 1, ObjectType.TABLE)


@pytest.mark.parametrize("failed", [False, True])
def test_observation_update_requires_a_pending_inventory_row(column, failed):
    def record(con, scan_id, target):
        if failed:
            record_failed_evaluation(con, scan_id, target, "RuntimeError")
        else:
            record_evaluation(con, scan_id, ColumnDetectionResult(target, (), ()), ())

    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run", [column])
        other_scan_id = start_scan(con, "other", [column])
        with pytest.raises(RuntimeError, match="Expected one pending observation"):
            record(con, scan_id, replace(column, schema="other"))

        record(con, scan_id, column)
        with pytest.raises(RuntimeError, match="Expected one pending observation"):
            record(con, scan_id, column)

        assert con.execute(
            "SELECT outcome FROM scan_state.column_observations WHERE scan_id = ?",
            [other_scan_id],
        ).fetchone() == ("pending",)


@pytest.mark.parametrize("failed", [False, True])
def test_finish_scan_rejects_incomplete_inventory(column, failed):
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run", [column])
        if failed:
            record_failed_evaluation(con, scan_id, column, "RuntimeError")

        with pytest.raises(RuntimeError, match="Expected one running scan"):
            finish_scan(con, scan_id)

        assert con.execute(
            "SELECT status, finished_at FROM scan_state.scan_runs WHERE scan_id = ?",
            [scan_id],
        ).fetchone() == ("running", None)


def test_finish_scan_requires_a_running_scan():
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run", [])
        with pytest.raises(RuntimeError, match="Expected one running scan"):
            finish_scan(con, str(uuid4()), error_type="RuntimeError")

        finish_scan(con, scan_id)
        with pytest.raises(RuntimeError, match="Expected one running scan"):
            finish_scan(con, scan_id, error_type="RuntimeError")

        assert con.execute(
            "SELECT status, error_type FROM scan_state.scan_runs WHERE scan_id = ?",
            [scan_id],
        ).fetchone() == ("completed", None)


def test_start_scan_adds_metrics_to_existing_observations(column):
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "earlier", [column])
        record_failed_evaluation(con, scan_id, column, "RuntimeError")
        finish_scan(con, scan_id, error_type="RuntimeError")
        con.execute("ALTER TABLE scan_state.column_observations DROP COLUMN metrics")

        start_scan(con, "later", [])

        assert con.execute(
            "SELECT outcome, metrics FROM scan_state.column_observations WHERE scan_id = ?",
            [scan_id],
        ).fetchone() == ("failed", None)


def test_start_scan_resumes_failed_scan_without_losing_observations(tmp_path):
    database_path = tmp_path / "test.duckdb"
    dagster_run_id = "test-dagster-run"

    columns = [
        ColumnMetadata(
            schema="raw",
            table="items",
            column="contact",
            data_type="VARCHAR",
            ordinal_position=1,
            object_type="BASE TABLE",
        ),
        ColumnMetadata(
            schema="raw",
            table="items",
            column="detail",
            data_type="VARCHAR",
            ordinal_position=2,
            object_type="BASE TABLE",
        ),
    ]

    with duckdb.connect(str(database_path)) as con:
        scan_id = start_scan(con, dagster_run_id, columns)

        # Simulate one successful evaluation and one failed evaluation.
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET outcome = 'no_match',
                evaluated_at = current_timestamp
            WHERE scan_id = ? AND column_name = 'contact'
            """,
            [scan_id],
        )

        record_failed_evaluation(
            con,
            scan_id,
            columns[1],
            "RuntimeError",
        )

        finish_scan(con, scan_id, error_type="RuntimeError")

        # Simulate Dagster retrying the asset within the same run.
        resumed_scan_id = start_scan(con, dagster_run_id, columns)

        assert resumed_scan_id == scan_id

        scan = con.execute(
            """
            SELECT status, finished_at, error_type
            FROM scan_state.scan_runs
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchone()

        assert scan == ("running", None, None)

        observations = con.execute(
            """
            SELECT column_name, outcome, error_type
            FROM scan_state.column_observations
            WHERE scan_id = ?
            ORDER BY ordinal_position
            """,
            [scan_id],
        ).fetchall()

        assert observations == [
            ("contact", "no_match", None),
            ("detail", "failed", "RuntimeError"),
        ]

        assert con.execute("SELECT COUNT(*) FROM scan_state.scan_runs").fetchone() == (
            1,
        )

        assert con.execute(
            "SELECT COUNT(*) FROM scan_state.column_observations"
        ).fetchone() == (2,)


def test_prepare_scan_retry_preserves_completed_observations(tmp_path):
    database_path = tmp_path / "test.duckdb"

    columns = [
        ColumnMetadata(
            schema="raw",
            table="items",
            column="contact",
            data_type="VARCHAR",
            ordinal_position=1,
            object_type="BASE TABLE",
        ),
        ColumnMetadata(
            schema="raw",
            table="items",
            column="detail",
            data_type="VARCHAR",
            ordinal_position=2,
            object_type="BASE TABLE",
        ),
        ColumnMetadata(
            schema="raw",
            table="items",
            column="quantity",
            data_type="INTEGER",
            ordinal_position=3,
            object_type="BASE TABLE",
        ),
    ]

    with duckdb.connect(str(database_path)) as con:
        scan_id = start_scan(con, "test-run-id", columns)

        # Completed checkpoint.
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET outcome = 'candidate',
                evaluated_at = current_timestamp,
                evidence = '[]',
                assessments = '[]'
            WHERE scan_id = ? AND column_name = 'contact'
            """,
            [scan_id],
        )

        # Failed checkpoint.
        record_failed_evaluation(
            con,
            scan_id,
            columns[1],
            "RuntimeError",
        )

        finish_scan(con, scan_id, error_type="RuntimeError")

        # Resume the same logical scan.
        resumed_scan_id = start_scan(con, "test-run-id", columns)
        assert resumed_scan_id == scan_id

        states = prepare_scan_retry(con, scan_id)

        assert states == {
            ("raw", "items", "contact"): "candidate",
            ("raw", "items", "detail"): "pending",
            ("raw", "items", "quantity"): "pending",
        }

        observations = con.execute(
            """
            SELECT column_name, outcome, evaluated_at,
                   evidence, assessments, error_type
            FROM scan_state.column_observations
            WHERE scan_id = ?
            ORDER BY ordinal_position
            """,
            [scan_id],
        ).fetchall()

        # Successful checkpoint remains intact.
        assert observations[0][0] == "contact"
        assert observations[0][1] == "candidate"
        assert observations[0][2] is not None
        assert json.loads(observations[0][3]) == []
        assert json.loads(observations[0][4]) == []

        # Failed checkpoint is reset for reevaluation.
        assert observations[1] == (
            "detail",
            "pending",
            None,
            None,
            None,
            None,
        )

        # Unreached checkpoint remains pending.
        assert observations[2] == (
            "quantity",
            "pending",
            None,
            None,
            None,
            None,
        )


def test_skipped_columns_do_not_block_scan_completion(column):
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run", [column])

        con.execute(
            """
            UPDATE scan_state.column_observations
            SET outcome = 'skipped',
                skip_reason = 'unchanged_schema'
            WHERE scan_id = ?
            """,
            [scan_id],
        )

        finish_scan(con, scan_id)

        assert con.execute(
            """
            SELECT status
            FROM scan_state.scan_runs
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchone() == ("completed",)


def test_skipped_columns_remain_skipped_on_retry(column):
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run", [column])

        con.execute(
            """
            UPDATE scan_state.column_observations
            SET outcome = 'skipped',
                skip_reason = 'unchanged_schema'
            WHERE scan_id = ?
            """,
            [scan_id],
        )

        # A retry resumes a failed scan, not a completed scan.
        con.execute(
            """
            UPDATE scan_state.scan_runs
            SET status = 'failed'
            WHERE scan_id = ?
            """,
            [scan_id],
        )

        # Resume the failed scan through the normal lifecycle.
        resumed_scan_id = start_scan(con, "run", [column])

        assert resumed_scan_id == scan_id

        states = prepare_scan_retry(con, resumed_scan_id)

        assert states[(column.schema, column.table, column.column)] == "skipped"

        assert con.execute(
            """
            SELECT outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchone() == ("skipped", "unchanged_schema")


def test_incremental_selection_skips_unchanged_column(column):
    key = (column.schema, column.table, column.column)

    selections = select_incremental_columns(
        [column],
        {key: column},
    )

    assert len(selections) == 1
    assert selections[0].should_evaluate is False
    assert selections[0].reason == "unchanged_schema"


def test_incremental_selection_rescans_changed_type(column):
    key = (column.schema, column.table, column.column)

    selections = select_incremental_columns(
        [replace(column, data_type="INTEGER")],
        {key: column},
    )

    assert selections[0].should_evaluate is True
    assert selections[0].reason == "schema_changed"


def test_incremental_selection_rescans_changed_object_type(column):
    key = (column.schema, column.table, column.column)

    selections = select_incremental_columns(
        [replace(column, object_type=ObjectType.VIEW)],
        {key: column},
    )

    assert selections[0].should_evaluate is True
    assert selections[0].reason == "schema_changed"


def test_load_previous_evaluations_returns_successful_column(column):
    """A successful prior evaluation is available for incremental selection."""
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run-1", [column])

        record_evaluation(
            con,
            scan_id,
            ColumnDetectionResult(column, (), ()),
            (),
        )
        finish_scan(con, scan_id)

        previous = load_previous_evaluations(
            con,
            exclude_dagster_run_id="run-2",
        )

        key = (column.schema, column.table, column.column)

        assert key in previous
        assert previous[key] == column


def test_load_previous_evaluations_ignores_later_skipped_observation(column):
    """A skipped observation must not replace an earlier successful evaluation."""
    with duckdb.connect(":memory:") as con:
        first_scan = start_scan(con, "run-1", [column])

        record_evaluation(
            con,
            first_scan,
            ColumnDetectionResult(column, (), ()),
            (),
        )
        finish_scan(con, first_scan)

        # The later scan records the complete inventory but skips evaluation.
        skipped_column = replace(column, ordinal_position=2)
        second_scan = start_scan(con, "run-2", [skipped_column])

        con.execute(
            """
            UPDATE scan_state.column_observations
            SET outcome = 'skipped',
                skip_reason = 'unchanged_schema'
            WHERE scan_id = ?
            """,
            [second_scan],
        )
        finish_scan(con, second_scan)

        previous = load_previous_evaluations(
            con,
            exclude_dagster_run_id="run-3",
        )

        key = (column.schema, column.table, column.column)

        assert key in previous
        # The original evaluation metadata, not the skipped inventory.
        assert previous[key] == column


def test_load_previous_evaluations_excludes_column_after_newer_failure(column):
    """A newer failure invalidates an older successful evaluation."""
    with duckdb.connect(":memory:") as con:
        first_scan = start_scan(con, "run-1", [column])

        record_evaluation(
            con,
            first_scan,
            ColumnDetectionResult(column, (), ()),
            (),
        )
        finish_scan(con, first_scan)

        second_scan = start_scan(con, "run-2", [column])

        record_failed_evaluation(
            con,
            second_scan,
            column,
            "RuntimeError",
        )
        finish_scan(con, second_scan, error_type="RuntimeError")

        # Make the ordering deterministic.
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET evaluated_at = TIMESTAMPTZ '2026-01-01 10:00:00+00'
            WHERE scan_id = ?
            """,
            [first_scan],
        )
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET evaluated_at = TIMESTAMPTZ '2026-01-02 10:00:00+00'
            WHERE scan_id = ?
            """,
            [second_scan],
        )

        previous = load_previous_evaluations(
            con,
            exclude_dagster_run_id="run-3",
        )

        key = (column.schema, column.table, column.column)

        assert key not in previous


def test_load_previous_evaluations_returns_column_after_recovery(column):
    """A later successful evaluation restores eligibility for skipping."""
    with duckdb.connect(":memory:") as con:
        first_scan = start_scan(con, "run-1", [column])

        record_failed_evaluation(
            con,
            first_scan,
            column,
            "RuntimeError",
        )
        finish_scan(con, first_scan, error_type="RuntimeError")

        second_scan = start_scan(con, "run-2", [column])

        record_evaluation(
            con,
            second_scan,
            ColumnDetectionResult(column, (), ()),
            (),
        )
        finish_scan(con, second_scan)

        # Ensure the successful evaluation is newer than the failure.
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET evaluated_at = TIMESTAMPTZ '2026-01-01 10:00:00+00'
            WHERE scan_id = ?
            """,
            [first_scan],
        )
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET evaluated_at = TIMESTAMPTZ '2026-01-02 10:00:00+00'
            WHERE scan_id = ?
            """,
            [second_scan],
        )

        previous = load_previous_evaluations(
            con,
            exclude_dagster_run_id="run-3",
        )

        key = (column.schema, column.table, column.column)

        assert key in previous
        assert previous[key] == column


def test_full_scan_initializes_all_columns_as_pending(column):
    columns = [
        column,
        replace(column, column="email", ordinal_position=2),
        replace(column, column="phone", ordinal_position=3),
    ]

    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "full-1", columns, scan_mode="full")

        rows = con.execute(
            """
            SELECT column_name, outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = ?
            ORDER BY ordinal_position
            """,
            [scan_id],
        ).fetchall()

        assert rows == [
            ("detail", "pending", None),
            ("email", "pending", None),
            ("phone", "pending", None),
        ]

        assert con.execute(
            "SELECT scan_mode FROM scan_state.scan_runs WHERE scan_id = ?",
            [scan_id],
        ).fetchone() == ("full",)


def test_first_incremental_scan_evaluates_all_columns(column):
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "incremental-1", [column], scan_mode="incremental")

        assert con.execute(
            """
            SELECT outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchone() == ("pending", None)


def test_incremental_scan_skips_previously_evaluated_column(column):
    with duckdb.connect(":memory:") as con:
        full_scan = start_scan(con, "full-1", [column])
        record_evaluation(con, full_scan, ColumnDetectionResult(column, (), ()), ())
        finish_scan(con, full_scan)

        incremental_scan = start_scan(
            con, "incremental-1", [column], scan_mode="incremental"
        )

        assert con.execute(
            """
            SELECT outcome, skip_reason, evaluated_at
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [incremental_scan],
        ).fetchone() == ("skipped", "unchanged_schema", None)

        states = prepare_scan_retry(con, incremental_scan)
        assert states[(column.schema, column.table, column.column)] == "skipped"

        finish_scan(con, incremental_scan)

        assert con.execute(
            "SELECT status FROM scan_state.scan_runs WHERE scan_id = ?",
            [incremental_scan],
        ).fetchone() == ("completed",)


def test_incremental_scan_evaluates_new_and_changed_columns(column):
    with duckdb.connect(":memory:") as con:
        full_scan = start_scan(con, "full-1", [column])
        record_evaluation(con, full_scan, ColumnDetectionResult(column, (), ()), ())
        finish_scan(con, full_scan)

        changed = replace(column, data_type="INTEGER")
        new = replace(column, column="new_field", ordinal_position=2)

        incremental_scan = start_scan(
            con,
            "incremental-1",
            [changed, new],
            scan_mode="incremental",
        )

        rows = con.execute(
            """
            SELECT column_name, outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = ?
            ORDER BY ordinal_position
            """,
            [incremental_scan],
        ).fetchall()

        assert rows == [
            ("detail", "pending", None),
            ("new_field", "pending", None),
        ]


def test_incremental_scan_retries_previous_failure(column):
    with duckdb.connect(":memory:") as con:
        full_scan = start_scan(con, "full-1", [column])
        record_failed_evaluation(con, full_scan, column, "RuntimeError")
        finish_scan(con, full_scan, error_type="RuntimeError")

        incremental_scan = start_scan(
            con, "incremental-1", [column], scan_mode="incremental"
        )

        assert con.execute(
            """
            SELECT outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [incremental_scan],
        ).fetchone() == ("pending", None)


def test_incremental_retry_preserves_original_selection(column):
    with duckdb.connect(":memory:") as con:
        full_scan = start_scan(con, "full-1", [column])
        record_evaluation(con, full_scan, ColumnDetectionResult(column, (), ()), ())
        finish_scan(con, full_scan)

        incremental_scan = start_scan(
            con, "incremental-1", [column], scan_mode="incremental"
        )

        # Simulate failure after selection but before scan completion.
        finish_scan(con, incremental_scan, error_type="RuntimeError")

        resumed = start_scan(con, "incremental-1", [column], scan_mode="incremental")

        assert resumed == incremental_scan

        states = prepare_scan_retry(con, resumed)
        assert states[(column.schema, column.table, column.column)] == "skipped"

        assert con.execute(
            """
            SELECT outcome, skip_reason
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [resumed],
        ).fetchone() == ("skipped", "unchanged_schema")


def test_retry_rejects_scan_mode_change(column):
    with duckdb.connect(":memory:") as con:
        scan_id = start_scan(con, "run-1", [column], scan_mode="full")
        finish_scan(con, scan_id, error_type="RuntimeError")

        with pytest.raises(RuntimeError, match="Scan mode changed during retry"):
            start_scan(con, "run-1", [column], scan_mode="incremental")

        assert con.execute(
            "SELECT scan_mode, status FROM scan_state.scan_runs WHERE scan_id = ?",
            [scan_id],
        ).fetchone() == ("full", "failed")
