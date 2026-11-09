"""Durable scan execution state, separate from governance decisions."""

from uuid import uuid4

import duckdb

from data_platform.projects.pii_scanner.core.incremental import (
    ColumnSelection as ColumnSelection,
)
from data_platform.projects.pii_scanner.core.incremental import (
    load_previous_evaluations as load_previous_evaluations,
)
from data_platform.projects.pii_scanner.core.incremental import (
    select_incremental_columns as select_incremental_columns,
)
from data_platform.projects.pii_scanner.core.scan_candidates import (
    load_scan_candidates as load_scan_candidates,
)
from data_platform.projects.pii_scanner.core.scan_observations import (
    get_scan_outcome_counts as get_scan_outcome_counts,
)
from data_platform.projects.pii_scanner.core.scan_observations import (
    record_evaluation as record_evaluation,
)
from data_platform.projects.pii_scanner.core.scan_observations import (
    record_failed_evaluation as record_failed_evaluation,
)
from data_platform.projects.pii_scanner.models import ColumnMetadata


def start_scan(
    con: duckdb.DuckDBPyConnection,
    dagster_run_id: str,
    columns: list[ColumnMetadata],
    *,
    scan_mode: str = "full",
) -> str:
    """Persist the inventory before any evaluations are attempted."""
    if scan_mode not in {"full", "incremental"}:
        raise ValueError(f"Unsupported scan mode: {scan_mode}")

    _ensure_scan_state_schema(con)

    con.execute("BEGIN TRANSACTION")
    try:
        existing = _find_existing_scan(con, dagster_run_id)
        if existing is None:
            scan_id = _create_new_scan(con, dagster_run_id, columns, scan_mode)
        else:
            scan_id = _resume_existing_scan(con, existing, columns, scan_mode)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return scan_id


def _ensure_scan_state_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS scan_state")
    con.execute("""
        CREATE TABLE IF NOT EXISTS scan_state.scan_runs (
            scan_id UUID PRIMARY KEY,
            dagster_run_id VARCHAR NOT NULL,
            source_catalog VARCHAR NOT NULL,
            scan_mode VARCHAR NOT NULL DEFAULT 'full'
                CHECK (scan_mode IN ('full', 'incremental')),
            status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
            started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
            finished_at TIMESTAMPTZ,
            error_type VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS scan_state.column_observations (
            scan_id UUID NOT NULL REFERENCES scan_state.scan_runs(scan_id),
            source_schema VARCHAR NOT NULL,
            relation_name VARCHAR NOT NULL,
            column_name VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            ordinal_position INTEGER NOT NULL,
            object_type VARCHAR NOT NULL,
            outcome VARCHAR NOT NULL DEFAULT 'pending' CHECK (outcome IN (
                'pending', 'candidate', 'no_match', 'insufficient_evidence', 'failed', 'skipped'
            )),
            evaluated_at TIMESTAMPTZ,
            evidence JSON,
            assessments JSON,
            error_type VARCHAR,
            skip_reason VARCHAR,
            metrics JSON,
            PRIMARY KEY (scan_id, source_schema, relation_name, column_name)
        )
    """)

    # Backfill metrics for databases created before per-column metrics were added.
    # Existing outcome CHECK constraints still require a separate schema migration.
    con.execute("""
        ALTER TABLE scan_state.column_observations ADD COLUMN IF NOT EXISTS metrics JSON
    """)

    # Existing scans predate incremental mode and default to full scans.
    # DuckDB does not support adding a NOT NULL column with a default in one step.
    con.execute("""
        ALTER TABLE scan_state.scan_runs
        ADD COLUMN IF NOT EXISTS scan_mode VARCHAR DEFAULT 'full'
    """)


def _find_existing_scan(
    con: duckdb.DuckDBPyConnection,
    dagster_run_id: str,
) -> tuple[str, str, str] | None:
    existing = con.execute(
        """
        SELECT scan_id, status, scan_mode
        FROM scan_state.scan_runs
        WHERE dagster_run_id = ?
        AND source_catalog = current_database()
        """,
        [dagster_run_id],
    ).fetchall()

    if len(existing) > 1:
        raise RuntimeError(f"Multiple scans found for Dagster run {dagster_run_id}")

    if not existing:
        return None

    scan_id, status, persisted_mode = existing[0]
    return str(scan_id), status, persisted_mode


def _resume_existing_scan(
    con: duckdb.DuckDBPyConnection,
    existing: tuple[str, str, str],
    columns: list[ColumnMetadata],
    scan_mode: str,
) -> str:
    scan_id, status, persisted_mode = existing

    if persisted_mode != scan_mode:
        raise RuntimeError(f"Scan mode changed during retry: {scan_id}")

    persisted = con.execute(
        """
        SELECT source_schema, relation_name, column_name,
            data_type, ordinal_position, object_type
        FROM scan_state.column_observations
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchall()

    expected = {
        (
            column.schema,
            column.table,
            column.column,
            column.data_type,
            column.ordinal_position,
            column.object_type,
        )
        for column in columns
    }

    if set(persisted) != expected or len(persisted) != len(columns):
        raise RuntimeError(f"Scan inventory changed during retry: {scan_id}")

    if status != "failed":
        raise RuntimeError(f"Cannot resume scan {scan_id} with status {status}")

    con.execute(
        """
        UPDATE scan_state.scan_runs
        SET status = 'running',
            finished_at = NULL,
            error_type = NULL
        WHERE scan_id = ? AND status = 'failed'
        """,
        [scan_id],
    )
    return scan_id


def _create_new_scan(
    con: duckdb.DuckDBPyConnection,
    dagster_run_id: str,
    columns: list[ColumnMetadata],
    scan_mode: str,
) -> str:
    scan_id = str(uuid4())

    if scan_mode == "incremental":
        previous = load_previous_evaluations(
            con,
            exclude_dagster_run_id=dagster_run_id,
        )
        selections = select_incremental_columns(columns, previous)
    else:
        selections = [
            ColumnSelection(
                column=column,
                should_evaluate=True,
                reason="full_scan",
            )
            for column in columns
        ]

    con.execute(
        """
        INSERT INTO scan_state.scan_runs
            (scan_id, dagster_run_id, source_catalog, scan_mode, status)
        VALUES (?, ?, current_database(), ?, 'running')
        """,
        [scan_id, dagster_run_id, scan_mode],
    )

    if columns:
        con.executemany(
            """
            INSERT INTO scan_state.column_observations (
                scan_id, source_schema, relation_name, column_name,
                data_type, ordinal_position, object_type,
                outcome, skip_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    scan_id,
                    selection.column.schema,
                    selection.column.table,
                    selection.column.column,
                    selection.column.data_type,
                    selection.column.ordinal_position,
                    selection.column.object_type,
                    "pending" if selection.should_evaluate else "skipped",
                    None if selection.should_evaluate else selection.reason,
                )
                for selection in selections
            ],
        )
    return scan_id


def prepare_scan_retry(
    con: duckdb.DuckDBPyConnection,
    scan_id: str,
) -> dict[tuple[str, str, str], str]:
    """Reset failed observations for retry, preserving evaluated and skipped work."""
    con.execute("BEGIN TRANSACTION")
    try:
        scan = con.execute(
            """
            SELECT status
            FROM scan_state.scan_runs
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchone()

        if scan is None or scan[0] != "running":
            raise RuntimeError(f"Scan {scan_id} is not running")

        # Only failed observations are reset; completed and skipped rows are checkpoints.
        con.execute(
            """
            UPDATE scan_state.column_observations
            SET outcome = 'pending',
                evaluated_at = NULL,
                evidence = NULL,
                assessments = NULL,
                error_type = NULL,
                skip_reason = NULL,
                metrics = NULL
            WHERE scan_id = ?
              AND outcome = 'failed'
            """,
            [scan_id],
        )

        rows = con.execute(
            """
            SELECT source_schema, relation_name, column_name, outcome
            FROM scan_state.column_observations
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchall()

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return {(schema, table, column): outcome for schema, table, column, outcome in rows}


def finish_scan(
    con: duckdb.DuckDBPyConnection, scan_id: str, *, error_type: str | None = None
) -> None:
    """Finish the detection pass independently of registry and report execution."""
    # Skipped observations are intentionally excluded and do not block completion.
    # DuckDB rejects RETURNING when observations reference the updated scan row.
    updated = con.execute(
        """
        UPDATE scan_state.scan_runs
        SET status = ?, finished_at = current_timestamp, error_type = ?
        WHERE scan_id = ? AND status = 'running'
          AND (? OR NOT EXISTS (
              SELECT 1 FROM scan_state.column_observations
              WHERE column_observations.scan_id = scan_runs.scan_id
                AND outcome IN ('pending', 'failed')
          ))
        """,
        [
            "completed" if error_type is None else "failed",
            error_type,
            scan_id,
            error_type is not None,
        ],
    ).fetchone()
    if updated != (1,):
        raise RuntimeError(
            f"Expected one running scan eligible for this transition: {scan_id}"
        )
