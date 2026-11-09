"""Incremental column selection using prior scan observations."""

from dataclasses import dataclass

import duckdb

from data_platform.projects.pii_scanner.models import ColumnMetadata


@dataclass(frozen=True)
class ColumnSelection:
    column: ColumnMetadata
    should_evaluate: bool
    reason: str


def load_previous_evaluations(
    con: duckdb.DuckDBPyConnection,
    *,
    exclude_dagster_run_id: str,
) -> dict[tuple[str, str, str], ColumnMetadata]:
    """Return columns whose latest actual evaluation succeeded.

    Skipped observations do not replace prior evaluations.
    A newer failure invalidates an older successful evaluation.
    """
    rows = con.execute(
        """
        WITH ranked AS (
            SELECT
                o.source_schema,
                o.relation_name,
                o.column_name,
                o.data_type,
                o.ordinal_position,
                o.object_type,
                o.outcome,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        o.source_schema,
                        o.relation_name,
                        o.column_name
                    ORDER BY
                        o.evaluated_at DESC,
                        r.started_at DESC,
                        r.scan_id DESC
                ) AS rn
            FROM scan_state.column_observations AS o
            JOIN scan_state.scan_runs AS r
                ON o.scan_id = r.scan_id
            WHERE r.dagster_run_id != ?
              AND r.source_catalog = current_database()
              AND o.outcome IN (
                  'candidate',
                  'no_match',
                  'insufficient_evidence',
                  'failed'
              )
        )
        SELECT
            source_schema,
            relation_name,
            column_name,
            data_type,
            ordinal_position,
            object_type
        FROM ranked
        WHERE rn = 1
          AND outcome != 'failed'
        """,
        [exclude_dagster_run_id],
    ).fetchall()

    previous = {}

    for schema, table, column, data_type, position, object_type in rows:
        metadata = ColumnMetadata(
            schema=schema,
            table=table,
            column=column,
            data_type=data_type,
            ordinal_position=position,
            object_type=object_type,
        )
        previous[(schema, table, column)] = metadata

    return previous


def select_incremental_columns(
    columns: list[ColumnMetadata],
    previous_evaluations: dict[tuple[str, str, str], ColumnMetadata],
) -> list[ColumnSelection]:
    """Select new or structurally changed columns for evaluation."""
    selections = []

    for column in columns:
        key = (column.schema, column.table, column.column)
        previous = previous_evaluations.get(key)

        if previous is None:
            reason = "no_prior_success"
            should_evaluate = True
        elif (
            previous.data_type != column.data_type
            or previous.object_type != column.object_type
        ):
            reason = "schema_changed"
            should_evaluate = True
        else:
            reason = "unchanged_schema"
            should_evaluate = False

        selections.append(
            ColumnSelection(
                column=column,
                should_evaluate=should_evaluate,
                reason=reason,
            )
        )

    return selections
