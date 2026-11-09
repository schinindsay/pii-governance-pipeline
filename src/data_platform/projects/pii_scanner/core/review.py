"""Record explicit human decisions in the existing PII registry."""

from datetime import datetime, timezone
from typing import Literal

import duckdb

from data_platform.projects.pii_scanner.core.registry import registry_table_name


def review_finding(
    con: duckdb.DuckDBPyConnection,
    model: str,
    field: str,
    *,
    decision: Literal["accept", "reject"],
    reviewed_by: str,
    notes: str | None = None,
) -> bool:
    """Review a suggested finding, returning False for a repeat decision."""
    if decision not in ("accept", "reject"):
        raise ValueError("Decision must be accept or reject")
    reviewed_by = reviewed_by.strip()
    if not model.strip() or not field.strip() or not reviewed_by:
        raise ValueError("Model, field, and reviewer identity are required")

    status = "accepted" if decision == "accept" else "rejected"
    table = registry_table_name()
    match = "lower(DBT_MODEL) = lower(?) AND lower(FIELD) = lower(?)"

    con.execute("BEGIN TRANSACTION")
    try:
        rows = con.execute(
            f"SELECT lower(PII_CLASSIFICATION_STATUS) FROM {table} WHERE {match}",
            [model, field],
        ).fetchall()
        if not rows:
            raise ValueError(f"No registry finding for {model}.{field}")
        if len(rows) != 1:
            raise ValueError(f"Multiple registry findings for {model}.{field}")

        current_status = rows[0][0]
        changed = current_status != status
        if changed:
            if current_status != "suggested":
                raise ValueError(
                    f"Cannot {decision} {model}.{field} with status {current_status!r}"
                )
            # Review edits only the decision and its audit fields. Curated query
            # metadata and masking state belong to separate workflows.
            updated = con.execute(
                f"""
                UPDATE {table}
                SET PII_CLASSIFICATION_STATUS = ?, REVIEWED_BY = ?, REVIEWED_AT = ?,
                    NOTES = COALESCE(?, NOTES)
                WHERE {match} AND lower(PII_CLASSIFICATION_STATUS) = 'suggested'
                RETURNING FIELD
                """,
                [
                    status,
                    reviewed_by,
                    datetime.now(timezone.utc).replace(tzinfo=None),
                    notes,
                    model,
                    field,
                ],
            ).fetchall()
            if len(updated) != 1:
                raise RuntimeError(
                    f"Expected one suggested finding for {model}.{field}"
                )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return changed
