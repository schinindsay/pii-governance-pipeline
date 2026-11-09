"""Warehouse sampling and missing-value profiling utilities."""

from dataclasses import dataclass

import duckdb

from data_platform.projects.pii_scanner.models import ColumnEvaluationMetrics


@dataclass(frozen=True)
class SampleProfile:
    sample_size: int
    non_missing_count: int


def quote_identifier(identifier: str) -> str:
    """Safely quote a SQL identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def is_missing(value: object) -> bool:
    """Treat NULL, empty strings, and whitespace-only strings as missing."""
    return value is None or (isinstance(value, str) and not value.strip())


def sample_column(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    column: str,
    limit: int = 1000,
    *,
    metrics: ColumnEvaluationMetrics | None = None,
) -> list[object]:
    """Retrieve a bounded sample, retaining missing values."""
    if limit <= 0:
        raise ValueError("Sample limit must be positive")

    query = f"""
        SELECT {quote_identifier(column)}
        FROM {quote_identifier(schema)}.{quote_identifier(table)}
        LIMIT ?
    """

    if metrics is not None:
        metrics.sampling_queries += 1
    values = [row[0] for row in con.execute(query, [limit]).fetchall()]
    if metrics is not None:
        metrics.sampled_values += len(values)
        metrics.non_null_sampled_values += sum(value is not None for value in values)
    return values


def profile_sample(values: list[object]) -> SampleProfile:
    """Count sampled values and values considered non-missing."""
    return SampleProfile(
        sample_size=len(values),
        non_missing_count=sum(not is_missing(value) for value in values),
    )
