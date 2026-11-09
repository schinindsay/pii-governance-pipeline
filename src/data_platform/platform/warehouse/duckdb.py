"""Shared DuckDB connection management."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb


@contextmanager
def connect_warehouse(
    database_path: Path,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open an existing warehouse and close it after use."""
    if not database_path.is_file():
        raise FileNotFoundError(
            f"Warehouse not found: {database_path}. "
            "Seed or provision it before running the scanner."
        )

    con = duckdb.connect(str(database_path))
    try:
        yield con
    finally:
        con.close()
