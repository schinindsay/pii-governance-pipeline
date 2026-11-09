"""Discover warehouse column metadata using DuckDB's information schema."""

import duckdb

from data_platform.projects.pii_scanner.models import (
    ColumnMetadata,
    ObjectType,
)


def discover_columns(
    con: duckdb.DuckDBPyConnection,
    schema: str = "raw",
) -> list[ColumnMetadata]:
    """Return column metadata for tables and views in the specified schema."""

    query = """
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.ordinal_position,
            t.table_type
        FROM information_schema.columns AS c
        JOIN information_schema.tables AS t
            ON c.table_catalog = t.table_catalog
            AND c.table_schema = t.table_schema
            AND c.table_name = t.table_name
        WHERE c.table_schema = ?
        ORDER BY c.table_name, c.ordinal_position
    """

    rows = con.execute(query, [schema]).fetchall()

    return [
        ColumnMetadata(
            schema=row[0],
            table=row[1],
            column=row[2],
            data_type=row[3],
            ordinal_position=row[4],
            object_type=ObjectType(row[5]),
        )
        for row in rows
    ]
