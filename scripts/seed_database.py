"""
Initialize a local DuckDB warehouse with synthetic data for PII discovery.

Design decisions:
- DuckDB replaces an external analytical warehouse to keep the project
  self-contained and reproducible.
- Source tables use explicit schemas to provide predictable data types
  for metadata discovery and classification testing.
- Raw tables are recreated on each execution to ensure deterministic
  test fixtures. Production source tables would be managed by ingestion
  pipelines and accessed read-only by the scanner.
- Governance data is treated as persistent application state and must
  not be reset when source fixtures are regenerated.
- All sample records are synthetic and include nulls, malformed values,
  and potential PII to exercise detection behavior.

This script initializes the raw source data. Governance tables are
managed separately by the scanner's persistence layer.
"""

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"
DATA_DIR = PROJECT_ROOT / "data"

SCHEMAS = ["raw", "governance"]

RAW_SOURCE_TABLES = {
    "customers": {
        "id": "VARCHAR",
        "first_name": "VARCHAR",
        "email_address": "VARCHAR",
        "phone_number": "VARCHAR",
        "birth_date": "DATE",
    },
    "orders": {
        "order_id": "INTEGER",
        "customer_id": "VARCHAR",
        "contact_email": "VARCHAR",
        "total_amount": "DECIMAL(10, 2)",
    },
}

MOCK_DATA = {
    "customers": [
        ("c001", "Alice", "alice@example.com", "919-555-0101", "1990-04-12"),
        ("c002", "Bob", "bob.smith@gmail.com", "212-555-0198", "1985-09-23"),
        ("c003", "Carol", "carol@example.org", None, "1992-06-15"),
        ("c004", "David", None, "415-555-0123", "1978-11-30"),
        ("c005", "Eve", "not-an-email", "555-0100", None),
        ("c006", "Frank", "frank@example.com", "invalid-phone", "2001-02-08"),
        ("c007", "Grace", "grace@example.net", "(919) 555-0142", "1995-07-19"),
        ("c008", "Henry", "", None, "1988-03-04"),
        ("c009", "Iris", "iris@example.com", "9195550167", "1999-12-25"),
        ("c010", "Jack", "jack@example.com", "646-555-0184", "1982-05-10"),
    ],
    "orders": [
        (1001, "c001", "alice@example.com", 49.99),
        (1002, "c002", "bob.smith@gmail.com", 129.50),
        (1003, "c003", None, 15.00),
        (1004, "c001", "alice+orders@example.com", 89.95),
        (1005, "c004", "david@example.org", 250.00),
        (1006, "c005", "invalid-email", 12.99),
        (1007, "c006", "frank@example.com", 75.25),
        (1008, "c007", "", 199.99),
        (1009, "c008", "henry@example.net", 35.50),
        (1010, "c009", "iris@example.com", 64.00),
    ],
}


def create_source_tables(con, source_tables, schema="raw"):
    for table_name, columns in source_tables.items():
        column_definitions = ", ".join(
            f"{column_name} {data_type}" for column_name, data_type in columns.items()
        )

        con.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table_name} ({column_definitions})"
        )


def load_mock_data(con, schema, table, columns, data):
    """Insert synthetic rows using parameterized SQL values."""

    column_names = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)

    sql = f"INSERT INTO {schema}.{table} ({column_names}) VALUES ({placeholders})"

    con.executemany(sql, data)


def create_schemas(con, schemas):
    for s in schemas:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")


def seed_database() -> None:
    """Create and populate synthetic source tables in the local warehouse."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    try:
        con.execute("BEGIN TRANSACTION")

        create_schemas(con, SCHEMAS)

        create_source_tables(con, source_tables=RAW_SOURCE_TABLES)

        for table_name, rows in MOCK_DATA.items():
            load_mock_data(
                con,
                "raw",
                table_name,
                RAW_SOURCE_TABLES[table_name],
                rows,
            )

        con.execute("COMMIT")

    except Exception:
        con.execute("ROLLBACK")
        raise

    finally:
        con.close()


if __name__ == "__main__":
    seed_database()
