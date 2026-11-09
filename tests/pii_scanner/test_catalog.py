import duckdb

from data_platform.projects.pii_scanner.catalog import discover_columns
from data_platform.projects.pii_scanner.models import ObjectType


def test_discover_columns_identifies_views():
    con = duckdb.connect(":memory:")

    try:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.customers (id INTEGER, email VARCHAR)")
        con.execute(
            "CREATE VIEW raw.customer_emails AS SELECT email FROM raw.customers"
        )

        columns = discover_columns(con)

        assert any(
            col.table == "customers" and col.object_type == ObjectType.TABLE
            for col in columns
        )

        assert any(
            col.table == "customer_emails" and col.object_type == ObjectType.VIEW
            for col in columns
        )
    finally:
        con.close()
