import duckdb

from scripts.seed_database import seed_database


def test_seed_database_is_idempotent(tmp_path, monkeypatch):
    # sets up an isolated db by monkeypatchin the DB_PATH
    import scripts.seed_database as seed_module

    test_db_path = tmp_path / "test_warehouse.duckdb"

    monkeypatch.setattr(seed_module, "DB_PATH", test_db_path)

    # run seed_db function twice
    seed_database()
    seed_database()

    # ensure db only adds 10 rows per table
    con = duckdb.connect(str(test_db_path))

    try:
        customer_count = con.execute("SELECT COUNT(*) FROM raw.customers").fetchone()[0]

        order_count = con.execute("SELECT COUNT(*) FROM raw.orders").fetchone()[0]

        assert customer_count == 10
        assert order_count == 10

    finally:
        con.close()
