import duckdb
import pytest

from data_platform.projects.pii_scanner.sampling import (
    is_missing,
    profile_sample,
    quote_identifier,
    sample_column,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, True),
        ("", True),
        ("   ", True),
        ("\t\n", True),
        ("a@example.com", False),
        (0, False),
        (False, False),
    ],
)
def test_is_missing(value, expected):
    assert is_missing(value) is expected


def test_profile_sample():
    profile = profile_sample([None, "", "  ", "a@example.com", "b@example.com"])

    assert profile.sample_size == 5
    assert profile.non_missing_count == 2


def test_quote_identifier():
    assert quote_identifier('some"column') == '"some""column"'


def test_sample_column_retains_missing_values():
    con = duckdb.connect(":memory:")

    try:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.customers (email VARCHAR)")
        con.executemany(
            "INSERT INTO raw.customers VALUES (?)",
            [(None,), ("",), ("a@example.com",)],
        )

        values = sample_column(
            con,
            schema="raw",
            table="customers",
            column="email",
        )

        assert len(values) == 3
        assert None in values
        assert "" in values
        assert "a@example.com" in values
    finally:
        con.close()


def test_sample_column_rejects_invalid_limit():
    con = duckdb.connect(":memory:")

    try:
        with pytest.raises(ValueError):
            sample_column(con, "raw", "customers", "email", limit=0)
    finally:
        con.close()
