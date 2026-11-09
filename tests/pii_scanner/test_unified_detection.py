import duckdb

from data_platform.projects.pii_scanner.core.detection import detect_column
from data_platform.projects.pii_scanner.models import (
    ColumnMetadata,
    ObjectType,
)


def test_detect_column_with_name_and_value_matches():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.customers (email_address VARCHAR)")
        con.execute(
            """
            INSERT INTO raw.customers VALUES
                ('a@example.com'),
                ('not-an-email'),
                (NULL)
            """
        )

        column = ColumnMetadata(
            schema="raw",
            table="customers",
            column="email_address",
            data_type="VARCHAR",
            ordinal_position=1,
            object_type=ObjectType.TABLE,
        )

        result = detect_column(con, column)

        evidence_by_type = {evidence.pii_type: evidence for evidence in result.evidence}

        assert set(evidence_by_type) == {"email", "phone"}

        email = evidence_by_type["email"]
        assert email.sample_size == 3
        assert email.non_missing_count == 2
        assert email.match_count == 1

        phone = evidence_by_type["phone"]
        assert phone.sample_size == 3
        assert phone.non_missing_count == 2
        assert phone.match_count == 0

    finally:
        con.close()


def test_detect_column_without_name_match():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.customers (notes VARCHAR)")
        con.execute("INSERT INTO raw.customers VALUES ('a@example.com'), (NULL)")

        column = ColumnMetadata(
            schema="raw",
            table="customers",
            column="notes",
            data_type="VARCHAR",
            ordinal_position=1,
            object_type=ObjectType.TABLE,
        )

        result = detect_column(con, column)

        assert result.name_candidates == ()
        email = next(
            evidence for evidence in result.evidence if evidence.pii_type == "email"
        )

        assert email.match_count == 1
        assert email.match_ratio == 1.0
    finally:
        con.close()
