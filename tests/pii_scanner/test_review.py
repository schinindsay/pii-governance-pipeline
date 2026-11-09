from datetime import datetime, timezone

import duckdb
import pytest

from data_platform.projects.pii_scanner.core.registry import ensure_registry
from data_platform.projects.pii_scanner.core.review import review_finding
from scripts.review_pii import main


def seed_finding(con):
    ensure_registry(con)
    con.execute("""
        INSERT INTO governance.pii_column_registry (
            DBT_MODEL, FIELD, PII_CLASSIFICATION_STATUS, ANONYMIZATION_QUERY,
            LOOKUP_KEY, MASKING_STATUS, NOTES, DESCRIPTION
        )
        VALUES ('Customers', 'Email', 'suggested', 'curated query',
                'contact_id', 'pending', 'curated note', 'curated description')
    """)


@pytest.fixture
def registry():
    with duckdb.connect(":memory:") as con:
        seed_finding(con)
        yield con


@pytest.mark.parametrize(
    ("decision", "status", "opposite"),
    [("accept", "accepted", "reject"), ("reject", "rejected", "accept")],
)
def test_review_preserves_curated_fields_and_repeated_decision(
    registry, decision, status, opposite
):
    query = """
        SELECT PII_CLASSIFICATION_STATUS, REVIEWED_BY, REVIEWED_AT, NOTES,
               ANONYMIZATION_QUERY, LOOKUP_KEY, MASKING_STATUS, DESCRIPTION
        FROM governance.pii_column_registry
    """
    before = datetime.now(timezone.utc).replace(tzinfo=None)
    assert review_finding(
        registry,
        "customers",
        "EMAIL",
        decision=decision,
        reviewed_by="reviewer",
        notes="review rationale",
    )
    after = datetime.now(timezone.utc).replace(tzinfo=None)

    reviewed = registry.execute(query).fetchone()
    assert reviewed[:2] == (status, "reviewer")
    assert before <= reviewed[2] <= after
    assert reviewed[3:] == (
        "review rationale",
        "curated query",
        "contact_id",
        "pending",
        "curated description",
    )

    assert not review_finding(
        registry,
        "CUSTOMERS",
        "email",
        decision=decision,
        reviewed_by="another reviewer",
        notes="different notes",
    )
    with pytest.raises(ValueError, match=f"with status '{status}'"):
        review_finding(
            registry,
            "Customers",
            "Email",
            decision=opposite,
            reviewed_by="another reviewer",
        )
    assert registry.execute(query).fetchone() == reviewed


def test_review_rejects_missing_and_duplicate_identity(registry):
    with pytest.raises(ValueError, match="No registry finding"):
        review_finding(
            registry, "customers", "absent", decision="accept", reviewed_by="reviewer"
        )

    registry.execute("""
        INSERT INTO governance.pii_column_registry (DBT_MODEL, FIELD, PII_CLASSIFICATION_STATUS)
        VALUES ('CUSTOMERS', 'EMAIL', 'accepted')
    """)
    with pytest.raises(ValueError, match="Multiple registry findings"):
        review_finding(
            registry, "customers", "email", decision="reject", reviewed_by="reviewer"
        )

    assert registry.execute("""
        SELECT PII_CLASSIFICATION_STATUS FROM governance.pii_column_registry ORDER BY 1
    """).fetchall() == [("accepted",), ("suggested",)]


def test_review_command_records_decision_and_preserves_notes(
    tmp_path, monkeypatch, capsys
):
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as con:
        seed_finding(con)
    monkeypatch.setenv("DATA_PLATFORM_ENV", "local")
    monkeypatch.setenv("DATA_PLATFORM_WAREHOUSE_PATH", str(database))
    monkeypatch.setattr(
        "sys.argv",
        ["review_pii", "accept", "customers", "email", "--reviewed-by", "reviewer"],
    )

    main()

    assert capsys.readouterr().out == "customers.email: accepted\n"
    with duckdb.connect(str(database)) as con:
        assert con.execute("""
            SELECT PII_CLASSIFICATION_STATUS, REVIEWED_BY, NOTES
            FROM governance.pii_column_registry
        """).fetchone() == ("accepted", "reviewer", "curated note")
