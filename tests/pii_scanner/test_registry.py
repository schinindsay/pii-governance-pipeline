"""Tests for PII registry creation and schema migration."""

import duckdb

from data_platform.projects.pii_scanner.core.registry import (
    REGISTRY_COLUMNS,
    RegistrationCandidate,
    compare_candidates,
    consolidate_candidates,
    create_registry_sql,
    ensure_columns_sql,
    ensure_registry,
    get_existing_columns,
    registry_table_name,
    upsert_candidates,
)
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnMetadata,
    DetectionAssessment,
    DetectionStatus,
    ObjectType,
)


def make_candidate(
    table="customers",
    field="email_address",
    pii_type="email",
    object_type=ObjectType.TABLE,
):
    column = ColumnMetadata(
        schema="raw",
        table=table,
        column=field,
        data_type="VARCHAR",
        ordinal_position=1,
        object_type=object_type,
    )
    return RegistrationCandidate(
        result=ColumnDetectionResult(
            column=column,
            name_candidates=(),
            evidence=(),
        ),
        assessment=DetectionAssessment(
            pii_type=pii_type,
            status=DetectionStatus.CANDIDATE,
            reasons=("value_match",),
        ),
    )


def test_upsert_inserts_new_candidate():
    con = duckdb.connect(":memory:")
    try:
        candidate = make_candidate()

        assert upsert_candidates(con, [candidate]) == 1

        row = con.execute(
            """
            SELECT DBT_MODEL, FIELD, PII_TYPE,
                   PII_CLASSIFICATION_STATUS,
                   MASKING_STATUS, DATA_TYPE
            FROM governance.pii_column_registry
            """
        ).fetchone()

        assert row == (
            "customers",
            "email_address",
            "email",
            "suggested",
            "DDL policy pending",
            "VARCHAR",
        )
    finally:
        con.close()


def test_upsert_is_idempotent():
    con = duckdb.connect(":memory:")
    try:
        candidate = make_candidate()

        assert upsert_candidates(con, [candidate]) == 1
        assert upsert_candidates(con, [candidate]) == 0

        count = con.execute(
            "SELECT count(*) FROM governance.pii_column_registry"
        ).fetchone()[0]

        assert count == 1
    finally:
        con.close()


def test_upsert_does_not_duplicate_multiple_pii_types():
    con = duckdb.connect(":memory:")
    try:
        candidates = [
            make_candidate(pii_type="email"),
            make_candidate(pii_type="phone"),
        ]

        assert upsert_candidates(con, candidates) == 1
    finally:
        con.close()


def test_rejected_candidate_is_reported_but_not_reinserted():
    con = duckdb.connect(":memory:")
    try:
        candidate = make_candidate()
        upsert_candidates(con, [candidate])

        con.execute(
            """
            UPDATE governance.pii_column_registry
            SET PII_CLASSIFICATION_STATUS = 'rejected',
                REVIEWED_BY = 'reviewer@example.com',
                NOTES = 'Known false positive'
            """
        )

        comparison = compare_candidates(con, [candidate])

        assert len(comparison.net_new) == 0
        assert len(comparison.previously_rejected) == 1
        assert upsert_candidates(con, [candidate]) == 0

        row = con.execute(
            """
            SELECT PII_CLASSIFICATION_STATUS, REVIEWED_BY, NOTES
            FROM governance.pii_column_registry
            """
        ).fetchone()

        assert row == (
            "rejected",
            "reviewer@example.com",
            "Known false positive",
        )
    finally:
        con.close()


def test_existing_curated_query_is_preserved():
    con = duckdb.connect(":memory:")
    try:
        candidate = make_candidate()
        upsert_candidates(con, [candidate])

        con.execute(
            """
            UPDATE governance.pii_column_registry
            SET PII_CLASSIFICATION_STATUS = 'accepted',
                ANONYMIZATION_QUERY = 'UPDATE customers SET email = NULL',
                LOOKUP_KEY = 'customer_id'
            """
        )

        assert upsert_candidates(con, [candidate]) == 0

        row = con.execute(
            """
            SELECT PII_CLASSIFICATION_STATUS,
                   ANONYMIZATION_QUERY, LOOKUP_KEY
            FROM governance.pii_column_registry
            """
        ).fetchone()

        assert row == (
            "accepted",
            "UPDATE customers SET email = NULL",
            "customer_id",
        )
    finally:
        con.close()


def test_view_gets_view_masking_status():
    con = duckdb.connect(":memory:")
    try:
        upsert_candidates(
            con,
            [make_candidate(object_type=ObjectType.VIEW)],
        )

        status = con.execute(
            """
            SELECT MASKING_STATUS
            FROM governance.pii_column_registry
            """
        ).fetchone()[0]

        assert status == "mask_pii pending"
    finally:
        con.close()


def test_registry_schema_contains_original_fields_and_data_type():
    """Preserve the original registry contract plus DATA_TYPE."""
    names = [name for name, _ in REGISTRY_COLUMNS]

    assert len(names) == 22
    assert len(names) == len(set(names))

    assert names[-1] == "DATA_TYPE"
    assert "ANONYMIZATION_QUERY" in names
    assert "LOOKUP_KEY" in names
    assert "PII_CLASSIFICATION_STATUS" in names
    assert "MASKING_STATUS" in names
    assert "REVIEWED_BY" in names
    assert "PR_NUMBER" in names


def test_create_registry_sql_contains_all_columns():
    sql = create_registry_sql("governance.pii_column_registry")

    assert "CREATE TABLE IF NOT EXISTS" in sql

    for name, data_type in REGISTRY_COLUMNS:
        assert f"{name} {data_type}" in sql


def test_ensure_columns_sql_only_adds_missing_columns():
    existing = [
        name for name, _ in REGISTRY_COLUMNS if name not in {"DATA_TYPE", "PR_URL"}
    ]

    statements = ensure_columns_sql(
        "governance.pii_column_registry",
        existing,
    )

    assert len(statements) == 2
    assert any("ADD COLUMN IF NOT EXISTS DATA_TYPE" in s for s in statements)
    assert any("ADD COLUMN IF NOT EXISTS PR_URL" in s for s in statements)


def test_ensure_columns_sql_is_case_insensitive():
    existing = [name.lower() for name, _ in REGISTRY_COLUMNS]

    statements = ensure_columns_sql(
        "governance.pii_column_registry",
        existing,
    )

    assert statements == []


def test_ensure_registry_creates_canonical_table():
    con = duckdb.connect(":memory:")

    try:
        ensure_registry(con)

        existing = get_existing_columns(con)
        expected = [name for name, _ in REGISTRY_COLUMNS]

        assert existing == expected
    finally:
        con.close()


def test_ensure_registry_is_idempotent():
    con = duckdb.connect(":memory:")

    try:
        ensure_registry(con)
        ensure_registry(con)

        assert len(get_existing_columns(con)) == 22
    finally:
        con.close()


def test_ensure_registry_migrates_without_losing_curated_data():
    con = duckdb.connect(":memory:")

    try:
        con.execute("CREATE SCHEMA governance")
        con.execute(
            """
            CREATE TABLE governance.pii_column_registry (
                DBT_MODEL VARCHAR,
                FIELD VARCHAR,
                PII_CLASSIFICATION_STATUS VARCHAR,
                ANONYMIZATION_QUERY VARCHAR,
                LOOKUP_KEY VARCHAR
            )
            """
        )

        con.execute(
            """
            INSERT INTO governance.pii_column_registry
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                "stg_customers",
                "email_address",
                "accepted",
                "UPDATE customers SET email_address = NULL WHERE id = ?",
                "id",
            ],
        )

        ensure_registry(con)

        assert len(get_existing_columns(con)) == 22

        row = con.execute(
            """
            SELECT
                DBT_MODEL,
                FIELD,
                PII_CLASSIFICATION_STATUS,
                ANONYMIZATION_QUERY,
                LOOKUP_KEY
            FROM governance.pii_column_registry
            """
        ).fetchone()

        assert row == (
            "stg_customers",
            "email_address",
            "accepted",
            "UPDATE customers SET email_address = NULL WHERE id = ?",
            "id",
        )
    finally:
        con.close()


def test_registry_table_name_quotes_identifiers():
    assert registry_table_name() == ('"governance"."pii_column_registry"')

    assert registry_table_name('my"schema', "registry") == ('"my""schema"."registry"')


def test_multiple_pii_types_become_mixed():
    candidates = [
        make_candidate(field="contact_notes", pii_type="email"),
        make_candidate(field="contact_notes", pii_type="phone"),
    ]

    result = consolidate_candidates(candidates)

    assert len(result) == 1
    assert result[0].assessment.pii_type == "mixed"
    assert result[0].assessment.reasons == ("value_match",)


def test_duplicate_pii_type_does_not_become_mixed():
    candidates = [
        make_candidate(pii_type="email"),
        make_candidate(pii_type="email"),
    ]

    result = consolidate_candidates(candidates)

    assert len(result) == 1
    assert result[0].assessment.pii_type == "email"


def test_mixed_classification_is_order_independent():
    candidates = [
        make_candidate(pii_type="email"),
        make_candidate(pii_type="phone"),
    ]

    forward = consolidate_candidates(candidates)
    reverse = consolidate_candidates(list(reversed(candidates)))

    assert forward[0].assessment == reverse[0].assessment
    assert forward[0].assessment.pii_type == "mixed"


def test_mixed_candidate_is_registered_once():
    con = duckdb.connect(":memory:")
    try:
        candidates = [
            make_candidate(field="contact_notes", pii_type="email"),
            make_candidate(field="contact_notes", pii_type="phone"),
        ]

        comparison = compare_candidates(con, candidates)
        assert len(comparison.net_new) == 1
        assert comparison.net_new[0].assessment.pii_type == "mixed"

        assert upsert_candidates(con, candidates) == 1
        assert upsert_candidates(con, list(reversed(candidates))) == 0

        rows = con.execute(
            """
            SELECT PII_TYPE, PII_CLASSIFICATION_STATUS,
                   DETECTION_METHOD
            FROM governance.pii_column_registry
            """
        ).fetchall()

        assert rows == [("mixed", "suggested", "value")]
    finally:
        con.close()


def test_mixed_detection_preserves_human_review():
    con = duckdb.connect(":memory:")
    try:
        email = make_candidate(pii_type="email")
        phone = make_candidate(pii_type="phone")

        assert upsert_candidates(con, [email]) == 1

        con.execute(
            """
            UPDATE governance.pii_column_registry
            SET PII_CLASSIFICATION_STATUS = 'accepted',
                REVIEWED_BY = 'reviewer',
                ANONYMIZATION_QUERY = 'curated query',
                LOOKUP_KEY = 'customer_id'
            """
        )

        comparison = compare_candidates(con, [email, phone])
        assert comparison.already_accepted == 1
        assert len(comparison.net_new) == 0

        assert upsert_candidates(con, [email, phone]) == 0

        row = con.execute(
            """
            SELECT PII_TYPE, PII_CLASSIFICATION_STATUS,
                   REVIEWED_BY, ANONYMIZATION_QUERY, LOOKUP_KEY
            FROM governance.pii_column_registry
            """
        ).fetchone()

        assert row == (
            "email",
            "accepted",
            "reviewer",
            "curated query",
            "customer_id",
        )
    finally:
        con.close()


def test_mixed_detection_combines_detection_methods():
    con = duckdb.connect(":memory:")
    try:
        email = make_candidate(pii_type="email")
        phone = make_candidate(pii_type="phone")

        email = RegistrationCandidate(
            result=email.result,
            assessment=DetectionAssessment(
                pii_type="email",
                status=DetectionStatus.CANDIDATE,
                reasons=("column_name_match",),
            ),
        )

        assert upsert_candidates(con, [email, phone]) == 1

        row = con.execute(
            """
            SELECT PII_TYPE, DETECTION_METHOD
            FROM governance.pii_column_registry
            """
        ).fetchone()

        assert row == ("mixed", "column_name,value")
    finally:
        con.close()
