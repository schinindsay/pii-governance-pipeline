"""Regression tests for locating and splicing dbt column metadata."""

import pytest
import yaml

from data_platform.projects.pii_scanner.core.dbt_yaml_edit import (
    ALREADY_SET,
    APPLIED,
    CREATE_UNSUPPORTED,
    CREATED,
    MODEL_NOT_FOUND,
    add_masking_policy,
    has_masking_policy,
)

POLICY = "pii_hash_string_mask"
BLOCK = (
    "        config:\n"
    "          meta:\n"
    f"            masking_policy: {POLICY}\n"
    "            contains_pii: true\n"
)


def test_only_direct_model_column_changes_with_sources_tests_and_sibling_models():
    before = """version: 2
sources:
  - name: customers
    columns:
      - name: email  # source column
models:
  - name: orders
    columns:
      - name: email  # sibling column

  # Reviewer comment
  - name: "Customers"
    tests:
      - name: email  # test entry
    columns:
      - name: 'Email'  # target
        description: Keep my formatting
  - name: shipments
    description: "Keep these quotes"
"""
    after, status = add_masking_policy(before, "customers", "email", POLICY)
    assert status == APPLIED
    assert after == before.replace(
        "      - name: 'Email'  # target\n", "      - name: 'Email'  # target\n" + BLOCK
    )
    assert has_masking_policy(after, "CUSTOMERS", "EMAIL")
    assert not has_masking_policy(after, "orders", "email")


@pytest.mark.parametrize(
    "source",
    [
        "models:\n  - name: customers\n  - name: CUSTOMERS\n",
        "models:\n  - name: customers\n    columns:\n      - name: email\n      - name: EMAIL\n",
        "models: []\nmodels: []\n",
        "models:\n  - name: customers\n    columns: []\n    columns: []\n",
        "models: [{name: customers, columns: [{name: email}]}]\n",
        "models:\n  - {name: customers, columns: []}\n",
        "models:\n  - name: customers\n    columns:\n      - {name: email}\n",
        "shared: &cols\n  - name: email\nmodels:\n  - name: customers\n    columns: *cols\n",
        "models:\n  - name: customers\n    columns:\n      - &email\n        name: email\n      - *email\n",
        "models: [\n",
    ],
)
def test_editor_and_verifier_reject_ambiguous_or_unsupported_targets(source):
    with pytest.raises(ValueError):
        add_masking_policy(source, "customers", "email", POLICY)
    with pytest.raises(ValueError):
        has_masking_policy(source, "customers", "email")


def test_indentless_creation_stays_inside_columns():
    before = """models:
- name: customers
  columns:
  - name: id
  tests:
  - not_null
"""
    edited, status = add_masking_policy(before, "customers", "email", POLICY)
    assert status == CREATED
    model = yaml.safe_load(edited)["models"][0]
    assert {entry["name"] for entry in model["columns"]} == {"id", "email"}
    assert model["tests"] == ["not_null"]
    assert [entry["name"] for entry in model["columns"]] == ["id", "email"]
    assert edited.endswith("  tests:\n  - not_null\n")
    assert has_masking_policy(edited, "customers", "email")


@pytest.mark.parametrize(
    "columns",
    [
        "    config:\n      columns:\n        - name: email\n",
        "    columns: []\n",
        "    columns:\n",
    ],
)
def test_missing_or_empty_direct_columns_preserves_manual_status(columns):
    before = "models:\n  - name: customers\n" + columns
    assert add_masking_policy(before, "customers", "email", POLICY) == (
        before,
        CREATE_UNSUPPORTED,
    )
    assert not has_masking_policy(before, "customers", "email")


def test_policy_like_text_in_literal_description_is_not_a_policy():
    before = (
        "models:\n  - name: customers\n    columns:\n      - name: email\n"
        f"        description: |\n          masking_policy: {POLICY}\n"
    )
    assert not has_masking_policy(before, "customers", "email")
    edited, status = add_masking_policy(before, "customers", "email", POLICY)
    assert status == APPLIED
    assert edited == before.replace(
        "        description:", BLOCK + "        description:"
    )
    assert has_masking_policy(edited, "customers", "email")
    assert add_masking_policy(edited, "customers", "email", POLICY) == (
        edited,
        ALREADY_SET,
    )


def test_nonstandard_dash_spacing_uses_parsed_property_indent():
    before = "models:\n  - name: customers\n    columns:\n      -   name: email"
    edited, status = add_masking_policy(before, "customers", "email", POLICY)
    assert status == APPLIED
    assert edited.startswith(before + "\n          config:\n")
    assert has_masking_policy(edited, "customers", "email")


def test_missing_model_does_not_match_source():
    before = "sources:\n  - name: customers\n    columns:\n      - name: email\n"
    assert add_masking_policy(before, "customers", "email", POLICY) == (
        before,
        MODEL_NOT_FOUND,
    )
    assert not has_masking_policy(before, "customers", "email")


def test_creation_preserves_comment_above_first_existing_column():
    before = """models:
  - name: customers
    columns:
      # Customer ID used for joins.
      - name: id
"""
    edited, status = add_masking_policy(before, "customers", "email", POLICY)
    assert status == CREATED
    assert edited == before + "      - name: email\n" + BLOCK


def test_creation_preserves_column_comments_and_following_tests():
    before = """models:
- name: customers
  columns:
  # Customer ID used for joins.
  - name: id
    description: Customer ID
  # Display name in the customer portal.
  - name: display_name
    description: Customer name
  # Tests on the customer model.
  tests:
  - not_null
"""
    edited, status = add_masking_policy(before, "customers", "email", POLICY)
    assert status == CREATED
    assert edited == before.replace(
        "  # Tests on the customer model.\n",
        "  - name: email\n"
        "    config:\n"
        "      meta:\n"
        f"        masking_policy: {POLICY}\n"
        "        contains_pii: true\n"
        "  # Tests on the customer model.\n",
    )
    original_model = yaml.safe_load(before)["models"][0]
    edited_model = yaml.safe_load(edited)["models"][0]
    assert edited_model["columns"][:-1] == original_model["columns"]
    assert edited_model["columns"][-1]["name"] == "email"
    assert edited_model["tests"] == original_model["tests"]


@pytest.mark.parametrize("final_newline", ["\n", ""], ids=["newline", "no-newline"])
def test_creation_rejects_ambiguous_trailing_column_comments(final_newline):
    before = (
        "models:\n"
        "  - name: customers\n"
        "    columns:\n"
        "      - name: id\n"
        "      # Customer ID used for joins.\n"
        "      # Keep this identifier stable." + final_newline
    )
    with pytest.raises(ValueError, match="ambiguous trailing comments"):
        add_masking_policy(before, "customers", "email", POLICY)
