from data_platform.projects.pii_scanner.core.detection import (
    evaluate_sample,
)
from data_platform.projects.pii_scanner.detectors import is_email


def test_evaluate_sample_with_mixed_values():
    evidence = evaluate_sample(
        [
            "a@example.com",
            "invalid",
            None,
            "",
            "  ",
            "b@example.com",
        ],
        pii_type="email",
        validator=is_email,
    )

    assert evidence.sample_size == 6
    assert evidence.non_missing_count == 3
    assert evidence.match_count == 2
    assert evidence.coverage == 0.5
    assert evidence.match_ratio == 2 / 3


def test_sparse_email_preserves_evidence():
    values = [None] * 999 + ["a@example.com"]

    evidence = evaluate_sample(
        values,
        pii_type="email",
        validator=is_email,
    )

    assert evidence.sample_size == 1000
    assert evidence.non_missing_count == 1
    assert evidence.match_count == 1
    assert evidence.coverage == 0.001
    assert evidence.match_ratio == 1.0


def test_all_missing_values():
    evidence = evaluate_sample(
        [None, "", "  "],
        pii_type="email",
        validator=is_email,
    )

    assert evidence.sample_size == 3
    assert evidence.non_missing_count == 0
    assert evidence.match_count == 0
    assert evidence.match_ratio is None


def test_empty_sample():
    evidence = evaluate_sample(
        [],
        pii_type="email",
        validator=is_email,
    )

    assert evidence.sample_size == 0
    assert evidence.coverage is None
    assert evidence.match_ratio is None


def test_non_string_values_do_not_match():
    evidence = evaluate_sample(
        [123, False, "a@example.com"],
        pii_type="email",
        validator=is_email,
    )

    assert evidence.non_missing_count == 3
    assert evidence.match_count == 1
