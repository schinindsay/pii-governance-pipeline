"""Report rendering must never expose raw sampled evidence."""

from data_platform.projects.pii_scanner.core.github_review import render_review_markdown


def test_render_markdown_omits_raw_evidence():
    report = {
        "pending_review": [
            {
                "model": "customers",
                "field": "email",
                "pii_type": "email",
                "detected_types": ["email"],
                "evidence": [{"value": "secret@example.com"}],
            }
        ]
    }
    text = render_review_markdown(report)
    assert "customers" in text
    assert "secret@example.com" not in text
