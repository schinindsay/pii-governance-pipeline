"""Render sanitized PII review reports for the GitHub/dbt publisher.

GitHub writes live in github_tree_commit.py and github_dbt_pr.py,
reusing the architecture of the original production implementation.
"""


def render_review_markdown(report: dict) -> str:
    """Render aggregate review information; never include sampled PII values."""
    pending = report.get("pending_review", [])
    lines = [
        "# PII review candidates",
        "",
        "Generated from the local scanner. These are suggestions, not approvals.",
        "",
        f"Pending review: **{len(pending)}**",
        "",
        "| Model | Field | Suggested type | Evidence types |",
        "| --- | --- | --- | --- |",
    ]
    for item in sorted(pending, key=lambda x: (x["model"], x["field"])):

        def safe(value):
            return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")

        lines.append(
            "| "
            + " | ".join(
                [
                    safe(item["model"]),
                    safe(item["field"]),
                    safe(item["pii_type"]),
                    safe(", ".join(item.get("detected_types", []))),
                ]
            )
            + " |"
        )
    lines.extend(
        ["", "No masking or governance decisions are applied by this report.", ""]
    )
    return "\n".join(lines)
