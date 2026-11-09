"""Publish a scanner report using the production-inspired batched Git tree writer."""

import argparse
import json
import os
from pathlib import Path

from data_platform.projects.pii_scanner.core.github_dbt_pr import open_dbt_pr
from data_platform.projects.pii_scanner.core.github_review import render_review_markdown


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--base", default="main")
    parser.add_argument("--branch", default="pii-review-candidates")
    parser.add_argument("--path", default="governance/pii-review.md")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Write to GitHub; default is offline dry run",
    )
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    files = {args.path: render_review_markdown(report)}
    if args.publish:
        from github import Github

        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            parser.error("GITHUB_TOKEN is required for --publish")
        client = Github(token, timeout=30)
    else:
        client = None
    result = open_dbt_pr(
        client,
        f"{args.owner}/{args.repo}",
        files,
        title="Review potential PII findings",
        body="Generated scanner findings for human review. No masking is applied.",
        head_branch=args.branch,
        base_branch=args.base,
        dry_run=not args.publish,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
