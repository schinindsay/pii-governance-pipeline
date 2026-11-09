"""Publish generated dbt artifacts through a single batched Git tree commit.

Adapted from the user's production github_dbt_pr and github_tree_commit design.
"""

import logging

from .github_tree_commit import (
    commit_files_to_branch,
    create_pr,
    find_open_pr,
    validate_file_mapping,
)


def open_dbt_pr(
    client,
    repo_name,
    files,
    *,
    title,
    body,
    head_branch,
    base_branch="main",
    dry_run=True,
    draft=True,
    logger=None,
):
    validate_file_mapping(files)
    log = logger or logging.getLogger(__name__)
    result = {
        "dry_run": dry_run,
        "repo": repo_name,
        "files": sorted(files),
        "file_count": len(files),
        "changed": 0,
        "skipped": 0,
        "pr_url": None,
        "pr_number": None,
    }
    if not files or dry_run:
        return result
    repo = client.get_repo(repo_name)
    # Existing PRs must not bypass content reconciliation; later runs may have new files.
    existing = find_open_pr(repo, head_branch, base_branch)
    commit = commit_files_to_branch(
        repo,
        files,
        branch=head_branch,
        base_branch=base_branch,
        commit_message=f"[Dagster] {title}",
        logger=log,
    )
    result.update(
        changed=commit["changed"],
        skipped=commit["skipped"],
        batches=commit["batches"],
        commit_sha=commit["commit_sha"],
    )
    if existing:
        result.update(
            pr_url=existing.html_url,
            pr_number=existing.number,
            skipped_existing_pr=True,
        )
        return result
    if not commit["changed"]:
        return result
    pr = create_pr(
        repo,
        branch=head_branch,
        base_branch=base_branch,
        title=title,
        body=body,
        draft=draft,
        logger=log,
    )
    result.update(pr_url=pr.html_url, pr_number=pr.number)
    return result
