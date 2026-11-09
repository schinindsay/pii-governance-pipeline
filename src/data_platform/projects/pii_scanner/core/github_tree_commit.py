"""Write a {path: content} file set to a branch as ONE commit, via the Git Data API.

Replaces the per-file `get_contents` + `update_file`/`create_file` loop that every
dbt-generation asset used to carry. That loop cost two API calls per file, so a
5,260-file run spent ~11,000 requests and, with the hand-rolled 1,000-calls-per-hour
guard, ~10 hours asleep. It also could not fit under GitHub's secondary limit of
500 content-generating requests/hour: 5,260 files needs 5,260 writes, a ten-hour
floor even with perfect throttling.

This module does the same work in roughly ten calls, independent of file count:

    1. read the base branch tip + its tree
    2. diff locally by computing each file's git blob SHA
    3. POST the changed entries as tree(s), chained
    4. POST one commit
    5. PATCH the branch ref

Deliberately Dagster-free: takes a PyGithub client and a files dict, so it can be
unit-tested against a mock. Callers pass `context.log` as `logger`.
"""

import hashlib
import re
from collections.abc import Mapping

from github import InputGitTreeElement
from github.GithubException import GithubException

# Regular non-executable file. The Git tree API requires an explicit mode.
_FILE_MODE = "100644"

# create_git_tree posts every entry's content inline in a single request body.
# 5,260 dbt models at ~2-12 KB each is tens of megabytes, which GitHub rejects,
# so the tree is built in chained batches. Bound by bytes rather than a flat
# entry count because model sizes vary by an order of magnitude.
_MAX_TREE_BATCH_BYTES = 4 * 1024 * 1024
_MAX_TREE_BATCH_ENTRIES = 500


def validate_file_mapping(files: Mapping[str, str]) -> None:
    """Require canonical repository paths and text content before publishing."""
    if not isinstance(files, Mapping):
        raise ValueError("Files must be a path-to-content mapping")

    for path, content in files.items():
        if not isinstance(path, str):
            raise ValueError("File paths must be strings")
        if (
            re.match(r"^[A-Za-z]:", path)
            or "\\" in path
            or "\0" in path
            or any(part in {"", ".", ".."} for part in path.split("/"))
        ):
            raise ValueError(f"Invalid repository-relative file path: {path!r}")
        if not isinstance(content, str):
            raise ValueError(f"File content must be a string for {path!r}")


def git_blob_sha(content: str) -> str:
    """Compute the git object SHA-1 for `content` exactly as git would.

    Lets us diff generated files against the repo without asking GitHub about
    each path. git hashes `blob <bytelen>\\0<bytes>`, not the bare content.
    """
    payload = content.encode("utf-8")
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload).hexdigest()


def diff_against_tree(files: dict, existing_shas: dict) -> dict:
    """Return only the entries of `files` whose content differs from the repo.

    Args:
        files: {repo_relative_path: full_file_content}
        existing_shas: {repo_relative_path: git_blob_sha} from the base tree

    Returns:
        {path: content} limited to new or genuinely modified files.
    """
    return {
        path: content
        for path, content in files.items()
        if existing_shas.get(path) != git_blob_sha(content)
    }


def read_tree_blob_shas(repo, tree_sha: str, logger) -> tuple:
    """Read every blob path -> SHA under `tree_sha` in one recursive call.

    Returns (shas, complete). `complete` is False when GitHub truncated the
    response (it caps recursive trees at 100k entries / 7MB). A truncated tree
    means the diff would report files as missing and rewrite them needlessly,
    so callers should treat it as "diffing unavailable" rather than trusting it.
    """
    tree = repo.get_git_tree(tree_sha, recursive=True)
    shas = {
        element.path: element.sha for element in tree.tree if element.type == "blob"
    }
    # PyGithub doesn't surface `truncated` as a property; it's only in the raw
    # API payload.
    complete = not tree.raw_data.get("truncated", False)
    if not complete:
        logger.warning(
            f"Base tree {tree_sha[:7]} was truncated by GitHub ({len(shas)} entries "
            "read); skipping the local diff and writing all files this run."
        )
    return shas, complete


def _batch_tree_elements(changed: dict) -> list:
    """Split changed files into request-sized batches of InputGitTreeElement."""
    batches, current, current_bytes = [], [], 0
    for path in sorted(changed):
        content = changed[path]
        size = len(content.encode("utf-8"))
        if size > _MAX_TREE_BATCH_BYTES:
            raise ValueError(f"Generated file exceeds tree batch budget: {path}")
        if current and (
            current_bytes + size > _MAX_TREE_BATCH_BYTES
            or len(current) >= _MAX_TREE_BATCH_ENTRIES
        ):
            batches.append(current)
            current, current_bytes = [], 0
        current.append(
            InputGitTreeElement(
                path=path, mode=_FILE_MODE, type="blob", content=content
            )
        )
        current_bytes += size
    if current:
        batches.append(current)
    return batches


def commit_files_to_branch(
    repo,
    files: Mapping[str, str],
    *,
    branch: str,
    base_branch: str = "main",
    commit_message: str,
    logger,
    skip_unchanged: bool = True,
) -> dict:
    """Diff generated files against current head, batch trees, make one non-force commit.

    Existing review branches are never force-reset. This preserves reviewer edits
    and allows subsequent runs to update an existing PR.
    """
    validate_file_mapping(files)
    base = repo.get_branch(base_branch)
    base_sha = base.commit.sha
    try:
        ref = repo.get_git_ref(f"heads/{branch}")
    except GithubException as exc:
        if exc.status != 404:
            raise
        ref = None
    parent_sha = ref.object.sha if ref is not None else base_sha
    parent_commit = repo.get_git_commit(parent_sha)
    parent_tree = parent_commit.tree
    changed = files
    diffed = False
    if skip_unchanged:
        shas, complete = read_tree_blob_shas(repo, parent_tree.sha, logger)
        if complete:
            changed = diff_against_tree(files, shas)
            diffed = True
    skipped = len(files) - len(changed)
    if not changed:
        return {
            "changed": 0,
            "skipped": skipped,
            "batches": 0,
            "commit_sha": None,
            "branch": branch,
            "base_sha": base_sha,
            "diffed": diffed,
        }
    batches = _batch_tree_elements(changed)
    tree = parent_tree
    for entries in batches:
        tree = repo.create_git_tree(entries, tree)
    commit = repo.create_git_commit(commit_message, tree, [parent_commit])
    if ref is None:
        # Create directly at new commit; no intermediate branch reset.
        repo.create_git_ref(ref=f"refs/heads/{branch}", sha=commit.sha)
    else:
        ref.edit(commit.sha, force=False)
    return {
        "changed": len(changed),
        "skipped": skipped,
        "batches": len(batches),
        "commit_sha": commit.sha,
        "branch": branch,
        "base_sha": base_sha,
        "diffed": diffed,
    }


def find_open_pr(repo, branch: str, base_branch: str):
    """Return the open PR for `branch` into `base_branch`, or None."""
    owner = repo.full_name.split("/")[0]
    for pull in repo.get_pulls(state="open", head=f"{owner}:{branch}"):
        if pull.base.ref == base_branch:
            return pull
    return None


def create_pr(
    repo,
    *,
    branch: str,
    base_branch: str,
    title: str,
    body: str,
    logger,
    draft: bool = False,
):
    """Open a PR, tolerating one that already exists for this branch.

    Writes retry at the transport layer, so a create_pull whose response was lost
    can come back as 422 "A pull request already exists" — that is success, not
    failure.
    """
    try:
        pull = repo.create_pull(
            title=title, body=body, head=branch, base=base_branch, draft=draft
        )
        logger.info(f"Created PR #{pull.number}: {pull.html_url}")
        return pull
    except GithubException as ex:
        if ex.status != 422:
            raise
        existing = find_open_pr(repo, branch, base_branch)
        if existing is None:
            raise
        logger.info(f"PR already exists for {branch}: {existing.html_url}")
        return existing


__all__ = [
    "commit_files_to_branch",
    "create_pr",
    "diff_against_tree",
    "find_open_pr",
    "git_blob_sha",
    "read_tree_blob_shas",
    "validate_file_mapping",
]
