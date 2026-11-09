"""Contract tests for the production-inspired GitHub/dbt publishing path."""

from types import MappingProxyType, SimpleNamespace

import pytest

from data_platform.projects.pii_scanner.core.dbt_masking_plan import build_masking_plan
from data_platform.projects.pii_scanner.core.github_dbt_pr import open_dbt_pr
from data_platform.projects.pii_scanner.core.github_tree_commit import (
    commit_files_to_branch,
)


class FakeRepo:
    full_name = "org/repo"

    def __init__(self, existing=False, same=False):
        self.existing = existing
        self.same = same
        self.calls = []
        self.ref = SimpleNamespace(object=SimpleNamespace(sha="head"), edit=self.edit)

    def edit(self, sha, force=False):
        self.calls.append(("edit", sha, force))

    def get_branch(self, name):
        return SimpleNamespace(commit=SimpleNamespace(sha="base"))

    def get_git_ref(self, path):
        from github.GithubException import GithubException

        if not self.existing:
            raise GithubException(404, "missing")
        return self.ref

    def get_git_commit(self, sha):
        return SimpleNamespace(tree=SimpleNamespace(sha="tree"))

    def get_git_tree(self, sha, recursive=False):
        from data_platform.projects.pii_scanner.core.github_tree_commit import (
            git_blob_sha,
        )

        return SimpleNamespace(
            tree=[
                SimpleNamespace(
                    path="models/a.yml", sha=git_blob_sha("same"), type="blob"
                )
            ]
            if self.same
            else [],
            raw_data={"truncated": False},
            sha="tree",
        )

    def create_git_tree(self, entries, tree):
        self.calls.append(("tree", len(entries)))
        return SimpleNamespace(sha="newtree")

    def create_git_commit(self, message, tree, parents):
        self.calls.append(("commit", message))
        return SimpleNamespace(sha="newcommit")

    def create_git_ref(self, ref, sha):
        self.calls.append(("ref", ref, sha))

    def get_pulls(self, **kwargs):
        if self.existing:
            return [
                SimpleNamespace(
                    base=SimpleNamespace(ref="main"),
                    html_url="https://example.test/pr/1",
                    number=1,
                )
            ]
        return []

    def create_pull(self, **kwargs):
        self.calls.append(("pr", kwargs))
        return SimpleNamespace(html_url="https://example.test/pr/2", number=2)


class FakeClient:
    def __init__(self, repo):
        self.repo = repo

    def get_repo(self, name):
        return self.repo


def publish(repo, files, dry_run=False):
    return open_dbt_pr(
        FakeClient(repo),
        "org/repo",
        files,
        title="Review dbt masking",
        body="Review before merge",
        head_branch="pii-review",
        dry_run=dry_run,
    )


def test_offline_dry_run_does_not_call_client():
    assert open_dbt_pr(
        None, "org/repo", {"a.yml": "x"}, title="x", body="x", head_branch="x"
    )["dry_run"]


def test_new_branch_single_commit_for_multiple_files():
    repo = FakeRepo()
    result = publish(repo, {"models/a.yml": "x", "models/b.yml": "y"})
    assert result["changed"] == 2
    assert len([x for x in repo.calls if x[0] == "commit"]) == 1
    assert len([x for x in repo.calls if x[0] == "pr"]) == 1


def test_existing_pr_gets_updated_without_force_reset():
    repo = FakeRepo(existing=True)
    result = publish(repo, {"models/a.yml": "new"})
    assert result["changed"] == 1
    assert result["pr_number"] == 1
    assert ("edit", "newcommit", False) in repo.calls
    assert not any(x[0] == "pr" for x in repo.calls)


def test_unchanged_content_does_not_write():
    repo = FakeRepo(same=True)
    result = publish(repo, {"models/a.yml": "same"})
    assert result["changed"] == 0
    assert repo.calls == []


def test_dbt_plan_edits_accepted_physical_only_and_preserves_other_yaml():
    original = "version: 2\nmodels:\n  - name: customers\n    columns:\n      - name: email\n        description: contact\n      - name: id\n"
    plan = build_masking_plan(
        [
            {"model": "customers", "field": "email", "materialization": "table"},
            {"model": "customers", "field": "id", "materialization": "view"},
        ],
        [
            {
                "model": "customers",
                "patch_path": "models/schema.yml",
                "materialization": "table",
            }
        ],
        lambda _: original,
    )
    assert "masking_policy:" in plan["files"]["models/schema.yml"]
    assert "description: contact" in plan["files"]["models/schema.yml"]
    assert len(plan["applied"]) == 1


def test_dbt_plan_flags_ambiguous_config():
    original = "version: 2\nmodels:\n  - name: customers\n    columns:\n      - name: email\n        config:\n          meta:\n            owner: team\n"
    plan = build_masking_plan(
        [{"model": "customers", "field": "email", "materialization": "table"}],
        [
            {
                "model": "customers",
                "patch_path": "models/schema.yml",
                "materialization": "table",
            }
        ],
        lambda _: original,
    )
    assert plan["files"] == {}
    assert plan["flagged"][0]["reason"] == "existing_config_manual_merge"


def test_batched_tree_entries_limit_and_one_commit(monkeypatch):
    from data_platform.projects.pii_scanner.core import github_tree_commit as writer

    monkeypatch.setattr(writer, "_MAX_TREE_BATCH_ENTRIES", 2)
    repo = FakeRepo()
    result = publish(repo, {f"models/{i}.yml": f"content {i}" for i in range(5)})
    assert result["changed"] == 5
    assert result["batches"] == 3
    assert len([x for x in repo.calls if x[0] == "tree"]) == 3
    assert len([x for x in repo.calls if x[0] == "commit"]) == 1


def test_oversized_file_fails_before_write(monkeypatch):
    from data_platform.projects.pii_scanner.core import github_tree_commit as writer

    monkeypatch.setattr(writer, "_MAX_TREE_BATCH_BYTES", 10)
    repo = FakeRepo()
    with pytest.raises(ValueError, match="budget"):
        publish(repo, {"models/a.yml": "a" * 100})
    assert not repo.calls


@pytest.mark.parametrize("entry_point", ["writer", "live_pr", "dry_run"])
@pytest.mark.parametrize(
    ("files", "message"),
    [
        ([("models/a.yml", "text")], "must be a path-to-content mapping"),
        ({"/models/a.yml": "text"}, "Invalid repository-relative file path"),
        ({"C:/models/a.yml": "text"}, "Invalid repository-relative file path"),
        ({"c:models/a.yml": "text"}, "Invalid repository-relative file path"),
        ({r"models\a.yml": "text"}, "Invalid repository-relative file path"),
        ({"models//a.yml": "text"}, "Invalid repository-relative file path"),
        ({"models/": "text"}, "Invalid repository-relative file path"),
        ({"../a.yml": "text"}, "Invalid repository-relative file path"),
        ({"models/../a.yml": "text"}, "Invalid repository-relative file path"),
        ({"": "text"}, "Invalid repository-relative file path"),
        (
            {"models/a.yml": "valid", "models/./a.yml": "alias"},
            "Invalid repository-relative file path",
        ),
        ({"models/a.yml": "valid", 1: "text"}, "File paths must be strings"),
        (
            {"models/a.yml": "valid", "models/b.yml": None},
            "File content must be a string",
        ),
    ],
)
def test_invalid_files_rejected_before_repository_access(entry_point, files, message):
    # These sentinels have no repository methods. Any access fails the test.
    with pytest.raises(ValueError, match=message):
        if entry_point == "writer":
            commit_files_to_branch(
                object(), files, branch="review", commit_message="Review", logger=None
            )
        else:
            dry_run = entry_point == "dry_run"
            open_dbt_pr(
                None if dry_run else object(),
                "org/repo",
                files,
                title="Review",
                body="Review",
                head_branch="review",
                dry_run=dry_run,
            )


@pytest.mark.parametrize("dry_run", [False, True])
def test_publish_accepts_empty_file_content(dry_run):
    result = publish(FakeRepo(), MappingProxyType({"models/empty.yml": ""}), dry_run)
    assert result["files"] == ["models/empty.yml"]
    assert result["changed"] == (0 if dry_run else 1)


def test_dbt_plan_flags_ambiguous_yaml_without_publishable_file():
    plan = build_masking_plan(
        [{"model": "customers", "field": "email", "materialization": "table"}],
        [{"model": "customers", "patch_path": "models/schema.yml"}],
        lambda _: "models:\n  - name: customers\n  - name: CUSTOMERS\n",
    )

    assert plan["files"] == {}
    assert plan["applied"] == []
    assert plan["flagged"] == [
        {"model": "customers", "field": "email", "reason": "ValueError"}
    ]
