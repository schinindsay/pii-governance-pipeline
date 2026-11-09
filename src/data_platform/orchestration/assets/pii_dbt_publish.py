"""Optional, explicitly configured Dagster asset for publishing reviewed dbt YAML plans."""

import json
from pathlib import Path

import dagster as dg

from data_platform.orchestration.resources.github import GitHubResource
from data_platform.projects.pii_scanner.core.github_dbt_pr import open_dbt_pr


class DBTPublishConfig(dg.Config):
    plan_json_path: str = ""
    repo_name: str = ""
    base_branch: str = "main"
    head_branch: str = "pii-masking-review"
    publish: bool = False


@dg.asset(group_name="pii_governance_review")
def pii_dbt_masking_pr(
    context: dg.AssetExecutionContext,
    config: DBTPublishConfig,
    github_resource: GitHubResource,
):
    """Read an explicitly reviewed plan. Default is no-op; publish requires opt-in."""
    if not config.plan_json_path:
        context.log.info("No dbt masking plan configured; skipping")
        return {"skipped": True, "reason": "no_plan"}
    plan = json.loads(Path(config.plan_json_path).read_text(encoding="utf-8"))
    files = plan.get("files", {})
    if not isinstance(files, dict):
        raise ValueError("Plan files must be a path-to-text mapping")
    if not config.repo_name:
        raise ValueError("repo_name required when a plan is configured")
    result = open_dbt_pr(
        github_resource.get_client() if config.publish else None,
        config.repo_name,
        files,
        title="Review proposed dbt PII masking metadata",
        body="Human-accepted findings only. Review YAML changes before merge; no policies applied by this asset.",
        head_branch=config.head_branch,
        base_branch=config.base_branch,
        dry_run=not config.publish,
    )
    context.add_output_metadata({"files": len(files), "dry_run": not config.publish})
    return result
