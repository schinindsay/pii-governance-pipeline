"""Dagster definitions for the PII scanner project."""

import os

import dagster as dg

from data_platform.orchestration.assets.pii_scanner import (
    pii_candidates_for_registration,
    pii_detection_candidates,
    pii_detection_report,
    registry_upsert,
    schema_snapshot,
)
from data_platform.orchestration.jobs.pii_scanner import (
    pii_full_scan,
    pii_incremental_scan,
)
from data_platform.orchestration.resources import DuckDBResource
from data_platform.orchestration.resources.github import GitHubResource
from data_platform.platform.config import load_settings

settings = load_settings()

resources = {
    "warehouse": DuckDBResource(database_path=str(settings.warehouse_path)),
}

assets = [
    schema_snapshot,
    pii_detection_candidates,
    pii_candidates_for_registration,
    registry_upsert,
    pii_detection_report,
]

if os.getenv("GITHUB_TOKEN"):
    from data_platform.orchestration.assets.pii_dbt_publish import pii_dbt_masking_pr

    resources["github_resource"] = GitHubResource(api_token=dg.EnvVar("GITHUB_TOKEN"))
    assets.append(pii_dbt_masking_pr)

defs = dg.Definitions(
    resources=resources,
    assets=assets,
    jobs=[pii_full_scan, pii_incremental_scan],
)
