"""Full and incremental PII scanner Dagster jobs."""

import dagster as dg

from data_platform.orchestration.assets.pii_scanner import (
    pii_candidates_for_registration,
    pii_detection_candidates,
    pii_detection_report,
    registry_upsert,
    schema_snapshot,
)

pii_assets = [
    schema_snapshot,
    pii_detection_candidates,
    pii_candidates_for_registration,
    registry_upsert,
    pii_detection_report,
]

pii_full_scan = dg.define_asset_job(
    name="pii_full_scan",
    selection=dg.AssetSelection.assets(*pii_assets),
    config={
        "ops": {
            "pii_detection_candidates": {
                "config": {"scan_mode": "full"},
            }
        }
    },
)

pii_incremental_scan = dg.define_asset_job(
    name="pii_incremental_scan",
    selection=dg.AssetSelection.assets(*pii_assets),
    config={
        "ops": {
            "pii_detection_candidates": {
                "config": {"scan_mode": "incremental"},
            }
        }
    },
)
