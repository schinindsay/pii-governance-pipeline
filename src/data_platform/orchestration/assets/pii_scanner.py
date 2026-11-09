"""Dagster assets for the local PII discovery and review pipeline."""

import json
from pathlib import Path
from time import perf_counter

import dagster as dg

from data_platform.orchestration.resources import DuckDBResource
from data_platform.projects.pii_scanner.catalog import discover_columns
from data_platform.projects.pii_scanner.core.detection import detect_column
from data_platform.projects.pii_scanner.core.observations import (
    finish_scan,
    get_scan_outcome_counts,
    load_scan_candidates,
    prepare_scan_retry,
    record_evaluation,
    start_scan,
)
from data_platform.projects.pii_scanner.core.registry import (
    compare_candidates,
    upsert_candidates,
)
from data_platform.projects.pii_scanner.core.reporting import build_review_report
from data_platform.projects.pii_scanner.core.scan_evaluation import (
    evaluate_pending_columns,
)


@dg.asset(group_name="pii_scanner")
def schema_snapshot(context: dg.AssetExecutionContext, warehouse: DuckDBResource):
    """Discover actual source columns, including undocumented fields."""
    with warehouse.connect() as con:
        columns = discover_columns(con, schema="raw")

    context.add_output_metadata({"column_count": len(columns)})
    return columns


@dg.asset(
    group_name="pii_scanner",
    retry_policy=dg.RetryPolicy(
        max_retries=1,
        delay=0,
    ),
    config_schema={
        "scan_mode": dg.Field(
            dg.String,
            default_value="full",
            is_required=False,
            description="Scan mode: full or incremental.",
        )
    },
)
def pii_detection_candidates(
    context: dg.AssetExecutionContext,
    schema_snapshot,
    warehouse: DuckDBResource,
):
    """Sample discovered columns and classify potential PII."""
    phase_started = perf_counter()

    with warehouse.connect() as con:
        scan_mode = context.op_execution_context.op_config["scan_mode"]

        scan_id = start_scan(
            con,
            context.run.run_id,
            schema_snapshot,
            scan_mode=scan_mode,
        )
        column_states = prepare_scan_retry(con, scan_id)
        workload, failure = evaluate_pending_columns(
            con,
            scan_id,
            schema_snapshot,
            column_states,
            detect_column=detect_column,
            record_evaluation=record_evaluation,
            clock=perf_counter,
        )
        # allow the column scans to run - collecting errors - that way existing work isn't lost on retry
        error_type = type(failure).__name__ if failure is not None else None
        finish_scan(con, scan_id, error_type=error_type)
        scan_counts = get_scan_outcome_counts(con, scan_id)
        candidate_count = workload["candidate_count"]
        if failure is None:
            candidates = load_scan_candidates(con, scan_id)
            candidate_count = len(candidates)

    metadata = {
        "scan_id": scan_id,
        "discovered_columns": len(schema_snapshot),
        "attempted_columns": workload["attempted_columns"],
        **scan_counts,
        "candidate_count": candidate_count,
        "distinct_columns": scan_counts["candidate_columns"],
        "sampling_queries": workload["sampling_queries"],
        "sampled_values": workload["sampled_values"],
        "non_null_sampled_values": workload["non_null_sampled_values"],
        "detection_duration_seconds": perf_counter() - phase_started,
        "skipped_columns": scan_counts["skipped_columns"],
    }

    if failure is not None:
        raise dg.Failure(
            description=f"PII detection failed ({error_type}).", metadata=metadata
        ) from None
    context.add_output_metadata(metadata)
    return candidates


@dg.asset(group_name="pii_scanner")
def pii_candidates_for_registration(
    context: dg.AssetExecutionContext,
    pii_detection_candidates,
    warehouse: DuckDBResource,
):
    """Compare detected candidates against persistent review decisions."""
    with warehouse.connect() as con:
        comparison = compare_candidates(con, pii_detection_candidates)

    context.add_output_metadata(
        {
            "net_new": len(comparison.net_new),
            "previously_rejected": len(comparison.previously_rejected),
            "already_accepted": comparison.already_accepted,
            "awaiting_review": comparison.awaiting_review,
        }
    )
    return comparison


@dg.asset(group_name="pii_scanner")
def registry_upsert(
    context: dg.AssetExecutionContext,
    pii_candidates_for_registration,
    warehouse: DuckDBResource,
):
    """Register new findings without overwriting curated decisions."""
    comparison = pii_candidates_for_registration

    with warehouse.connect() as con:
        inserted = upsert_candidates(con, list(comparison.net_new))

    context.add_output_metadata({"inserted": inserted})
    return inserted


@dg.asset(
    group_name="pii_scanner",
    config_schema={
        "report_json_path": dg.Field(dg.String, default_value="", is_required=False)
    },
)
def pii_detection_report(
    context: dg.AssetExecutionContext,
    pii_candidates_for_registration,
    registry_upsert,
    warehouse: DuckDBResource,
):
    """Summarize findings and current human-review workload."""
    comparison = pii_candidates_for_registration

    with warehouse.connect() as con:
        report = build_review_report(
            con,
            comparison,
            registry_upsert,
            dagster_run_id=context.run.run_id,
        )

    output_path = context.op_execution_context.op_config["report_json_path"]
    if output_path:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    context.log.info("PII review report: %s", report)
    context.add_output_metadata(
        {
            "newly_registered": registry_upsert,
            "pending_review_count": report["pending_review_count"],
            "previously_rejected": report["previously_rejected"],
        }
    )
    return report
