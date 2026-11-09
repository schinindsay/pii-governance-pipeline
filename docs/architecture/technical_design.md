# Technical design

This is a local, production-inspired PII governance sample—not a deployed masking system. It uses Dagster, DuckDB, and synthetic data; the original system used Snowflake, dbt metadata, and downstream governance workflows.

## Contents

- [Architecture and execution](#architecture-and-execution)
- [Detection and sampling](#detection-and-sampling)
- [State, retries, and incremental scans](#state-retries-and-incremental-scans)
- [Governance decisions](#governance-decisions)
- [dbt and GitHub review](#dbt-and-github-review)
- [Production adaptations and limits](#production-adaptations-and-limits)

## Architecture and execution

The five scanner assets run in order: `schema_snapshot` → `pii_detection_candidates` → `pii_candidates_for_registration` → `registry_upsert` → `pii_detection_report`. `pii_full_scan` evaluates all discovered columns; `pii_incremental_scan` selectively reevaluates them. Both use one DuckDB database: `raw` holds source data, `scan_state` holds execution history, and `governance` holds current review state. The optional GitHub/dbt publisher is **not** part of the scan job.

## Detection and sampling

DuckDB `information_schema` supplies column and table/view metadata. The scanner evaluates columns serially, with a `LIMIT` of 1,000 returned values by default, reusing each sample across name/value detectors. Evidence can indicate email, phone, multiple types, no match, or insufficient evidence. A positive signal creates a **review candidate**, not a confirmed classification; raw sampled values are not stored in findings.

**Decision:** Column-level, serial evaluation makes failures attributable and avoids cursor contention, but scales queries with schema width. `LIMIT` is not random sampling and does not bound the cost of complex views. Production used Snowflake Bernoulli sampling and name-based prioritization; cost/coverage benchmarking would precede any local redesign.

## State, retries, and incremental scans

`scan_state.scan_runs` records logical scans; `scan_state.column_observations` records per-column outcomes, metrics, and evidence. Inventory is persisted before evaluation. Completed observations survive later column failures; failed or pending work can be retried **within the same Dagster run**, with candidates reconstructed from persisted observations. Observation-write failure stops the attempt. A completed scan status means **detection** completed, not that registry/report/publishing succeeded.

Incremental mode still inventories every column, then skips structurally unchanged columns with usable prior successful evaluations. It considers physical identity, data type, and table/view type; a newer failure is not masked by an older success. Skips preserve prior evidence provenance. Data-only changes and unchanged-schema view SQL changes are not detected; full scans remain necessary.

**Decision:** Persist evidence separately from governance state and use bounded same-run retries. This is not cross-run crash recovery; scans are assumed not to overlap.

## Governance decisions

The registry is keyed logically by model/field, unlike scan observations, which identify physical columns within a scan. Registration adds findings without resetting human accepted/rejected decisions or curated masking metadata. Multiple detector types may be consolidated into a single `mixed` registry classification; detailed assessments remain in observations. The report combines current review state with scan evidence.

**Decision:** Detection suggests; people decide. A rescan must not silently reverse a review decision. The single-catalog model/field identity and application-level writes do not provide multi-catalog reconciliation or concurrent-writer safety.

## dbt and GitHub review

The optional offline planner accepts **operator-supplied, explicitly human-accepted** records and dbt model metadata; it does not read accepted rows directly from the registry or resolve a live dbt manifest. It proposes minimal YAML changes for physical dbt models. A separate opt-in Dagster asset reads an explicit plan JSON; a standalone CLI can publish a sanitized Markdown review report. Neither is an automatic downstream dependency of scanning.

The shared GitHub writer compares Git blob hashes, skips unchanged files, batches tree updates, creates one commit, and creates/reuses a review PR without force-resetting the review branch. Dry-run and actual publishing are distinct. A PR is a proposal, not approval or policy deployment. GitHub behavior is tested with fakes, not a live repository. See [GitHub/dbt workflow](github_dbt_workflow.md) for operational details.

## Production adaptations and limits

| Concern | Production context | Local scope |
| --- | --- | --- |
| Metadata | Snowflake catalog plus dbt manifest | DuckDB `information_schema` |
| Sampling | Bernoulli and column-name prioritization | Per-column bounded-result `LIMIT` |
| Orchestration | Dagster and downstream governance | Five scanner assets; optional separate publisher |
| Governance | Curated registry and approval workflow | Registry preserves human decisions; no review UI |
| Masking proposals | dbt YAML and view SQL paths | Offline physical-model YAML proposals; no view SQL edits |
| Enforcement | Deployment and reconciliation | No automatic masking, deletion, or PR-status reconciliation |

The local durable observation/checkpoint design is demonstrated here; it is **not claimed to match** the original production implementation. Remaining gaps include scan concurrency control, cross-run recovery, versioned migrations, warehouse query-cost telemetry, data-freshness invalidation, and live GitHub integration tests. These are explicit scope limits, not implemented features.
