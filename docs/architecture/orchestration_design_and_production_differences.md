## Agent-assisted governance (production only)

The original production workflow included agent-assisted capabilities that are intentionally omitted from this local sample.

Agents supported the governance review process alongside deterministic PII detection, registry management, and human approval.

The local implementation focuses on the deterministic components of the system: detecting potential PII, preserving evidence, maintaining review decisions, and preparing masking changes.

This separation keeps the sample reproducible without requiring an LLM provider, agent runtime, or external credentials.

Agent-generated recommendations should be treated as advisory rather than authoritative. Human review remains the decision point for accepting or rejecting PII classifications.

## Schema snapshot Asset

### Local implementation

The local `schema_snapshot` asset queries DuckDB's `information_schema` to discover tables, columns, data types, and object types in the configured scope. It passes the resulting column metadata directly to `pii_detection_candidates` rather than persisting a separate schema snapshot table.

### Production implementation

The original production implementation combines metadata from the dbt Cloud manifest and Snowflake's `INFORMATION_SCHEMA`.

- **Database-backed snapshot:** The asset writes the discovered columns to a Snowflake table. Each run truncates and repopulates that table, preserving the current snapshot for downstream assets without maintaining historical versions.
- **dbt enrichment:** The dbt manifest supplies documented columns, model metadata, and materialization types. Snowflake catalog queries supplement this with undocumented columns, actual data types, and table/view classification where needed.
- **Scoped, batched discovery:** Discovery uses selected dbt models and their downstream dependencies, excludes raw Kafka tables, and restricts queries to configured databases. Tables are grouped by database so catalog metadata can be retrieved in batches rather than issuing a query for every table.
- **Downstream consumption:** `pii_detection_candidates` reads the persisted snapshot directly from Snowflake, rather than receiving an in-memory list from the upstream asset.

### Why the local sample differs

The local implementation preserves the separation between schema discovery and PII detection while removing dependencies on Snowflake, dbt Cloud, and production-scale metadata discovery.

This makes the sample self-contained and easier to run, but it does not reproduce the production system's dbt-based discovery scope, metadata enrichment, database-backed snapshot handoff, or multi-database batching.

## PII detection candidates Assset

### Local implementation

The local `pii_detection_candidates` asset receives column metadata from `schema_snapshot` and evaluates each discovered column using name-based patterns and sampled-value validators.

Every column is sampled, regardless of whether its name matches a known PII pattern. Detection evidence is classified into `CANDIDATE`, `NO_MATCH`, or `INSUFFICIENT_EVIDENCE`. A candidate can be identified through either a column-name match or a sampled-value match; a name match does not require confirmation from sampled values.

The implementation does not assign confidence scores or apply a minimum match-rate threshold. Instead, it preserves the evidence and reasons for classification, leaving subsequent governance decisions to downstream assets.

Scan execution state is persisted in DuckDB through `scan_runs` and `column_observations`. Each column has a recorded outcome, allowing failed Dagster asset executions to resume within the same run without reevaluating successfully completed columns. The asset also supports full and structurally incremental scanning.

### Production implementation

The original production implementation uses a two-tier detection process against Snowflake:

- **Tier 1 — Column-name matching:** Reads the persisted Snowflake schema snapshot, excludes columns on a configured allowlist, and matches column names against known PII patterns. Only matching columns advance to further evaluation.
- **Tier 2 — Sample validation:** Samples values from matching string-like columns and validates them against patterns for the identified PII type. It calculates a confidence score based on the proportion of matching samples, subject to a configurable minimum sample size and confidence threshold.
- **Non-string columns:** Records name-matched non-string columns as `pattern_only`, without sampling their values or marking them as confirmed PII.
- **Sampling safeguards:** Uses approximate Snowflake table sizes to avoid sampling known tables exceeding 50 million rows, applies bounded sampling with a query timeout, and executes sampling serially to avoid Snowflake cursor contention.
- **Result persistence:** Writes detection results to a Snowflake `PII_DETECTION_CANDIDATES` table, including PII type, detection method, confidence score, match count, sample size, and `is_pii` status. The table is truncated and repopulated on each run rather than maintaining historical results.
- **Failure handling:** Individual sampling failures are recorded as unconfirmed `pattern_only` results rather than aborting the entire asset. The original implementation does not use the local sample's durable per-column execution checkpoints.

Downstream production assets filter detection results to `is_pii = TRUE` before considering registration.

### Why the local sample differs

The local implementation preserves the fundamental separation between PII detection and registry management, but deliberately changes several detection and execution behaviors.

Sampling every column allows the local scanner to discover PII in columns whose names do not match known patterns, at the cost of additional sampling work. This differs from production's name-first filtering strategy, which reduces the number of expensive Snowflake sampling queries.

The local classifier avoids production's confidence thresholds and minimum sample-size requirements. It records potential matches and supporting evidence without claiming that a column has been definitively confirmed as PII.

The local implementation also introduces durable per-column scan outcomes, resumable execution, and incremental scanning. These are reliability and efficiency improvements rather than direct reproductions of the original production asset.

The result is a self-contained scanner that demonstrates detection, evidence collection, and recoverable execution without reproducing the original Snowflake-specific sampling and confidence-scoring behavior.

## PII candidates for registration Asset

### Local implementation

The `pii_candidates_for_registration` asset receives the detection candidates produced by `pii_detection_candidates` and compares them against the persistent DuckDB registry.

The asset calls `compare_candidates()`, which ensures the registry exists, reads existing review statuses, consolidates positive findings by column, and categorizes them into four groups:

- `net_new`: Detected columns not already present in the registry.
- `previously_rejected`: Detected columns with an existing human rejection.
- `already_accepted`: Count of detected columns previously accepted.
- `awaiting_review`: Count of detected columns already registered as suggested.

Registry identity is determined by a case-insensitive `(DBT_MODEL, FIELD)` key, corresponding to the detected table and column names. `SOURCE_SYSTEM` is intentionally excluded from this comparison, consistent with the original production design.

When multiple positive PII assessments exist for the same column, they are consolidated into one registration candidate. Distinct PII types are represented as `mixed`, while the individual detection evidence remains available in the scan observations.

The asset returns an in-memory `RegistrationResult` containing candidate collections and counts. It does not modify existing human review decisions.

### Production implementation

The original production asset reads confirmed candidates (`is_pii = TRUE`) from the Snowflake detection candidates table, compares them against the Snowflake PII registry, and writes the resulting registration candidates into a separate Snowflake table.

It distinguishes net-new, accepted, rejected, and suggested findings using the same general review-state categories.

The production implementation:

- Reads candidates from the persisted detection table, ordered by confidence score.
- Uses the case-insensitive `(DBT_MODEL, FIELD)` registry identity.
- Writes net-new and previously rejected findings to `PII_CANDIDATES_FOR_REGISTRATION`, distinguishing them with `registration_status`.
- Excludes accepted and suggested findings from the output table.
- Truncates and repopulates the output table on every execution.
- Preserves production detection metadata, including confidence scores.

The production asset's `net_new` metadata count includes previously rejected findings because both categories are written into the same output collection.

### Design differences and rationale

The local implementation replaces the intermediate Snowflake registration-candidates table with a typed, in-memory result passed directly between Dagster assets.

It also separates `net_new` and `previously_rejected` into distinct collections, making the categories explicit and avoiding the ambiguity in the production metadata count.

Local candidate consolidation supports multiple positive PII types per column without creating multiple registration entries.

The key governance behavior remains unchanged: an automated detection does not overwrite a previous human acceptance or rejection.

---

## Registry upsert Asset

### Local implementation

The `registry_upsert` asset receives the `RegistrationResult` from `pii_candidates_for_registration` and passes only `net_new` candidates to `upsert_candidates()`.

The helper ensures the registry schema exists and performs inserts inside a DuckDB transaction.

Each inserted row is initialized with:

- The source schema, model/table, and field name.
- The detected PII type and detection method.
- The source column's data type and materialization.
- `DETECTED_BY = 'scanner'`.
- `PII_CLASSIFICATION_STATUS = 'suggested'`.
- A masking remediation status based on whether the source object is a table or view.
- A creation timestamp.

The helper checks for an existing registry entry using the case-insensitive `(DBT_MODEL, FIELD)` key before inserting.

It returns the number of rows actually inserted. If an exception occurs during the transaction, the transaction is rolled back.

Existing registry rows, including accepted, rejected, and suggested findings, are not updated or replaced.

### Production implementation

The original production asset reads candidates marked `net_new` or `previously_rejected` from the persisted Snowflake registration-candidates table.

It loads existing registry rows, carries forward existing registry metadata where applicable, and prepares rows for insertion.

The production implementation also:

- Ensures the registry matches the canonical production schema, including adding missing columns.
- Uses a temporary Snowflake table to stage candidate rows.
- Inserts rows that do not already exist, based on `(DBT_MODEL, FIELD)`.
- Preserves existing registry entries and their human-curated metadata.
- Seeds masking remediation statuses based on materialization.
- Reports actual inserted rows and skipped duplicates.

Although previously rejected findings are included in the production asset's input, existing registry records are protected from duplicate insertion.

### Design differences and rationale

The local implementation uses a parameterized DuckDB insert with `WHERE NOT EXISTS` and `RETURNING`, rather than staging data in a Snowflake temporary table.

It passes only `net_new` candidates into the insertion helper. Previously rejected findings remain visible for review but are not automatically reinserted.

The local implementation retains the original registry schema, with an additional `DATA_TYPE` column for type-aware masking.

The central design principle is the same in both implementations: detection may propose new registry entries, but it must not overwrite human governance decisions.

---

## PII detection report Asset

### Local implementation

The `pii_detection_report` asset combines the registration comparison, the number of newly inserted registry rows, and persisted scan observations to generate a reviewer-oriented report.

The report includes:

- Current-run counts of net-new, previously rejected, already accepted, and awaiting-review findings.
- Counts of newly registered findings.
- Current-run candidate counts grouped by PII type.
- Previously rejected findings that have been detected again.
- The full set of registry entries currently awaiting review.
- Detection evidence, assessment types, and relevant scan metadata for pending findings.
- Information distinguishing the latest successful evidence from the latest evaluation attempt.
- A suggested command for accepting each pending finding.

The report excludes actual sampled data values, exposing only aggregate evidence such as sample size and match count.

This is important because the report itself should not become an additional source of sensitive data.

The report reads existing governance decisions but does not change them.

### Optional local behavior

The asset supports an optional `report_json_path` configuration parameter.

When provided, the asset creates the destination directory if necessary and writes a formatted JSON report to the specified path.

When omitted or empty, no JSON file is written. The report is still logged, returned from the asset, and summarized in Dagster metadata.

The optional export does not affect registry contents or human review decisions.

### Production implementation

The original production asset generates a report from the Snowflake registration-candidates table.

It summarizes candidate findings by PII type, confidence tier, and registration status, and persists the resulting report in a Snowflake detection-report table.

The production implementation also generates a YAML review-configuration template to support the downstream review workflow.

It includes optional AI-assisted review behavior. When enabled, the report can incorporate AI-generated audit recommendations into the template. Failures in that optional process are designed to degrade to an unaudited template rather than prevent the report from being generated.

### Design differences and rationale

The local implementation replaces the production YAML-template workflow with a structured JSON report and a separate command-line review tool.

It does not reproduce the production confidence-tier reporting or optional AI-assisted audit.

Instead, it emphasizes:

- Reviewable detection evidence without exposing sampled values.
- Durable historical scan observations.
- Visibility into failed reevaluations without losing prior successful evidence.
- Explicit separation of automated findings and human decisions.
- A small, auditable human-review interface.

The local report is therefore both a summary of the current scan and a view of outstanding governance work, whereas the production report is primarily oriented around the current detection batch and its review-configuration workflow.