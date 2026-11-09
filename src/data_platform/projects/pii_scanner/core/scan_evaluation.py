"""Evaluate pending columns and persist their scan observations."""

from collections.abc import Callable

import duckdb

from data_platform.projects.pii_scanner.classification import classify_detection
from data_platform.projects.pii_scanner.core.scan_observations import (
    record_failed_evaluation,
)
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnEvaluationMetrics,
    ColumnMetadata,
    DetectionStatus,
)


def evaluate_pending_columns(
    con: duckdb.DuckDBPyConnection,
    scan_id: str,
    columns: list[ColumnMetadata],
    column_states: dict[tuple[str, str, str], str],
    *,
    detect_column: Callable[..., ColumnDetectionResult],
    record_evaluation: Callable[..., DetectionStatus],
    clock: Callable[[], float],
) -> tuple[dict[str, int], Exception | None]:
    """Evaluate pending columns in order and return this attempt's results."""
    candidate_count = 0
    attempted = 0
    sampling_queries = sampled_values = non_null_sampled_values = 0
    failure = None
    column_failure = None

    try:
        for column in columns:
            key = (column.schema, column.table, column.column)

            if column_states[key] != "pending":
                continue

            metrics = ColumnEvaluationMetrics()
            attempted += 1
            evaluation_started = clock()
            try:
                detection = detect_column(con, column, metrics=metrics)
                assessments = classify_detection(detection)
            except Exception as error:
                metrics.evaluation_duration_seconds = clock() - evaluation_started
                record_failed_evaluation(
                    con, scan_id, column, type(error).__name__, metrics=metrics
                )
                if column_failure is None:
                    column_failure = error
            else:
                metrics.evaluation_duration_seconds = clock() - evaluation_started
                record_evaluation(con, scan_id, detection, assessments, metrics=metrics)
                candidate_count += sum(
                    assessment.status == DetectionStatus.CANDIDATE
                    for assessment in assessments
                )
            finally:
                sampling_queries += metrics.sampling_queries
                sampled_values += metrics.sampled_values
                non_null_sampled_values += metrics.non_null_sampled_values
    except Exception as error:
        failure = error
    if failure is None:
        failure = column_failure

    return (
        {
            "attempted_columns": attempted,
            "candidate_count": candidate_count,
            "sampling_queries": sampling_queries,
            "sampled_values": sampled_values,
            "non_null_sampled_values": non_null_sampled_values,
        },
        failure,
    )
