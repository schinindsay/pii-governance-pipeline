"""Reconstruct registration candidates from persisted scan evidence."""

import json

import duckdb

from data_platform.projects.pii_scanner.core.detection import detect_name_candidates
from data_platform.projects.pii_scanner.core.registry import RegistrationCandidate
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnMetadata,
    DetectionAssessment,
    DetectionEvidence,
    DetectionStatus,
)


def load_scan_candidates(
    con: duckdb.DuckDBPyConnection,
    scan_id: str,
) -> list[RegistrationCandidate]:
    """Reconstruct all registration candidates from persisted scan evidence."""
    rows = con.execute(
        """
        SELECT source_schema, relation_name, column_name,
               data_type, ordinal_position, object_type,
               evidence, assessments
        FROM scan_state.column_observations
        WHERE scan_id = ? AND outcome = 'candidate'
        ORDER BY source_schema, relation_name, ordinal_position
        """,
        [scan_id],
    ).fetchall()

    candidates = []

    for (
        schema,
        table,
        column,
        data_type,
        position,
        object_type,
        evidence,
        assessments,
    ) in rows:
        metadata = ColumnMetadata(
            schema=schema,
            table=table,
            column=column,
            data_type=data_type,
            ordinal_position=position,
            object_type=object_type,
        )

        detection = ColumnDetectionResult(
            column=metadata,
            name_candidates=tuple(detect_name_candidates(metadata)),
            evidence=tuple(DetectionEvidence(**item) for item in json.loads(evidence)),
        )

        for item in json.loads(assessments):
            assessment = DetectionAssessment(**item)
            if assessment.status == DetectionStatus.CANDIDATE:
                candidates.append(
                    RegistrationCandidate(
                        result=detection,
                        assessment=assessment,
                    )
                )

    return candidates
