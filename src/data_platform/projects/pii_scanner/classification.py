from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    DetectionAssessment,
    DetectionStatus,
)


def classify_detection(
    result: ColumnDetectionResult,
) -> tuple[DetectionAssessment, ...]:
    """Classify name and value observations without making review decisions."""
    name_types = {candidate.pii_type for candidate in result.name_candidates}
    evidence_by_type = {evidence.pii_type: evidence for evidence in result.evidence}

    pii_types = sorted(name_types | evidence_by_type.keys())
    assessments = []

    for pii_type in pii_types:
        reasons = []

        if pii_type in name_types:
            reasons.append("column_name_match")

        evidence = evidence_by_type.get(pii_type)

        if evidence is not None and evidence.match_count > 0:
            reasons.append("value_match")

        if reasons:
            status = DetectionStatus.CANDIDATE
        elif evidence is None or evidence.non_missing_count == 0:
            status = DetectionStatus.INSUFFICIENT_EVIDENCE
        else:
            status = DetectionStatus.NO_MATCH

        assessments.append(
            DetectionAssessment(
                pii_type=pii_type,
                status=status,
                reasons=tuple(reasons),
            )
        )

    return tuple(assessments)
