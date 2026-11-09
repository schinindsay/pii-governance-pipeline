from data_platform.projects.pii_scanner.classification import (
    classify_detection,
)
from data_platform.projects.pii_scanner.models import (
    ColumnDetectionResult,
    ColumnMetadata,
    DetectionCandidate,
    DetectionEvidence,
    DetectionStatus,
    ObjectType,
)

COLUMN = ColumnMetadata(
    schema="raw",
    table="customers",
    column="notes",
    data_type="VARCHAR",
    ordinal_position=1,
    object_type=ObjectType.TABLE,
)


def make_result(
    *,
    name_match=False,
    sample_size=10,
    non_missing_count=10,
    match_count=0,
):
    candidates = (
        (
            DetectionCandidate(
                column=COLUMN,
                pii_type="email",
                detection_method="column_name",
            ),
        )
        if name_match
        else ()
    )

    return ColumnDetectionResult(
        column=COLUMN,
        name_candidates=candidates,
        evidence=(
            DetectionEvidence(
                pii_type="email",
                sample_size=sample_size,
                non_missing_count=non_missing_count,
                match_count=match_count,
            ),
        ),
    )


def test_name_match_creates_candidate():
    result = classify_detection(make_result(name_match=True))

    assert result[0].status == DetectionStatus.CANDIDATE
    assert result[0].reasons == ("column_name_match",)


def test_value_match_creates_candidate():
    result = classify_detection(make_result(match_count=1))

    assert result[0].status == DetectionStatus.CANDIDATE
    assert result[0].reasons == ("value_match",)


def test_sparse_value_match_is_preserved():
    result = classify_detection(
        make_result(
            sample_size=1000,
            non_missing_count=1,
            match_count=1,
        )
    )

    assert result[0].status == DetectionStatus.CANDIDATE


def test_empty_sample_is_insufficient():
    result = classify_detection(
        make_result(
            sample_size=0,
            non_missing_count=0,
        )
    )

    assert result[0].status == DetectionStatus.INSUFFICIENT_EVIDENCE


def test_no_matches():
    result = classify_detection(make_result())

    assert result[0].status == DetectionStatus.NO_MATCH


def test_name_and_value_match():
    result = classify_detection(make_result(name_match=True, match_count=2))

    assert result[0].reasons == (
        "column_name_match",
        "value_match",
    )


def test_name_only_pii_type_is_preserved():
    candidate = DetectionCandidate(
        column=COLUMN,
        pii_type="ssn",
        detection_method="column_name",
    )

    result = ColumnDetectionResult(
        column=COLUMN,
        name_candidates=(candidate,),
        evidence=(),
    )

    assessments = classify_detection(result)

    assert len(assessments) == 1
    assert assessments[0].pii_type == "ssn"
    assert assessments[0].status == DetectionStatus.CANDIDATE
    assert assessments[0].reasons == ("column_name_match",)
