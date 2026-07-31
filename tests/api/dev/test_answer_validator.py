from __future__ import annotations

from copy import deepcopy

import pytest

from dev_health_ops.api.dev.answer_validator import (
    AnswerValidationContext,
    AnswerValidationError,
    validate_answer_candidate,
)
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevContractVersions,
    DevModelMetadata,
    DevScopeResolution,
    DevToolResult,
)


def _context() -> AnswerValidationContext:
    fixtures = positive_fixtures()
    answer = DevAnswer.model_validate(fixtures["dev_answer.v1"])
    return AnswerValidationContext(
        conversation_id=answer.conversation_id,
        answer_id=answer.answer_id,
        scope_resolution=DevScopeResolution.model_validate(
            fixtures["dev_scope_resolution.v1"]
        ),
        versions=DevContractVersions.model_validate(answer.versions),
        model=DevModelMetadata.model_validate(answer.model),
        tool_results=(DevToolResult.model_validate(fixtures["dev_tool_result.v1"]),),
    )


def test_valid_answer_preserves_server_issued_metric_and_evidence() -> None:
    answer = validate_answer_candidate(positive_fixtures()["dev_answer.v1"], _context())
    assert answer.metrics[0].value == 12
    assert answer.evidence[0].evidence_ref_id == "ev_01"


def test_mutated_metric_is_nonrepairable() -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["metrics"][0]["value"] = 999
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    assert raised.value.repairable is False


def test_unknown_evidence_is_not_sent_back_for_model_repair() -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["claims"][0]["evidence_ref_ids"] = ["invented"]
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    assert raised.value.repairable is False


def test_schema_only_failure_allows_one_bounded_repair() -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload.pop("direct_summary")
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    assert raised.value.repairable is True
    # CHAOS-3288: a missing-field detail names the actual field (one of our
    # own fixed dev_answer.v1 field names, never model-echoed content) so a
    # bare "Field required" repair prompt is actionable.
    assert "direct_summary" in raised.value.detail
    assert "Field required" in raised.value.detail


def test_repairability_is_classified_from_safe_messages_not_echoed_input() -> None:
    """CHAOS-3288 review: repairability must not depend on `str(exc)`, which
    also renders the model's own (echoed) input value. Two unrelated invalid
    `status` values that happen to produce the identical safe message must
    get the identical classification, even when one value's *text* happens
    to collide with a non-repairable marker like "unknown metric".
    """
    baseline = deepcopy(positive_fixtures()["dev_answer.v1"])

    ordinary_payload = deepcopy(baseline)
    ordinary_payload["status"] = "not-a-status"
    with pytest.raises(AnswerValidationError) as ordinary:
        validate_answer_candidate(ordinary_payload, _context())

    colliding_payload = deepcopy(baseline)
    colliding_payload["status"] = "unknown metric"
    with pytest.raises(AnswerValidationError) as colliding:
        validate_answer_candidate(colliding_payload, _context())

    assert ordinary.value.repairable is True
    assert colliding.value.repairable is True
    assert "unknown metric" not in colliding.value.detail.casefold()


def test_many_validation_errors_produce_a_bounded_detail_without_a_cut_word() -> None:
    """CHAOS-3288 review: bounding the joined detail must not slice through
    the middle of a message. A large number of forbidden extra fields
    produces many identical "Extra inputs are not permitted" errors; the
    bounded detail must end on a whole message (or an explicit omitted
    count), never a truncated fragment like "Extra inputs are not".
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload.update({f"unexpected_extra_field_{i}": i for i in range(30)})
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    detail = raised.value.detail
    assert len(detail) <= 200
    assert not detail.rstrip().endswith("Extra inputs are not")
    assert detail.endswith("more)") or detail.strip().endswith(
        "Extra inputs are not permitted"
    )


def test_server_identity_scope_and_runtime_metadata_cannot_be_rewritten() -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["conversation_id"] = "another_conversation"
    with pytest.raises(AnswerValidationError, match="server issued"):
        validate_answer_candidate(payload, _context())


def test_numeric_inference_requires_metric_or_source_reference() -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["claims"] = [
        {
            **payload["claims"][0],
            "kind": "inferred",
            "text": "Risk increased by 42%.",
            "confidence": 0.5,
            "evidence_ref_ids": [],
            "metric_ref_ids": [],
        }
    ]
    with pytest.raises(AnswerValidationError, match="numeric claim"):
        validate_answer_candidate(payload, _context())
