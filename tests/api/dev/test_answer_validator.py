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
