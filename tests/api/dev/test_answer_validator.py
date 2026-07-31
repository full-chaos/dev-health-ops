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


# --- CHAOS-3290: a complete/substantive answer cannot be an empty shell ---


def _context_without_groundable_material() -> AnswerValidationContext:
    """A run whose only executed tool is a catalog/definitional one
    (list_metrics.v1 in production): it returns `metric_definitions` but
    mints no `metrics`/`evidence`/other groundable fact, exactly like the
    real tool result behind the CHAOS-3290 live reproduction.
    """
    fixtures = positive_fixtures()
    answer = DevAnswer.model_validate(fixtures["dev_answer.v1"])
    tool_result = deepcopy(fixtures["dev_tool_result.v1"])
    tool_result.update(
        {
            "metrics": [],
            "evidence": [],
            "status_facts": [],
            "pull_requests": [],
            "ci_checks": [],
            "deployments": [],
            "incidents": [],
        }
    )
    return AnswerValidationContext(
        conversation_id=answer.conversation_id,
        answer_id=answer.answer_id,
        scope_resolution=DevScopeResolution.model_validate(
            fixtures["dev_scope_resolution.v1"]
        ),
        versions=DevContractVersions.model_validate(answer.versions),
        model=DevModelMetadata.model_validate(answer.model),
        tool_results=(DevToolResult.model_validate(tool_result),),
    )


def _empty_payload(*, status: str, direct_summary: str) -> dict:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload.update(
        {
            "status": status,
            "direct_summary": direct_summary,
            "claims": [],
            "metrics": [],
            "evidence": [],
            "coverage": {
                "required_source_count": 1,
                "available_source_count": 1,
                "unavailable_required_sources": [],
                "stale_required_sources": [],
                "as_of": payload["as_of"],
            },
        }
    )
    return payload


def test_complete_answer_with_available_grounding_cannot_be_empty() -> None:
    """PRD §8: a complete answer with material tool output (real metrics/
    evidence existed for this run) but zero claims, metrics, and evidence
    of its own is structurally impossible, regardless of what its prose
    says.
    """
    payload = _empty_payload(
        status="complete",
        direct_summary=(
            "Everything checked out fine across the board this period, no "
            "issues to report anywhere in the organization's delivery."
        ),
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    assert raised.value.code == "answer_grounding_floor_not_met"
    assert raised.value.repairable is False


def test_complete_catalog_answer_with_a_stub_summary_is_rejected() -> None:
    """Literal CHAOS-3290 live reproduction: a platform gpt-5-nano run for
    the metrics-catalog question (list_metrics.v1 only -- no metric/
    evidence/claim is representable for that tool) terminated
    terminal_reason=complete with empty claims/metrics/evidence and the
    stub summary "Available Ask Dev metrics and their definitions." -- a
    silent non-answer presented as success with favorable (1-of-1)
    coverage and no visible error.
    """
    payload = _empty_payload(
        status="complete",
        direct_summary="Available Ask Dev metrics and their definitions.",
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context_without_groundable_material())
    assert raised.value.code == "answer_grounding_floor_not_met"
    assert raised.value.repairable is False


def test_complete_catalog_answer_with_a_real_listing_is_not_a_stub() -> None:
    """The same list_metrics.v1-only tool shape, but with the prose that a
    thorough catalog answer actually requires (as the real BYO gpt-4o-mini
    run for the identical question produced), must not be penalized just
    because a metric catalog has nothing representable as a claim, metric,
    or evidence ref -- there is nothing to falsely present as grounded here.
    """
    payload = _empty_payload(
        status="complete",
        direct_summary=(
            "The available Ask Dev metrics are as follows: "
            "1. Items completed -- Completed work items in the selected "
            "window. 2. Cycle time p50 -- Median work-item cycle time in "
            "hours. 3. Average WIP -- Average work-in-progress across "
            "daily snapshots. 4. Deployments -- Deployments recorded in "
            "the selected window."
        ),
    )
    answer = validate_answer_candidate(payload, _context_without_groundable_material())
    assert answer.status == "complete"


def test_substantive_partial_narrative_cannot_be_ungrounded() -> None:
    """A partial answer presenting long, confident-sounding prose with zero
    structured grounding is exactly as untrustworthy as a complete one
    (CHAOS-3290) -- "partial" alone is not an excuse for an unsupported
    narrative when real tool material existed to ground it in.
    """
    payload = _empty_payload(
        status="partial",
        direct_summary=(
            "Delivery throughput climbed steadily this period while review "
            "latency held flat, and the organization's overall investment "
            "mix shifted meaningfully toward new feature work across every "
            "team without any material regression worth flagging."
        ),
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    assert raised.value.code == "answer_grounding_floor_not_met"


def test_honest_short_partial_is_not_penalized() -> None:
    """An honestly modest partial answer ("no data yet") never claimed to
    be a substantive result in the first place and must not be gated by
    the grounding floor.
    """
    payload = _empty_payload(status="partial", direct_summary="No data available yet.")
    answer = validate_answer_candidate(payload, _context_without_groundable_material())
    assert answer.status == "partial"
