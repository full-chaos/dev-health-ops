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


def _context_with_withheld_completion() -> AnswerValidationContext:
    """A run whose tool result reports a completion assessment with the
    required-child denominator withheld (CHAOS-3297 s2 round 2/3): the
    required-child source itself was truncated, so
    required_child_total/required_child_complete are ``None`` -- an
    honestly unknown denominator, never a fabricated count.
    """
    fixtures = positive_fixtures()
    answer = DevAnswer.model_validate(fixtures["dev_answer.v1"])
    tool_result = deepcopy(fixtures["dev_tool_result.v1"])
    tool_result["actual_completion"] = {
        "state": "indeterminate",
        "rule_id": "actual-completion",
        "rule_version": "actual-completion.v4",
        "reason_codes": ["assessment_source_limit_reached"],
        "required_children": [],
        "required_child_total": None,
        "required_child_complete": None,
        "display_truncated": True,
        "conflicts": [],
        "evidence_ref_ids": [],
    }
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


def test_claim_cannot_state_a_completion_ratio_the_server_withheld() -> None:
    """CHAOS-3297 s2 round 3 (codex HIGH) exact repro: a PARTIAL answer
    claiming 'Required work is 100% complete' must not pass just because
    the claim happens to cite SOME metric/evidence ref -- the existing
    numeric-claim check above only requires a citation exists, it never
    verifies the citation actually grounds the specific number, so a
    withheld (None) denominator must be caught independently. direct_summary
    is neutralized so this isolates the claims-loop mechanism specifically
    (not the sibling direct_summary check below).
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = "See the linked claim for details."
    payload["claims"] = [
        {
            **payload["claims"][0],
            "text": "Required work is 100% complete.",
        }
    ]
    with pytest.raises(AnswerValidationError, match="claim states a completion ratio"):
        validate_answer_candidate(payload, _context_with_withheld_completion())


def test_direct_summary_cannot_state_a_completion_ratio_the_server_withheld() -> None:
    """Same repro, direct_summary variant (codex round 3): direct_summary
    carries no citation requirement at all, so it needs the identical
    guard independently of the claims loop. claims is emptied so this
    isolates the direct_summary check specifically (grounding floor stays
    satisfied via the base fixture's metrics/evidence, which do not
    depend on claims).
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = "Required work is 100% complete."
    payload["claims"] = []
    with pytest.raises(
        AnswerValidationError, match="direct summary states a completion ratio"
    ):
        validate_answer_candidate(payload, _context_with_withheld_completion())


# CHAOS-3297 s2 round 5 (codex HIGH): a digit-and-ratio-shape check alone
# is a paraphrase away from bypassed. Every one of these states the same
# withheld completion total via a different surface form -- a percentage,
# a digit fraction, a bare count + totalizing word, pure totalizing
# vocabulary with no number at all, and a spelled-out fraction.
_WITHHELD_COMPLETION_TOTAL_PARAPHRASES = [
    "Required work is 100% complete.",
    "3 of 5 required items are done.",
    "All 500 required items are finished.",
    "Nothing remains to be done on the required work.",
    "The required work is fully complete.",
    "Three of five required items are complete.",
    "All required items are complete.",
    "None of the required work is outstanding.",
]


@pytest.mark.parametrize("text", _WITHHELD_COMPLETION_TOTAL_PARAPHRASES)
def test_every_paraphrase_of_a_withheld_completion_total_is_rejected(
    text: str,
) -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = "See the linked claim for details."
    payload["claims"] = [{**payload["claims"][0], "text": text}]
    with pytest.raises(AnswerValidationError, match="completion ratio") as raised:
        validate_answer_candidate(payload, _context_with_withheld_completion())
    # Round 5 (codex MEDIUM): repairable, not a dead end -- a phrasing
    # choice gets one bounded correction pass, not a hard failure.
    assert raised.value.repairable is True
    assert raised.value.code == "completion_denominator_withheld"


# CHAOS-3297 s2 round 5 (codex MEDIUM): over-blocking honest answers is
# its own bug. Hedged/uncertain phrasing, a partial-but-honest claim, and
# language about something else entirely (an unrelated throughput number,
# no completion vocabulary at all) must all still pass, even though the
# tool result withheld its denominator.
_LEGITIMATE_PHRASING_EVEN_WHEN_WITHHELD = [
    "The completion status of required work is unknown because the data "
    "source was truncated.",
    "Some required items appear complete based on available data, but "
    "the full set could not be verified.",
    "Twelve work items were touched in the selected period.",
]


@pytest.mark.parametrize("text", _LEGITIMATE_PHRASING_EVEN_WHEN_WITHHELD)
def test_honest_hedged_phrasing_is_never_blocked_even_when_withheld(
    text: str,
) -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = text
    payload["claims"] = [{**payload["claims"][0], "text": text}]
    # Must not raise.
    validate_answer_candidate(payload, _context_with_withheld_completion())


def test_completion_ratio_language_is_fine_when_the_denominator_is_known() -> None:
    """The completion-ratio guard only ever fires when the tool result
    withheld its denominator -- the identical phrasing that's rejected
    above must pass cleanly against a normal (non-withheld) context, or
    this would be a blanket ban on completion language rather than a
    guard against a specific, withheld-data claim.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["direct_summary"] = "All required work is complete."
    payload["claims"] = [
        {**payload["claims"][0], "text": "All required work is complete."}
    ]
    # Should not raise -- _context()'s tool result has actual_completion=None.
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


def _empty_payload(
    *,
    status: str,
    direct_summary: str,
    claims: list | None = None,
    coverage_gap: bool = False,
) -> dict:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload.update(
        {
            "status": status,
            "direct_summary": direct_summary,
            "claims": claims if claims is not None else [],
            "metrics": [],
            "evidence": [],
            "coverage": (
                {
                    "required_source_count": 1,
                    "available_source_count": 0,
                    "unavailable_required_sources": ["list_metrics.v1"],
                    "stale_required_sources": [],
                    "as_of": payload["as_of"],
                }
                if coverage_gap
                else {
                    "required_source_count": 1,
                    "available_source_count": 1,
                    "unavailable_required_sources": [],
                    "stale_required_sources": [],
                    "as_of": payload["as_of"],
                }
            ),
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
            "window. Definition version: items_completed.v1. "
            "2. Cycle time p50 -- Median work-item cycle time in hours."
        ),
    )
    answer = validate_answer_candidate(payload, _context_without_groundable_material())
    assert answer.status == "complete"


def test_complete_catalog_answer_stating_only_the_count_is_still_a_stub() -> None:
    """Codex adversarial review (CHAOS-3290 follow-up): accepting the bare
    retrieved *count* as proof of having read the catalog is gameable --
    "I have 1 unresolved limitation and cannot provide the requested metric
    catalog." coincidentally contains "1" when exactly one definition was
    retrieved, with zero relationship to the catalog. Only naming the
    definitions' own machine identifiers counts; a bare count never does,
    regardless of how many were retrieved.
    """
    context = _context_without_groundable_material()
    retrieved_count = len(context.tool_results[0].metric_definitions)
    payload = _empty_payload(
        status="complete",
        direct_summary=f"{retrieved_count} Ask Dev metrics are available in this scope.",
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, context)
    assert raised.value.code == "answer_grounding_floor_not_met"


def test_complete_catalog_answer_covering_half_the_definitions_is_not_a_stub() -> None:
    """A real, thorough catalog answer only needs to demonstrably cover at
    least half of what was actually retrieved (naming every one of a large
    catalog verbatim is not required for the answer to be genuine).
    """
    fixtures = positive_fixtures()
    tool_result = deepcopy(fixtures["dev_tool_result.v1"])
    real_metric_ids = [
        "items_completed",
        "cycle_time_p50_hours",
        "avg_wip",
        "deployments_count",
    ]
    definitions = []
    for metric_id in real_metric_ids:
        definition = deepcopy(tool_result["metric_definitions"][0])
        definition["metric_id"] = metric_id
        definition["definition_version"] = f"{metric_id}.v1"
        definitions.append(definition)
    tool_result.update(
        {
            "metric_definitions": definitions,
            "metrics": [],
            "evidence": [],
            "status_facts": [],
            "pull_requests": [],
            "ci_checks": [],
            "deployments": [],
            "incidents": [],
        }
    )
    answer = DevAnswer.model_validate(fixtures["dev_answer.v1"])
    context = AnswerValidationContext(
        conversation_id=answer.conversation_id,
        answer_id=answer.answer_id,
        scope_resolution=DevScopeResolution.model_validate(
            fixtures["dev_scope_resolution.v1"]
        ),
        versions=DevContractVersions.model_validate(answer.versions),
        model=DevModelMetadata.model_validate(answer.model),
        tool_results=(DevToolResult.model_validate(tool_result),),
    )
    payload = _empty_payload(
        status="complete",
        direct_summary=(
            "Two of the four registered metrics: items_completed.v1 and "
            "cycle_time_p50_hours.v1."
        ),
    )
    answer = validate_answer_candidate(payload, context)
    assert answer.status == "complete"


def test_complete_catalog_answer_naming_the_metrics_but_not_the_catalog_is_a_stub() -> (
    None
):
    """Live-reproduced follow-up: a platform run for a metric-comparison
    question called only list_metrics.v1, then answered status=complete
    with a summary that names the metrics *from the user's own question*
    ("cycle time p50 and Average WIP are defined metrics over a 30-day
    window...") without ever citing anything from the catalog it actually
    retrieved, and admitted "I don't have the actual numeric data in this
    thread." Long, and "30" satisfies a bare digit check, but it never
    reflects the retrieved catalog -- this must still be rejected.
    """
    payload = _empty_payload(
        status="complete",
        direct_summary=(
            "I don’t have the actual numeric data in this thread. Based on "
            "the available tool results, cycle time p50 and Items Completed "
            "are defined metrics over a 30-day window with daily "
            "granularity. The metric definitions indicate:"
        ),
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context_without_groundable_material())
    assert raised.value.code == "answer_grounding_floor_not_met"


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
    """An honestly modest partial answer ("no data yet") is only honest
    because the server's own coverage accounting backs it up -- a required
    source really is reported unavailable here. It must not be gated by the
    grounding floor.
    """
    payload = _empty_payload(
        status="partial", direct_summary="No data available yet.", coverage_gap=True
    )
    answer = validate_answer_candidate(payload, _context_without_groundable_material())
    assert answer.status == "partial"


def test_short_confident_partial_with_favorable_coverage_is_still_rejected() -> None:
    """Codex adversarial review (CHAOS-3290 follow-up): prose length is not
    a safe grounding signal -- a *short*, confident, fabricated-sounding
    narrative ("Delivery performance improved substantially across every
    team.") is exactly as untrustworthy as a long one when there is zero
    structured grounding and the server's own coverage reports nothing
    missing. Must be rejected regardless of brevity.
    """
    payload = _empty_payload(
        status="partial",
        direct_summary="Delivery performance improved substantially across every team.",
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context())
    assert raised.value.code == "answer_grounding_floor_not_met"


def test_unsupported_inferred_claim_does_not_disable_the_grounding_floor() -> None:
    """Codex adversarial review (CHAOS-3290 follow-up): DevClaim permits an
    ``inferred`` claim with confidence < 1 and zero metric/evidence refs.
    Adding exactly one such claim previously made ``answer.claims``
    non-empty and disabled the entire grounding floor, letting a complete
    answer with a stub summary and one fabricated, unreferenced claim
    through. The floor must check whether any claim actually references a
    metric or evidence ID, not merely whether the claims array is
    non-empty.
    """
    payload = _empty_payload(
        status="complete",
        direct_summary="Available Ask Dev metrics and their definitions.",
        claims=[
            {
                "schema_version": "dev_claim.v1",
                "claim_id": "unsupported_claim_01",
                "kind": "inferred",
                "text": "Delivery performance improved substantially across every team.",
                "confidence": 0.5,
                "evidence_ref_ids": [],
                "metric_ref_ids": [],
                "validity_scope": positive_fixtures()["dev_scope_resolution.v1"][
                    "resolved_scope"
                ],
                "flags": {
                    "stale": False,
                    "uncertain": False,
                    "conflicting": False,
                    "untrusted_source": False,
                },
                "recommendation_rule_version": None,
            }
        ],
    )
    with pytest.raises(AnswerValidationError) as raised:
        validate_answer_candidate(payload, _context_without_groundable_material())
    assert raised.value.code == "answer_grounding_floor_not_met"
