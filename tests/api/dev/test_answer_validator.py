from __future__ import annotations

from copy import deepcopy

import pytest

from dev_health_ops.api.dev.answer_validator import (
    INCOMPLETE_DENOMINATOR_DISCLOSURE,
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


# -----------------------------------------------------------------------
# CHAOS-3297 s2 round 8 (closure, ratified on the ticket): positive-
# obligation partition for the completion-denominator guard.
#
# Rounds 5, 6, and 7 each tried to CLOSE an open set -- detect every
# possible fabricated-completion phrasing by vocabulary. Each round's
# sweep (digit/ratio shapes -> totalizing words -> bare unhedged
# predicates -> hedge-word rescue) was defeated by the next round's
# fresh synonym ("all set", "concluded", "resolved", "delivered",
# "shipped") or a whole-text hedge token rescuing an unequivocal clause
# elsewhere in the SAME sentence ("The work appears fully complete --
# and it is."). That is structural, not a vocabulary gap: natural-
# language completion semantics has no finite vocabulary, so absence-
# of-bad-phrasing can never be a closed check.
#
# The fix inverts the obligation to PRESENCE of a fixed, exact, server-
# specified sentence (answer_validator.INCOMPLETE_DENOMINATOR_
# DISCLOSURE). This makes the domain of every prose field (each
# claim.text, and direct_summary), whenever ANY tool result withheld its
# completion denominator, a TWO-CELL partition -- nothing left to
# enumerate:
#
#   Cell 1 (ACCEPTED): the field's text contains
#   INCOMPLETE_DENOMINATOR_DISCLOSURE, case-insensitive, verbatim.
#   Covered structurally: presence is a bounded, exact, checkable fact,
#   independent of what else the text says. The reader always sees the
#   caveat verbatim alongside any other claim in that field -- the
#   residual risk (ratified as acceptable) is a confident clause NEXT TO
#   an explicit caveat, never a confident clause with no caveat visible
#   anywhere in that field.
#
#   Cell 2 (REJECTED): the field's text does not contain the disclosure.
#   Covered structurally: this is the logical complement of Cell 1 -- by
#   definition every string not in Cell 1 is in Cell 2. No vocabulary
#   detection, no hedge-word list, no synonym enumeration is involved in
#   deciding which cell a string falls into.
#
# Every round-5/6/7 repro -- both the fabricated paraphrases AND the
# "honest hedge" phrasings that used to be treated specially -- now
# lands in Cell 2 uniformly, because none of them happen to contain the
# exact marker. That is deliberate: it proves the mechanism no longer
# depends on recognizing WHICH phrasings are dishonest vs. which are
# honest hedges (see test_every_round_5_6_7_repro_lands_in_cell_2_and_
# is_rejected below).
#
# The obligation applies independently to EVERY claim and to
# direct_summary (never "the disclosure appears somewhere in the
# answer") -- a reader of one field alone (e.g. claims rendered as a
# separate list from direct_summary) must never see an unqualified
# assertion whose only caveat lives in a different field. This closes
# the round-7 "confident clause not adjacent to its caveat" bypass shape
# at the ANSWER level, not just within one sentence. The accepted cost
# (test_topically_unrelated_claim_also_requires_the_disclosure below):
# a claim about something entirely unrelated to completion is ALSO
# rejected in this state, because gating the obligation on an "is this
# claim about completion" vocabulary check would reopen exactly the
# open-set problem this fix exists to close.
#
# When the denominator is NOT withheld
# (test_completion_language_is_fine_when_the_denominator_is_known
# below), the check does not run at all -- the partition is scoped to
# the withheld state, not a blanket ban on completion language.
# -----------------------------------------------------------------------


def test_claim_omitting_the_disclosure_is_rejected() -> None:
    """Cell 2 (claims loop): a claim asserting completion without the
    exact disclosure is rejected regardless of what it cites.
    direct_summary carries the disclosure so this isolates the
    claims-loop mechanism.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = (
        f"See the linked claim. {INCOMPLETE_DENOMINATOR_DISCLOSURE.capitalize()}."
    )
    payload["claims"] = [
        {**payload["claims"][0], "text": "Required work is 100% complete."}
    ]
    with pytest.raises(
        AnswerValidationError, match="claim omits the required disclosure"
    ):
        validate_answer_candidate(payload, _context_with_withheld_completion())


def test_direct_summary_omitting_the_disclosure_is_rejected() -> None:
    """Cell 2 (direct_summary): direct_summary carries no citation
    requirement at all, so it needs the identical positive obligation
    independently of the claims loop. claims carries the disclosure so
    this isolates the direct_summary mechanism.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = "Required work is 100% complete."
    payload["claims"] = [
        {
            **payload["claims"][0],
            "text": (
                f"See direct_summary. {INCOMPLETE_DENOMINATOR_DISCLOSURE.capitalize()}."
            ),
        }
    ]
    with pytest.raises(
        AnswerValidationError, match="direct summary omits the required disclosure"
    ):
        validate_answer_candidate(payload, _context_with_withheld_completion())


# Every prior round's repro, verbatim: none contain the disclosure, so
# all land in Cell 2 (rejected) under the round-8 partition.
_ROUND_5_6_7_REPROS_LAND_IN_CELL_2 = [
    # Round 5: digit/ratio-shape bypasses.
    "Required work is 100% complete.",
    "3 of 5 required items are done.",
    "All 500 required items are finished.",
    "Nothing remains to be done on the required work.",
    "The required work is fully complete.",
    "Three of five required items are complete.",
    "All required items are complete.",
    "None of the required work is outstanding.",
    # Round 6: bare, unhedged predicates -- neither a number nor a
    # totalizing word.
    "The required work is done.",
    "The required work is finished.",
    "The required work is wrapped up.",
    "The required work is closed out.",
    "No required work is left.",
    # Round 6 negatives: were treated as "honestly hedged" under the old
    # vocabulary check. Under the positive-obligation partition, natural-
    # language hedging alone is no longer sufficient -- only the exact
    # marker is.
    "The completion status of required work is unknown because the data "
    "source was truncated.",
    "Some required items appear complete based on available data, but "
    "the full set could not be verified.",
    "The required work is likely done, though this could not be fully verified.",
    "It is unclear whether the required work is finished.",
    "Some of the required work appears done, but full verification was not possible.",
    # Round 7: fresh synonyms the vocabulary sweep missed.
    "The required work is all set.",
    "The required work is concluded.",
    "The required work is resolved.",
    "The required work is delivered.",
    "The required work is shipped.",
    "The required work is fully addressed.",
    "The work is over.",
    # Round 7: whole-text hedge-rescue bypass -- one hedge token
    # ("appears") no longer rescues an unequivocal clause elsewhere in
    # the same sentence.
    "The work appears fully complete -- and it is.",
]


@pytest.mark.parametrize("text", _ROUND_5_6_7_REPROS_LAND_IN_CELL_2)
def test_every_round_5_6_7_repro_lands_in_cell_2_and_is_rejected(text: str) -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = "See the linked claim for details."
    payload["claims"] = [{**payload["claims"][0], "text": text}]
    with pytest.raises(
        AnswerValidationError, match="omits the required disclosure"
    ) as raised:
        validate_answer_candidate(payload, _context_with_withheld_completion())
    assert raised.value.repairable is True
    assert raised.value.code == "completion_denominator_withheld"


def test_topically_unrelated_claim_also_requires_the_disclosure() -> None:
    """Accepted cost of uniform enforcement: a claim about something
    entirely unrelated to completion (no completion language at all) is
    ALSO rejected when the denominator is withheld anywhere in the run's
    tool results. Gating the obligation on an "is this claim about
    completion" check would reopen exactly the open-set problem the
    positive obligation exists to close. The cost is bounded -- one
    repair pass appends the boilerplate sentence (repairable=True); the
    alternative is unbounded.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = "See the linked claim for details."
    payload["claims"] = [
        {
            **payload["claims"][0],
            "text": "Twelve work items were touched in the selected period.",
        }
    ]
    with pytest.raises(
        AnswerValidationError, match="claim omits the required disclosure"
    ):
        validate_answer_candidate(payload, _context_with_withheld_completion())


# Cell 1: text carrying the exact disclosure is accepted regardless of
# case, position, or what else the sentence says.
_TEXT_WITH_DISCLOSURE_LANDS_IN_CELL_1_AND_IS_ACCEPTED = [
    INCOMPLETE_DENOMINATOR_DISCLOSURE.capitalize() + ".",
    f"Some required items look done, but {INCOMPLETE_DENOMINATOR_DISCLOSURE}.",
    INCOMPLETE_DENOMINATOR_DISCLOSURE.upper() + ".",
    # The documented residual: a confident clause NEXT TO the caveat is
    # accepted per the ratified residual-risk note above, not silently
    # absent.
    f"It's 100% done. Note: {INCOMPLETE_DENOMINATOR_DISCLOSURE}.",
]


@pytest.mark.parametrize("text", _TEXT_WITH_DISCLOSURE_LANDS_IN_CELL_1_AND_IS_ACCEPTED)
def test_text_carrying_the_disclosure_lands_in_cell_1_and_is_accepted(
    text: str,
) -> None:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = text
    payload["claims"] = [{**payload["claims"][0], "text": text}]
    # Must not raise.
    validate_answer_candidate(payload, _context_with_withheld_completion())


def test_completion_language_is_fine_when_the_denominator_is_known() -> None:
    """Known-denominator control: the positive obligation only applies
    when the denominator is withheld -- the same phrasing that requires
    the disclosure above must pass cleanly, WITHOUT the disclosure, when
    the tool result carries a real (non-null) denominator.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["direct_summary"] = "All required work is complete."
    payload["claims"] = [
        {**payload["claims"][0], "text": "All required work is complete."}
    ]
    # Should not raise -- _context()'s tool result has actual_completion=None.
    validate_answer_candidate(payload, _context())


def test_honest_partial_answer_with_the_disclosure_passes_end_to_end() -> None:
    """Honest-partial control: a well-formed PARTIAL answer that
    correctly discloses the withheld denominator passes cleanly end to
    end -- this guard, the numeric-citation check, and the CHAOS-3290
    grounding floor all satisfied together.
    """
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    payload["direct_summary"] = (
        f"Some required items appear complete, but {INCOMPLETE_DENOMINATOR_DISCLOSURE}."
    )
    payload["claims"] = [
        {
            **payload["claims"][0],
            "text": (
                f"One required item is done; {INCOMPLETE_DENOMINATOR_DISCLOSURE}."
            ),
        }
    ]
    validate_answer_candidate(payload, _context_with_withheld_completion())


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
