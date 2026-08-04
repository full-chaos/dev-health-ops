"""CHAOS-3377: the §10 deterministic status-answer renderer.

Per-defect fail->pass coverage for the ticket's five presentation defects,
scoped to the pure rendering functions in ``status_answer_render.py``. The
orchestrator-level wiring (candidate override, status never REFUSED for a
run with a status_snapshot result) is covered separately in
``test_chaos_3377_orchestrator_status_answer.py``.
"""

from __future__ import annotations

from typing import get_args

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    DevActualCompletion,
    DevCoverage,
    DevRequiredChildFact,
    DevScope,
    DevToolResult,
)
from dev_health_ops.api.dev.status_answer_render import (
    build_deterministic_status_claims,
    deterministic_answer_status,
    is_open_child_status,
    open_required_children,
    render_verdict_summary,
    status_snapshot_result,
    translate_completion_state,
    translate_reason_code,
)
from dev_health_ops.api.dev.status_change_service import STATUS_REASON_CODES

SCOPE = DevScope.model_validate(positive_fixtures()["dev_scope.v1"])


def _actual(
    *,
    state: str = "not_ready",
    reason_codes: tuple[str, ...] = ("open_blocker",),
    required_children: tuple[DevRequiredChildFact, ...] = (),
    required_child_total: int | None = 69,
    required_child_complete: int | None = 39,
    evidence_ref_ids: tuple[str, ...] = ("ev_01",),
) -> DevActualCompletion:
    return DevActualCompletion(
        state=state,
        rule_id="actual-completion",
        rule_version="actual-completion.v4",
        reason_codes=list(reason_codes),
        required_children=list(required_children),
        required_child_total=required_child_total,
        required_child_complete=required_child_complete,
        display_truncated=False,
        conflicts=[],
        evidence_ref_ids=list(evidence_ref_ids),
    )


def _child(fact_id: str, text: str, status: str, evidence_ref_ids=("ev_01",)):
    return DevRequiredChildFact(
        fact_id=fact_id,
        text=text,
        status=status,
        evidence_ref_ids=list(evidence_ref_ids),
    )


def _tool_result(
    *, actual_completion: DevActualCompletion | None, status: str = "success"
):
    payload = dict(positive_fixtures()["dev_tool_result.v1"])
    payload["actual_completion"] = (
        actual_completion.model_dump(mode="json") if actual_completion else None
    )
    payload["status"] = status
    return DevToolResult.model_validate(payload)


# --- defect 2: raw internal vocabulary must never reach translated copy ---


def test_translation_tables_are_total_and_closed() -> None:
    """Every ``STATUS_REASON_CODES`` member translates to safe copy, and the
    completion ``state`` Literal is fully covered -- the totality assertion
    the module raises at import time if this ever drifts.
    """

    for code in STATUS_REASON_CODES:
        translated = translate_reason_code(code)
        assert code not in translated

    for state in get_args(DevActualCompletion.model_fields["state"].annotation):
        translated = translate_completion_state(state)
        assert state not in translated


def test_unknown_reason_code_fails_closed_to_generic_copy() -> None:
    """A code the table has never seen must never reach the raw token --
    this is what makes the table fail-closed rather than a best-effort map.
    """

    translated = translate_reason_code("some_future_reason_code_v7")
    assert translated == "an unresolved requirement"
    assert "some_future_reason_code_v7" not in translated


def test_verdict_summary_never_contains_raw_state_or_reason_tokens() -> None:
    actual = _actual(
        state="not_ready",
        reason_codes=("open_blocker", "required_child_incomplete"),
    )
    summary = render_verdict_summary(actual)
    for forbidden in (
        "not_ready",
        "open_blocker",
        "required_child_incomplete",
        "actual_completion",
    ):
        assert forbidden not in summary
    # The numeric completion fraction is still real, checkable content.
    assert "39" in summary and "69" in summary


def test_verdict_claim_never_leaks_an_evidence_handle() -> None:
    actual = _actual(evidence_ref_ids=("ev1_" + "a" * 40,))
    claims = build_deterministic_status_claims(
        actual=actual,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev1_" + "a" * 40}),
    )
    assert "ev1_" not in claims[0].text


# --- defect 5: blocker list must never contradict itself ---


def test_open_required_children_excludes_completed_and_done_items() -> None:
    """CHAOS-3377 defect 5 fail->pass: the live bug listed an item labeled
    'completed'/'done' under 'Current blockers'. A blocker section built
    from ``open_required_children`` cannot reproduce that -- it filters on
    the frame's OWN ``status`` field, not narrative.
    """

    actual = _actual(
        required_children=(
            _child("issue:OPEN-1", "Open work item", "in_progress"),
            _child("issue:DONE-1", "Finished work item", "completed"),
            _child("issue:DONE-2", "Also finished", "done"),
            _child("issue:CLOSED-1", "Closed out", "Closed"),
        )
    )
    open_children = open_required_children(actual)
    open_ids = {child.fact_id for child in open_children}
    assert open_ids == {"issue:OPEN-1"}
    assert "issue:DONE-1" not in open_ids
    assert "issue:DONE-2" not in open_ids
    assert "issue:CLOSED-1" not in open_ids


@pytest.mark.parametrize(
    "status",
    [
        "complete",
        "completed",
        "DONE",
        "closed",
        "canceled",
        "cancelled",
        "resolved",
        "merged",
    ],
)
def test_is_open_child_status_closed_vocabulary(status: str) -> None:
    assert is_open_child_status(status) is False


@pytest.mark.parametrize("status", ["in_progress", "Open", "Blocked", "in_review"])
def test_is_open_child_status_open_vocabulary(status: str) -> None:
    assert is_open_child_status(status) is True


def test_blocker_claims_never_include_a_closed_item() -> None:
    actual = _actual(
        required_children=(
            _child("issue:OPEN-1", "Open work item", "in_progress", ("ev_01",)),
            _child("issue:DONE-1", "Finished work item", "completed", ("ev_01",)),
        )
    )
    claims = build_deterministic_status_claims(
        actual=actual,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev_01"}),
    )
    blocker_claim_ids = {
        claim.claim_id
        for claim in claims
        if claim.claim_id.startswith("status-blocker:")
    }
    assert blocker_claim_ids == {"status-blocker:issue:OPEN-1"}
    for claim in claims:
        assert "issue:DONE-1" not in claim.claim_id


def test_blocker_claim_skipped_rather_than_fabricated_when_evidence_is_truncated() -> (
    None
):
    """An open child whose evidence didn't survive result truncation is
    omitted, not emitted with an invented reference -- an OBSERVED claim
    must cite real, canonical evidence.
    """

    actual = _actual(
        required_children=(
            _child("issue:OPEN-1", "Open item", "Open", ("ev_missing",)),
        )
    )
    claims = build_deterministic_status_claims(
        actual=actual,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev_01"}),
    )
    assert all(claim.claim_id != "status-blocker:issue:OPEN-1" for claim in claims)


# --- defect 1: a deterministic verdict is never a refusal ---


def test_deterministic_status_is_never_refused() -> None:
    coverage = DevCoverage.model_validate(
        {
            "required_source_count": 1,
            "available_source_count": 1,
            "unavailable_required_sources": [],
            "stale_required_sources": [],
            "as_of": positive_fixtures()["dev_answer.v1"]["as_of"],
        }
    )
    status = deterministic_answer_status(
        coverage=coverage, tool_results=(_tool_result(actual_completion=_actual()),)
    )
    assert status is not AnswerStatus.REFUSED
    assert status is AnswerStatus.COMPLETE


def test_deterministic_status_downgrades_on_degraded_tool_result() -> None:
    coverage = DevCoverage.model_validate(
        {
            "required_source_count": 1,
            "available_source_count": 1,
            "unavailable_required_sources": [],
            "stale_required_sources": [],
            "as_of": positive_fixtures()["dev_answer.v1"]["as_of"],
        }
    )
    status = deterministic_answer_status(
        coverage=coverage,
        tool_results=(_tool_result(actual_completion=_actual(), status="unavailable"),),
    )
    assert status is AnswerStatus.DEGRADED


# --- seam: which runs get the deterministic renderer at all ---


def test_status_snapshot_result_finds_the_actual_completion_bearing_result() -> None:
    plain = _tool_result(actual_completion=None)
    status_bearing = _tool_result(actual_completion=_actual())
    assert status_snapshot_result((plain,)) is None
    assert status_snapshot_result((plain, status_bearing)) is status_bearing


# --- CHAOS-3297 s2 round 8 disclosure obligation still applies ---


def test_verdict_carries_the_disclosure_when_denominator_withheld() -> None:
    actual = _actual(required_child_total=None, required_child_complete=None)
    summary = render_verdict_summary(actual, denominator_withheld=True)
    assert (
        "the required-work completion total could not be fully verified"
        in summary.casefold()
    )
