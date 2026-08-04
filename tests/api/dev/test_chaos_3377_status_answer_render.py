"""CHAOS-3377: the §10 deterministic status-answer renderer.

Per-defect fail->pass coverage for the ticket's five presentation defects,
plus the codex adversarial review's HIGH/MEDIUM findings against the first
revision (scope binding, per-category outstanding facts, no raw-status
interpolation), scoped to the pure rendering functions in
``status_answer_render.py``. The orchestrator-level wiring (candidate
override applied to AgentFinalAnswer/AgentRefusal/BudgetExceeded alike, scope
binding end to end) is covered separately in ``test_orchestrator.py``.
"""

from __future__ import annotations

from typing import get_args

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    DevActualCompletion,
    DevCIFact,
    DevCoverage,
    DevDeploymentFact,
    DevIncidentFact,
    DevPullRequestFact,
    DevRequiredChildFact,
    DevScope,
    DevToolRequest,
    DevToolResult,
)
from dev_health_ops.api.dev.no_match_terminal import INTERNAL_TOKEN_DENYLIST
from dev_health_ops.api.dev.status_answer_render import (
    build_deterministic_status_claims,
    deterministic_answer_status,
    open_blockers,
    open_required_children,
    outstanding_facts,
    render_declared_project_summary,
    render_verdict_summary,
    status_snapshot_result,
    translate_completion_state,
    translate_reason_code,
)
from dev_health_ops.api.dev.status_change_service import (
    STATUS_REASON_CODES,
    is_required_child_open,
)
from dev_health_ops.api.dev.status_completion_copy import translate_project_state

SCOPE = DevScope.model_validate(positive_fixtures()["dev_scope.v1"])
OTHER_SCOPE = DevScope.model_validate(
    {**positive_fixtures()["dev_scope.v1"], "organization_id": "org_other"}
)
OBSERVED_AT = positive_fixtures()["dev_answer.v1"]["as_of"]


def _actual(
    *,
    state: str = "not_ready",
    reason_codes: tuple[str, ...] = ("open_blocker",),
    required_children: tuple[DevRequiredChildFact, ...] = (),
    blockers: tuple[DevRequiredChildFact, ...] = (),
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
        blockers=list(blockers),
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


def _pull_request(
    entity_id: str,
    *,
    required: bool = True,
    merged: bool = True,
    review_state: str | None = "approved",
    changes_requested: int = 0,
    evidence_ref_ids=("ev_01",),
) -> DevPullRequestFact:
    return DevPullRequestFact(
        entity_id=entity_id,
        display_label=f"PR {entity_id}",
        state="open",
        review_state=review_state,
        changes_requested=changes_requested,
        merged=merged,
        required=required,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=list(evidence_ref_ids),
    )


def _ci(
    entity_id: str,
    *,
    required: bool | None = True,
    skipped_required_work: bool | None = False,
    conclusion: str = "success",
    evidence_ref_ids=("ev_01",),
) -> DevCIFact:
    return DevCIFact(
        entity_id=entity_id,
        display_label=f"CI {entity_id}",
        conclusion=conclusion,
        required=required,
        skipped_required_work=skipped_required_work,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=list(evidence_ref_ids),
    )


def _deployment(
    entity_id: str,
    *,
    required: bool = True,
    status: str = "succeeded",
    evidence_ref_ids=("ev_01",),
) -> DevDeploymentFact:
    return DevDeploymentFact(
        entity_id=entity_id,
        display_label=f"Deployment {entity_id}",
        status=status,
        required=required,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=list(evidence_ref_ids),
    )


def _incident(
    entity_id: str,
    *,
    active: bool = False,
    blocking: bool = False,
    evidence_ref_ids=("ev_01",),
) -> DevIncidentFact:
    return DevIncidentFact(
        entity_id=entity_id,
        display_label=f"Incident {entity_id}",
        status="open",
        active=active,
        blocking=blocking,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=list(evidence_ref_ids),
    )


def _tool_request(
    *,
    scope: DevScope = SCOPE,
    tool_id: str = "status_snapshot.v1",
    tool_call_id: str = "tool_call_01",
):
    payload = dict(positive_fixtures()["dev_tool_request.v1"])
    payload.update(
        {
            "tool_id": tool_id,
            "scope": scope.model_dump(mode="json"),
            "tool_call_id": tool_call_id,
        }
    )
    return DevToolRequest.model_validate(payload)


def _evidence_stub(ref_id: str) -> dict:
    base = dict(positive_fixtures()["dev_tool_result.v1"]["evidence"][0])
    base["evidence_ref_id"] = ref_id
    return base


def _tool_result(
    *,
    actual_completion: DevActualCompletion | None,
    status: str = "success",
    tool_call_id: str = "tool_call_01",
    pull_requests: tuple[DevPullRequestFact, ...] = (),
    ci_checks: tuple[DevCIFact, ...] = (),
    deployments: tuple[DevDeploymentFact, ...] = (),
    incidents: tuple[DevIncidentFact, ...] = (),
    declared_project_state: str | None = None,
    declared_project_target_date: str | None = None,
    declared_project_evidence_ref_ids: tuple[str, ...] = (),
) -> DevToolResult:
    payload = dict(positive_fixtures()["dev_tool_result.v1"])
    payload["tool_call_id"] = tool_call_id
    payload["actual_completion"] = (
        actual_completion.model_dump(mode="json") if actual_completion else None
    )
    payload["status"] = status
    payload["pull_requests"] = [item.model_dump(mode="json") for item in pull_requests]
    payload["ci_checks"] = [item.model_dump(mode="json") for item in ci_checks]
    payload["deployments"] = [item.model_dump(mode="json") for item in deployments]
    payload["incidents"] = [item.model_dump(mode="json") for item in incidents]
    payload["declared_project_state"] = declared_project_state
    payload["declared_project_target_date"] = declared_project_target_date
    payload["declared_project_evidence_ref_ids"] = list(
        declared_project_evidence_ref_ids
    )
    # DevToolResult.validate_evidence_closure requires every evidence ID any
    # fact references to be present in `evidence` -- mint a stub for
    # whatever `actual_completion`/the category facts above reference so
    # tests can freely choose evidence ids without hand-maintaining a
    # matching `evidence` array.
    referenced: set[str] = set()
    if actual_completion is not None:
        referenced.update(actual_completion.evidence_ref_ids)
        for child in actual_completion.required_children:
            referenced.update(child.evidence_ref_ids)
        for blocker in actual_completion.blockers:
            referenced.update(blocker.evidence_ref_ids)
    for group in (pull_requests, ci_checks, deployments, incidents):
        for item in group:
            referenced.update(item.evidence_ref_ids)
    referenced.update(declared_project_evidence_ref_ids)
    payload["evidence"] = [_evidence_stub(ref_id) for ref_id in sorted(referenced)]
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


# --- CHAOS-3368 step 2: declared project state/target date in §10 ---------


def test_render_declared_project_summary_translates_state_and_names_target_date() -> (
    None
):
    """CHAOS-3368 acceptance: a bound status_snapshot.v1 result carrying a
    declared project state/target date renders both into the verdict/
    summary section -- the state TRANSLATED (never the raw provider token),
    the date as a plain ISO string (structured data, not vocabulary).
    """

    result = _tool_result(
        actual_completion=_actual(state="ready", reason_codes=()),
        declared_project_state="started",
        declared_project_target_date="2026-09-01",
        declared_project_evidence_ref_ids=("ev_01",),
    )
    canonical_evidence_ids = frozenset(item.evidence_ref_id for item in result.evidence)
    summary = render_declared_project_summary(result, canonical_evidence_ids)
    assert summary is not None
    assert "in progress" in summary
    assert "2026-09-01" in summary
    assert "started" not in summary.casefold()


def test_render_declared_project_summary_returns_none_when_absent() -> None:
    """CHAOS-3368 negative control: no declared state/target date at all
    (the RawStatusSnapshot layer's own absence case) renders no clause --
    never a fabricated 'Declared state: None.' or similar.
    """

    result = _tool_result(actual_completion=_actual(state="ready", reason_codes=()))
    assert result.declared_project_state is None
    assert result.declared_project_target_date is None
    canonical_evidence_ids = frozenset(item.evidence_ref_id for item in result.evidence)
    assert render_declared_project_summary(result, canonical_evidence_ids) is None


def test_render_declared_project_summary_unknown_state_falls_back_safely() -> None:
    """CHAOS-3368: a provider state outside the known vocabulary (a future
    Linear addition, or a different provider entirely) must render through
    the safe generic fallback, never the raw token -- ``translate_project_state``
    is deliberately NOT total/closed against a pinned set (unlike reason
    codes), since this is external provider data.
    """

    result = _tool_result(
        actual_completion=_actual(state="ready", reason_codes=()),
        declared_project_state="archived",
        declared_project_evidence_ref_ids=("ev_01",),
    )
    canonical_evidence_ids = frozenset(item.evidence_ref_id for item in result.evidence)
    summary = render_declared_project_summary(result, canonical_evidence_ids)
    assert summary is not None
    assert "archived" not in summary.casefold()
    assert "outside the known vocabulary" in summary


def test_render_declared_project_summary_returns_none_when_evidence_is_not_canonical() -> (
    None
):
    """CHAOS-3368 Codex HIGH fix (delta review, 2026-08-04): a bound
    status_snapshot.v1 result with a real declared state, but whose
    evidence did NOT survive this run's canonical evidence set (a run-wide
    cap this function's caller controls, distinct from this one tool
    result's own evidence array), must render NO clause -- never an
    ungrounded assertion with no claim and no evidence behind it anywhere
    in the final answer.
    """

    result = _tool_result(
        actual_completion=_actual(state="ready", reason_codes=()),
        declared_project_state="started",
        declared_project_target_date="2026-09-01",
        declared_project_evidence_ref_ids=("ev_01",),
    )
    # Deliberately excludes "ev_01" -- simulates the run-wide canonical
    # evidence cap having truncated it out before this function ever runs.
    assert render_declared_project_summary(result, frozenset()) is None


@pytest.mark.parametrize(
    "raw_state", ["planned", "started", "paused", "completed", "canceled", "cancelled"]
)
def test_project_state_translation_covers_the_known_linear_vocabulary(
    raw_state: str,
) -> None:
    """Every state Linear's own API can report today translates to
    non-empty, deterministic copy -- not a fabricated pass-through of
    whatever the caller supplied, and not the fail-closed default (which is
    reserved for values this table has genuinely never seen).
    """

    translated = translate_project_state(raw_state)
    assert translated
    assert translated != "a declared state outside the known vocabulary"


def test_declared_project_state_translation_never_produces_a_denylisted_token() -> None:
    """CHAOS-3368 (Codex point 4, this fix round): the exact failure mode
    that bit CHAOS-3377's own renderer -- a legitimate value colliding with
    an internal denylisted token and flipping the answer to
    ``internal_error`` -- must not be reachable through this table. Every
    known translation, AND the fail-closed default, must be clear of every
    ``no_match_terminal.INTERNAL_TOKEN_DENYLIST`` member.
    """

    candidates = [
        translate_project_state(state)
        for state in ("planned", "started", "paused", "completed", "canceled")
    ] + [translate_project_state("some_future_provider_state")]
    for text in candidates:
        lowered = text.casefold()
        for token in INTERNAL_TOKEN_DENYLIST:
            assert token not in lowered, (
                f"translated declared-project-state copy {text!r} contains "
                f"the denylisted token {token!r}"
            )


def test_build_deterministic_status_claims_includes_declared_project_claim_when_grounded() -> (
    None
):
    """CHAOS-3368: the declared-state/target-date content reaches the §10
    claims list as its own grounded, translated claim -- not only folded
    into the verdict sentence.
    """

    actual = _actual(state="ready", reason_codes=())
    result = _tool_result(
        actual_completion=actual,
        declared_project_state="paused",
        declared_project_target_date="2026-12-25",
        declared_project_evidence_ref_ids=("ev_02",),
    )
    canonical_evidence_ids = frozenset(item.evidence_ref_id for item in result.evidence)
    claims = build_deterministic_status_claims(
        actual=actual,
        status_result=result,
        validity_scope=SCOPE,
        canonical_evidence_ids=canonical_evidence_ids,
    )
    declared_claims = [
        claim
        for claim in claims
        if claim.claim_id.startswith("status-declared-project:")
    ]
    assert len(declared_claims) == 1
    claim = declared_claims[0]
    assert "paused" in claim.text
    assert "2026-12-25" in claim.text
    assert claim.evidence_ref_ids == ["ev_02"]


def test_build_deterministic_status_claims_skips_declared_project_claim_when_evidence_is_truncated() -> (
    None
):
    """CHAOS-3368: mirrors ``test_blocker_claim_skipped_rather_than_fabricated_when_evidence_is_truncated``
    -- an outstanding declared-state clause whose own evidence was
    truncated out of ``canonical_evidence_ids`` must be omitted from claims
    entirely, never emitted ungrounded. The verdict/summary TEXT still
    names it (``render_declared_project_summary`` has no evidence
    dependency), but no claim backs it -- a caller wiring only claims would
    correctly see nothing here.
    """

    actual = _actual(state="ready", reason_codes=())
    result = _tool_result(
        actual_completion=actual,
        declared_project_state="paused",
        declared_project_evidence_ref_ids=("ev_02",),
    )
    claims = build_deterministic_status_claims(
        actual=actual,
        status_result=result,
        validity_scope=SCOPE,
        # Deliberately excludes "ev_02" -- simulates the fact that minted
        # it having been truncated out of this run's canonical evidence set.
        canonical_evidence_ids=frozenset({"ev_01"}),
    )
    assert not any(
        claim.claim_id.startswith("status-declared-project:") for claim in claims
    )


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
    status_result = _tool_result(actual_completion=actual)
    claims = build_deterministic_status_claims(
        actual=actual,
        status_result=status_result,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev1_" + "a" * 40}),
    )
    assert "ev1_" not in claims[0].text


# --- MEDIUM 4 (codex adversarial review): raw provider status strings must
# never be interpolated into claim text -- a provider-supplied status can
# coincidentally equal a denylisted internal token.


def test_blocker_claim_text_never_interpolates_the_raw_child_status() -> None:
    """Reviewer repro: a required child whose own status happens to equal a
    denylisted token ('not_ready') must not turn its claim text into a
    leak -- the fix is structural (never render raw status), not a denylist
    dodge.
    """

    actual = _actual(
        required_children=(
            _child("issue:OPEN-1", "release gate", "not_ready", ("ev_01",)),
        )
    )
    status_result = _tool_result(actual_completion=actual)
    claims = build_deterministic_status_claims(
        actual=actual,
        status_result=status_result,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev_01"}),
    )
    blocker_claims = [c for c in claims if c.claim_id.startswith("status-blocker:")]
    assert blocker_claims
    for claim in blocker_claims:
        assert "not_ready" not in claim.text


# --- defect 5 / HIGH 3: outstanding work must never contradict itself, and
# must be complete across every blocker-producing category ---


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


def test_open_required_children_uses_the_assessors_exact_predicate() -> None:
    """HIGH 3 fail->pass: a prior revision's own "safe superset" treated
    'resolved'/'merged' as closed for required children. The assessor
    (``status_change_service._assess``) does NOT -- it counts a required
    child with either status as INCOMPLETE. The renderer must agree, or a
    NOT_READY verdict can render a supposedly-closed item that the assessor
    itself still counted as the reason it is not ready.
    """

    actual = _actual(
        required_children=(
            _child("issue:RESOLVED-1", "Marked resolved", "resolved"),
            _child("issue:MERGED-1", "Marked merged", "merged"),
        )
    )
    open_ids = {child.fact_id for child in open_required_children(actual)}
    assert open_ids == {"issue:RESOLVED-1", "issue:MERGED-1"}
    # And the renderer's predicate is LITERALLY the assessor's, not a
    # separately-maintained copy that happens to agree today.
    assert is_required_child_open("resolved") is True
    assert is_required_child_open("merged") is True


def test_open_blockers_treats_resolved_as_closed_but_not_merged() -> None:
    """The blocker vocabulary is intentionally different from the required-
    child one (``status_change_service`` treats a blocker's own 'resolved'
    as closed, but not 'merged' -- pre-existing, tested behavior preserved
    by importing the exact predicate rather than re-deriving it).
    """

    actual = _actual(
        blockers=(
            _child("issue:BLOCKER-RESOLVED", "Blocker resolved", "resolved"),
            _child("issue:BLOCKER-MERGED", "Blocker merged", "merged"),
            _child("issue:BLOCKER-OPEN", "Blocker open", "open"),
        )
    )
    open_ids = {blocker.fact_id for blocker in open_blockers(actual)}
    assert open_ids == {"issue:BLOCKER-MERGED", "issue:BLOCKER-OPEN"}


@pytest.mark.parametrize(
    "status",
    ["complete", "completed", "DONE", "closed", "canceled", "cancelled"],
)
def test_is_required_child_open_closed_vocabulary(status: str) -> None:
    assert is_required_child_open(status) is False


@pytest.mark.parametrize("status", ["in_progress", "Open", "Blocked", "in_review"])
def test_is_required_child_open_open_vocabulary(status: str) -> None:
    assert is_required_child_open(status) is True


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
    status_result = _tool_result(actual_completion=actual)
    claims = build_deterministic_status_claims(
        actual=actual,
        status_result=status_result,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev_01"}),
    )
    assert all(
        claim.claim_id != "status-blocker:child:issue:OPEN-1" for claim in claims
    )


# --- HIGH 3 (codex adversarial review): every blocker-producing category
# must be able to independently explain a NOT_READY verdict -- one test per
# category as the SOLE cause, per the reviewer's request.


def test_open_blocker_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(
        reason_codes=("open_blocker",),
        blockers=(_child("b1", "Open blocker item", "open"),),
    )
    status_result = _tool_result(actual_completion=actual)
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "blocker:b1" for f in facts)


def test_unmerged_pull_request_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("required_pull_request_unmerged",))
    status_result = _tool_result(
        actual_completion=actual,
        pull_requests=(_pull_request("pr-1", required=True, merged=False),),
    )
    facts = outstanding_facts(actual, status_result)
    matching = [f for f in facts if f.claim_id_suffix == "pr-unmerged:pr-1"]
    assert len(matching) == 1
    assert "has not merged" in matching[0].text


def test_review_unresolved_pull_request_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("required_review_unresolved",))
    status_result = _tool_result(
        actual_completion=actual,
        pull_requests=(
            _pull_request("pr-2", required=True, merged=True, review_state=None),
        ),
    )
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "pr-review-unresolved:pr-2" for f in facts)


def test_changes_requested_pull_request_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("review_changes_requested",))
    status_result = _tool_result(
        actual_completion=actual,
        pull_requests=(
            _pull_request("pr-3", required=True, merged=True, changes_requested=2),
        ),
    )
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "pr-changes-requested:pr-3" for f in facts)


def test_ci_not_passing_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("required_ci_not_passing",))
    status_result = _tool_result(
        actual_completion=actual,
        ci_checks=(_ci("ci-1", required=True, conclusion="failure"),),
    )
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "ci-not-passing:ci-1" for f in facts)


def test_ci_work_skipped_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("required_ci_work_skipped",))
    status_result = _tool_result(
        actual_completion=actual,
        ci_checks=(_ci("ci-2", required=True, skipped_required_work=True),),
    )
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "ci-skipped:ci-2" for f in facts)


def test_deployment_not_succeeded_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("required_deployment_not_succeeded",))
    status_result = _tool_result(
        actual_completion=actual,
        deployments=(_deployment("dep-1", required=True, status="failed"),),
    )
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "deployment-not-succeeded:dep-1" for f in facts)


def test_active_blocking_incident_is_the_sole_cause_and_is_rendered() -> None:
    actual = _actual(reason_codes=("active_blocking_incident",))
    status_result = _tool_result(
        actual_completion=actual,
        incidents=(_incident("inc-1", active=True, blocking=True),),
    )
    facts = outstanding_facts(actual, status_result)
    assert any(f.claim_id_suffix == "incident:inc-1" for f in facts)


def test_outstanding_facts_never_interpolates_raw_status_fields() -> None:
    """None of the per-category phrases embed the fact's own raw
    status/conclusion string -- only its display_label and a fixed,
    translated phrase (MEDIUM 4).
    """

    actual = _actual(
        reason_codes=(
            "required_pull_request_unmerged",
            "required_ci_not_passing",
            "required_deployment_not_succeeded",
        )
    )
    status_result = _tool_result(
        actual_completion=actual,
        pull_requests=(_pull_request("pr-9", required=True, merged=False),),
        ci_checks=(_ci("ci-9", required=True, conclusion="not_ready"),),
        deployments=(_deployment("dep-9", required=True, status="not_ready"),),
    )
    facts = outstanding_facts(actual, status_result)
    for fact in facts:
        assert "not_ready" not in fact.text


def test_claims_built_across_multiple_categories_at_once() -> None:
    """A verdict caused by several categories at once gets a claim for
    EACH, not just the first one found.
    """

    actual = _actual(
        reason_codes=(
            "open_blocker",
            "required_pull_request_unmerged",
            "active_blocking_incident",
        ),
        blockers=(_child("b1", "Blocking item", "open", ("ev_01",)),),
    )
    status_result = _tool_result(
        actual_completion=actual,
        pull_requests=(
            _pull_request(
                "pr-1", required=True, merged=False, evidence_ref_ids=("ev_01",)
            ),
        ),
        incidents=(
            _incident("inc-1", active=True, blocking=True, evidence_ref_ids=("ev_01",)),
        ),
    )
    claims = build_deterministic_status_claims(
        actual=actual,
        status_result=status_result,
        validity_scope=SCOPE,
        canonical_evidence_ids=frozenset({"ev_01"}),
    )
    blocker_claim_ids = {
        c.claim_id for c in claims if c.claim_id.startswith("status-blocker:")
    }
    assert blocker_claim_ids == {
        "status-blocker:blocker:b1",
        "status-blocker:pr-unmerged:pr-1",
        "status-blocker:incident:inc-1",
    }


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


# --- HIGH 1 (codex adversarial review): the renderer must bind to the
# CURRENT resolved scope, never the first status_snapshot result seen ---


def test_status_snapshot_result_requires_a_status_snapshot_tool_id() -> None:
    other_tool_request = _tool_request(tool_id="query_metric.v1")
    result_with_actual = _tool_result(actual_completion=_actual())
    assert (
        status_snapshot_result(
            (other_tool_request,), (result_with_actual,), authorized_scope=SCOPE
        )
        is None
    )


def test_status_snapshot_result_requires_scope_to_match_the_final_authorized_scope() -> (
    None
):
    """Fail->pass for HIGH 1: a status_snapshot.v1 result called against a
    DIFFERENT scope than the run's final resolved scope must never be
    selected -- an earlier subject's snapshot must not silently overwrite a
    later, differently-scoped answer.
    """

    stale_request = _tool_request(scope=OTHER_SCOPE)
    stale_result = _tool_result(
        actual_completion=_actual(), tool_call_id="tool_call_01"
    )
    assert (
        status_snapshot_result(
            (stale_request,), (stale_result,), authorized_scope=SCOPE
        )
        is None
    )


def test_status_snapshot_result_selects_the_latest_match_for_the_current_scope() -> (
    None
):
    stale_request = _tool_request(
        scope=OTHER_SCOPE, tool_id="status_snapshot.v1", tool_call_id="tool_call_01"
    )
    stale_result = _tool_result(
        actual_completion=_actual(state="ready"), tool_call_id="tool_call_01"
    )
    current_request = _tool_request(
        scope=SCOPE, tool_id="status_snapshot.v1", tool_call_id="tool_call_02"
    )
    current_result = _tool_result(
        actual_completion=_actual(state="not_ready"), tool_call_id="tool_call_02"
    )
    selected = status_snapshot_result(
        (stale_request, current_request),
        (stale_result, current_result),
        authorized_scope=SCOPE,
    )
    assert selected is current_result
    assert selected is not None and selected.actual_completion is not None
    assert selected.actual_completion.state == "not_ready"


def test_status_snapshot_result_picks_the_last_of_two_matches_for_the_same_scope() -> (
    None
):
    first_request = _tool_request(scope=SCOPE, tool_call_id="tool_call_01")
    first_result = _tool_result(
        actual_completion=_actual(state="not_ready"), tool_call_id="tool_call_01"
    )
    second_request = _tool_request(scope=SCOPE, tool_call_id="tool_call_02")
    second_result = _tool_result(
        actual_completion=_actual(state="ready", reason_codes=()),
        tool_call_id="tool_call_02",
    )
    selected = status_snapshot_result(
        (first_request, second_request),
        (first_result, second_result),
        authorized_scope=SCOPE,
    )
    assert selected is second_result


# --- CHAOS-3297 s2 round 8 disclosure obligation still applies ---


def test_verdict_carries_the_disclosure_when_denominator_withheld() -> None:
    actual = _actual(required_child_total=None, required_child_complete=None)
    summary = render_verdict_summary(actual, denominator_withheld=True)
    assert (
        "the required-work completion total could not be fully verified"
        in summary.casefold()
    )
