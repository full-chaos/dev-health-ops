"""CHAOS-3377: the §10 deterministic status-answer renderer.

The live defect this closes: a project-status question's ``status_snapshot.v1``
tool call already computes a real, server-owned ``ActualCompletion`` verdict
(``DevToolResult.actual_completion``) -- state, the completion fraction, a
reason-code explanation, and a per-child status list -- but the FINAL
``dev_answer.v1`` the client renders never carried any of that structured
data. It only has free-text ``direct_summary``/``claims[].text``, which the
model authors by reading the same tool result and narrating it in prose. That
narration is where CHAOS-3377's five defects came from: the model can
self-declare ``status=refused`` over a fully assessed answer, it can quote the
tool result's raw internal tokens (``not_ready``, ``open_blocker``,
``ev1_...`` evidence ids) verbatim, and it can misclassify a completed
required item as a "current blocker".

This module is the fix: build the verdict sentence and the outstanding-work
list directly from ``DevActualCompletion`` (plus the sibling fact lists on
``DevToolResult`` -- pull requests, CI, deployments, incidents), using closed,
fail-closed translation tables and the SAME openness predicates
``status_change_service`` uses, so a STATUS-class answer's §10 content is
server-rendered rather than model-narrated. ``orchestrator.py`` calls this to
OVERWRITE the model's ``status``/``direct_summary``/``claims`` for a run whose
tool results include an ``actual_completion`` assessment bound to the run's
current resolved scope -- the model's own prose for that content never
reaches the wire; the deterministic sections cannot be authored, contradicted,
or polluted by it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .contracts import (
    AnswerStatus,
    ClaimKind,
    DevActualCompletion,
    DevCIFact,
    DevClaim,
    DevClaimFlags,
    DevCoverage,
    DevDeploymentFact,
    DevIncidentFact,
    DevPullRequestFact,
    DevRequiredChildFact,
    DevScope,
    DevToolRequest,
    DevToolResult,
    ToolID,
)
from .status_change_service import (
    is_blocker_open,
    is_ci_not_passing,
    is_ci_work_skipped,
    is_deployment_not_succeeded,
    is_incident_active_and_blocking,
    is_pull_request_changes_requested,
    is_pull_request_review_unresolved,
    is_pull_request_unmerged,
    is_required_child_open,
)
from .status_completion_copy import (
    INCOMPLETE_DENOMINATOR_DISCLOSURE,
    any_tool_result_withheld_its_completion_denominator,
    translate_project_state,
    translate_reason_code,
)

__all__ = [
    "DISPLAY_TRUNCATED_DISCLOSURE",
    "build_deterministic_status_claims",
    "deterministic_answer_status",
    "open_blockers",
    "open_required_children",
    "outstanding_facts",
    "render_declared_project_summary",
    "render_verdict_summary",
    "status_snapshot_result",
    "translate_completion_state",
    "translate_reason_code",
]

#: Closed, total vocabulary for ``DevActualCompletion.state`` (the wire type
#: is itself a 3-member ``Literal``, so this dict is exhaustive by
#: construction -- ``test_chaos_3377_status_answer_render.py`` pins totality
#: against that ``Literal``'s own ``get_args``).
_COMPLETION_STATE_COPY: Mapping[str, str] = {
    "ready": "Ready: every required item is complete.",
    "not_ready": "Not ready: required work is still open.",
    "indeterminate": "Indeterminate: completion could not be determined from the available evidence.",
}
_DEFAULT_STATE_COPY = "The completion state could not be determined."

#: Reason-code translation (``translate_reason_code``) is imported from
#: ``status_completion_copy.py`` above -- that module owns the closed-
#: vocabulary table and its totality check against ``STATUS_REASON_CODES``,
#: shared with ``answer_validator.completion_truncation_detail`` so the two
#: user-visible surfaces cannot render the reason codes two different ways
#: (or one raw and one translated).


def translate_completion_state(state: str) -> str:
    """The safe, closed-vocabulary sentence for a raw ``ActualCompletion.state``.

    Fail-closed: an unrecognized state (never possible for a validated
    ``DevActualCompletion`` today, since the wire field is itself a 3-member
    ``Literal``, but kept total rather than partial so a future wire
    relaxation cannot reintroduce a raw-token leak here) renders as the
    generic ``_DEFAULT_STATE_COPY``, never the input string.
    """

    return _COMPLETION_STATE_COPY.get(state, _DEFAULT_STATE_COPY)


def open_required_children(
    actual: DevActualCompletion,
) -> list[DevRequiredChildFact]:
    """Required children whose own ``status`` reads as outstanding.

    Uses ``status_change_service.is_required_child_open`` -- the SAME
    predicate ``_assess`` itself calls to decide ``required_child_incomplete``
    -- rather than a locally re-derived copy (CHAOS-3377 HIGH 3, codex
    adversarial review: a prior revision's own "safe superset" of closed
    tokens disagreed with the assessor, treating ``resolved``/``merged`` as
    closed when ``_assess`` still counted them as the reason a verdict was
    NOT_READY). Importing the one predicate both call makes that class of
    disagreement structurally impossible, not just less likely.
    """

    return [
        child
        for child in actual.required_children
        if is_required_child_open(child.status)
    ]


def open_blockers(actual: DevActualCompletion) -> list[DevRequiredChildFact]:
    """Blockers (``open_blocker``) whose own ``status`` reads as outstanding.

    CHAOS-3377 HIGH 3: blockers previously had no typed wire representation
    at all distinct from ``required_children`` -- a NOT_READY verdict caused
    solely by an open blocker (no incomplete required child) rendered an
    EMPTY outstanding-work list, silently omitting the exact work making it
    not ready. ``DevActualCompletion.blockers`` (added alongside this fix)
    closes that gap; this mirrors ``open_required_children`` but against the
    blocker-specific closed-vocabulary predicate (``is_blocker_open``, which
    treats ``resolved`` as closed and ``required_child``'s predicate does
    not -- see ``status_change_service``'s own comment on why the two
    vocabularies are intentionally different).
    """

    return [blocker for blocker in actual.blockers if is_blocker_open(blocker.status)]


#: CHAOS-3377 hotfix: disclosed whenever ``actual.display_truncated`` is
#: True -- ``status_change_service.status_snapshot`` bounds both
#: ``required_children`` and ``blockers`` to ``request.max_items`` (100) for
#: display, independent of ``required_child_total``/``required_child_complete``
#: (which always reflect the true, UNBOUNDED assessment -- see that
#: module). A real project can have more than 100 outstanding items (the
#: live acceptance probe found 155 blockers on one project); when that
#: happens, some of them are silently absent from ``open_required_children``/
#: ``open_blockers`` below, and that must never be silent -- "never a silent
#: cap", the same rule every other bounded list in this codebase follows.
DISPLAY_TRUNCATED_DISCLOSURE = (
    "some required items or blockers were not displayed because there were "
    "more than this answer's display limit"
)


def render_verdict_summary(
    actual: DevActualCompletion, *, denominator_withheld: bool = False
) -> str:
    """One deterministic sentence describing the completion verdict.

    Never the raw ``state``/``reason_codes`` tokens (PRD §12) -- every
    dynamic piece is either a plain integer (the completion fraction) or
    routed through the closed-vocabulary translation tables above.

    ``denominator_withheld`` mirrors ``status_completion_copy``'s own
    ``any_tool_result_withheld_its_completion_denominator`` /
    ``INCOMPLETE_DENOMINATOR_DISCLOSURE`` positive-disclosure obligation
    (CHAOS-3297 s2 round 8): when the required-child source was truncated,
    every user-visible piece of completion language -- deterministic or
    model-authored -- must carry that exact sentence, so a server-rendered
    verdict is not exempt from the same honesty requirement a model-authored
    one is held to.

    ``actual.display_truncated`` drives the same kind of positive
    disclosure for a DIFFERENT truncation: not a withheld denominator, but a
    real denominator whose full required-item/blocker LIST does not fit in
    one display page (CHAOS-3377 hotfix).
    """

    parts = [translate_completion_state(actual.state)]
    if (
        actual.required_child_total is not None
        and actual.required_child_complete is not None
    ):
        parts.append(
            f"{actual.required_child_complete} of {actual.required_child_total} "
            "required items are complete."
        )
    seen: set[str] = set()
    reasons: list[str] = []
    for code in actual.reason_codes:
        translated = translate_reason_code(code)
        if translated not in seen:
            seen.add(translated)
            reasons.append(translated)
    if reasons:
        parts.append("Open items: " + "; ".join(reasons) + ".")
    if denominator_withheld:
        parts.append(INCOMPLETE_DENOMINATOR_DISCLOSURE.capitalize() + ".")
    if actual.display_truncated:
        parts.append(DISPLAY_TRUNCATED_DISCLOSURE.capitalize() + ".")
    return " ".join(parts)


def _grounded_declared_project_evidence(
    status_result: DevToolResult, canonical_evidence_ids: frozenset[str]
) -> list[str]:
    """The declared-state/target-date evidence that actually survived this
    RUN's canonical evidence set (not merely this one tool result's own,
    per-call evidence budget) -- the single computation both
    ``render_declared_project_summary`` and
    ``build_deterministic_status_claims`` read, so the summary clause and
    its backing claim can never disagree about whether this content is
    grounded.

    CHAOS-3368 Codex HIGH (2026-08-04, delta review of step 2): a prior
    revision rendered the summary clause unconditionally while the claim
    correctly required grounding -- in a run whose EARLIER tool results
    already filled ``Orchestrator._canonical_answer_data``'s run-wide
    25-entry cap, the declared-state fact's own evidence could be truncated
    out of ``canonical_evidence_ids`` there (a cap this function's caller
    does not control) while the per-tool-call priority reservation in
    ``production_runtime.py`` still let it survive onto the wire. The
    result: a summary sentence asserting a declared state with no claim and
    no evidence behind it anywhere in the answer -- an ungrounded assertion
    from the ONE renderer that exists to prevent exactly that class of
    defect. Treated the same as any other externally-sourced fact this
    renderer touches (mirrors ``outstanding_facts``'s "skip rather than
    fabricate" rule) rather than like the verdict sentence, which is
    server-DERIVED and therefore rendered regardless of evidence survival.
    """

    return [
        ref
        for ref in status_result.declared_project_evidence_ref_ids
        if ref in canonical_evidence_ids
    ][:25]


def render_declared_project_summary(
    status_result: DevToolResult, canonical_evidence_ids: frozenset[str]
) -> str | None:
    """The project's own declared state / target date (CHAOS-3368 step 2),
    as a deterministic clause appended to the §10 verdict/summary section --
    ``None`` when the bound ``status_snapshot.v1`` result has neither, OR
    when its evidence did not survive this run's canonical evidence set
    (see ``_grounded_declared_project_evidence`` -- this is never rendered
    ungrounded, exactly like a blocker/required-child claim).

    Reads ``DevToolResult.declared_project_state``/
    ``declared_project_target_date`` -- typed fields on the SAME
    scope-verified tool result ``status_snapshot_result`` already selected
    (see that function's own scope-binding guarantee), never a re-parse of
    the interim ``status_facts`` display text.

    ``declared_project_state`` is translated through
    ``status_completion_copy.translate_project_state`` -- NEVER interpolated
    raw -- mirroring the exact rule ``outstanding_facts`` documents for
    every other raw provider field this renderer touches (a provider string
    is unconstrained and can coincidentally equal an internal denylisted
    token). ``declared_project_target_date`` is a structured ISO date, not
    provider vocabulary, so it renders directly -- it cannot coincidentally
    collide with an underscore-bearing internal token.
    """

    if (
        status_result.declared_project_state is None
        and status_result.declared_project_target_date is None
    ):
        return None
    if not _grounded_declared_project_evidence(status_result, canonical_evidence_ids):
        return None
    parts: list[str] = []
    if status_result.declared_project_state is not None:
        parts.append(
            "Declared state: "
            + translate_project_state(status_result.declared_project_state)
            + "."
        )
    if status_result.declared_project_target_date is not None:
        parts.append(
            "Target date: "
            + status_result.declared_project_target_date.isoformat()
            + "."
        )
    return " ".join(parts)


def status_snapshot_result(
    tool_requests: Sequence[DevToolRequest],
    tool_results: Sequence[DevToolResult],
    *,
    authorized_scope: DevScope,
) -> DevToolResult | None:
    """The most recent ``status_snapshot.v1`` result bound to the run's
    CURRENT resolved scope, or ``None``.

    CHAOS-3377 HIGH 1 (codex adversarial review): a prior revision returned
    the FIRST tool result carrying ``actual_completion``, with no check that
    it was even a ``status_snapshot.v1`` call, let alone that its own request
    scope matched the answer being rendered. A run's authorized scope can
    change mid-run (a ``resolve_scope.v1`` commit narrows/widens it -- see
    ``Orchestrator.run``'s ``resolution``/``authorized_scope`` reassignment),
    and a model may call ``status_snapshot.v1`` more than once; an earlier
    snapshot for a DIFFERENT subject would silently overwrite the final
    answer's verdict/blockers with someone else's data -- confidently wrong
    scope, not just stale content.

    ``tool_requests``/``tool_results`` are the orchestrator's own
    index-aligned lists (appended together at the same call sites), so they
    are paired by position; ``tool_call_id`` is also cross-checked as a
    cheap integrity assertion. Filtered to ``STATUS_SNAPSHOT`` calls whose
    request scope equals ``authorized_scope`` -- the run's FINAL resolved
    scope, passed by the caller -- and the LAST such match wins (a repeated
    call for the same scope supersedes an earlier one). Ambiguity is never
    guessed at: a call whose scope does not match is simply excluded, never
    substituted.
    """

    if len(tool_requests) != len(tool_results):
        return None
    latest: DevToolResult | None = None
    for request, result in zip(tool_requests, tool_results, strict=True):
        if request.tool_id is not ToolID.STATUS_SNAPSHOT:
            continue
        if result.actual_completion is None:
            continue
        if request.tool_call_id != result.tool_call_id:
            continue
        if request.scope != authorized_scope:
            continue
        latest = result
    return latest


def deterministic_answer_status(
    *, coverage: DevCoverage, tool_results: Sequence[DevToolResult]
) -> AnswerStatus:
    """The answer status for a server-rendered §10 answer -- never REFUSED.

    Mirrors ``Orchestrator._server_grounded_answer``'s own coverage-driven
    status choice: DEGRADED if any executed tool came back unavailable/error,
    COMPLETE only when the server's own coverage accounting shows every
    required source fresh and available (the same invariant
    ``DevAnswer.validate_answer_invariants`` enforces), PARTIAL otherwise.
    A deterministic, evidence-grounded verdict is by construction not a
    refusal -- Ask Dev looked, and reports what it found -- so REFUSED is not
    a reachable output of this function.
    """

    degraded = any(result.status in {"unavailable", "error"} for result in tool_results)
    if degraded:
        return AnswerStatus.DEGRADED
    fully_covered = (
        coverage.available_source_count == coverage.required_source_count
        and not coverage.unavailable_required_sources
        and not coverage.stale_required_sources
        and not coverage.degraded_required_sources
    )
    return AnswerStatus.COMPLETE if fully_covered else AnswerStatus.PARTIAL


@dataclass(frozen=True, slots=True)
class _OutstandingFact:
    """One typed piece of "why not ready" content, from any blocker-producing
    category, before it becomes a ``DevClaim``. See ``outstanding_facts``.
    """

    claim_id_suffix: str
    text: str
    evidence_ref_ids: tuple[str, ...]


def _pull_request_facts(
    pull_requests: Sequence[DevPullRequestFact],
) -> list[_OutstandingFact]:
    facts: list[_OutstandingFact] = []
    for pr in pull_requests:
        if is_pull_request_unmerged(required=pr.required, merged=pr.merged):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"pr-unmerged:{pr.entity_id}",
                    text=f"Blocked: pull request {pr.display_label} has not merged.",
                    evidence_ref_ids=tuple(pr.evidence_ref_ids),
                )
            )
        if is_pull_request_changes_requested(
            required=pr.required, changes_requested=pr.changes_requested
        ):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"pr-changes-requested:{pr.entity_id}",
                    text=(
                        f"Blocked: pull request {pr.display_label} has "
                        "requested changes outstanding."
                    ),
                    evidence_ref_ids=tuple(pr.evidence_ref_ids),
                )
            )
        elif is_pull_request_review_unresolved(
            required=pr.required, review_state=pr.review_state
        ):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"pr-review-unresolved:{pr.entity_id}",
                    text=f"Blocked: pull request {pr.display_label} awaits review.",
                    evidence_ref_ids=tuple(pr.evidence_ref_ids),
                )
            )
    return facts


def _ci_facts(ci_checks: Sequence[DevCIFact]) -> list[_OutstandingFact]:
    facts: list[_OutstandingFact] = []
    for ci in ci_checks:
        if is_ci_work_skipped(
            required=ci.required, skipped_required_work=ci.skipped_required_work
        ):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"ci-skipped:{ci.entity_id}",
                    text=f"Blocked: required CI work was skipped for {ci.display_label}.",
                    evidence_ref_ids=tuple(ci.evidence_ref_ids),
                )
            )
        elif is_ci_not_passing(required=ci.required, conclusion=ci.conclusion):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"ci-not-passing:{ci.entity_id}",
                    text=f"Blocked: CI check {ci.display_label} is not passing.",
                    evidence_ref_ids=tuple(ci.evidence_ref_ids),
                )
            )
    return facts


def _deployment_facts(
    deployments: Sequence[DevDeploymentFact],
) -> list[_OutstandingFact]:
    facts: list[_OutstandingFact] = []
    for deployment in deployments:
        if is_deployment_not_succeeded(
            required=deployment.required, status=deployment.status
        ):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"deployment-not-succeeded:{deployment.entity_id}",
                    text=f"Blocked: deployment {deployment.display_label} has not succeeded.",
                    evidence_ref_ids=tuple(deployment.evidence_ref_ids),
                )
            )
    return facts


def _incident_facts(incidents: Sequence[DevIncidentFact]) -> list[_OutstandingFact]:
    facts: list[_OutstandingFact] = []
    for incident in incidents:
        if is_incident_active_and_blocking(
            active=incident.active, blocking=incident.blocking
        ):
            facts.append(
                _OutstandingFact(
                    claim_id_suffix=f"incident:{incident.entity_id}",
                    text=(
                        f"Blocked: active incident {incident.display_label} "
                        "is blocking this work."
                    ),
                    evidence_ref_ids=tuple(incident.evidence_ref_ids),
                )
            )
    return facts


def _deduplicated_facts(
    facts: Sequence[_OutstandingFact],
) -> list[_OutstandingFact]:
    """Collapse outstanding facts to one per (category, entity), first
    occurrence wins.

    CHAOS-3377 residual defect (live acceptance probe, 2026-08-04): a single
    project-scope blocker with ``blocks`` edges to several blocked issues IN
    SCOPE produces one row PER EDGE from ``_BLOCKERS_SQL``
    (``native_status_change.py``) -- nothing between that query and here
    (``status_change_service._ordered_status`` only sorts; it never
    dedupes) collapses those rows back to one per blocker entity. The live
    answer rendered the identical "Blocked: ..." claim 5 times, each citing
    the SAME evidence ref, for a single blocker that happened to block 5
    in-scope issues.

    ``claim_id_suffix`` is already ``f"{category}:{entity_or_fact_id}"``
    (see ``outstanding_facts`` below) for every category this renderer
    knows about, so it is exactly the (category, entity) key without
    re-deriving either half -- and de-duplicating on it here closes the gap
    regardless of WHICH upstream category produced the repeat (an edge-count
    artifact today, any other duplicate source tomorrow), not just the one
    arm the live probe happened to exercise.

    Order is preserved and deterministic: every category's own fact list is
    already produced in a fixed, sorted order (``status_change_service``'s
    ``_ordered_status``/``_pr_key``/etc.), and categories are always visited
    in the same fixed sequence below -- so "first occurrence wins" always
    picks the same one across identical inputs.

    A dedup key is scoped to ONE category by construction (the suffix
    always starts with that category's own prefix), so the SAME entity
    appearing in two DIFFERENT categories -- e.g. both a required child AND
    a blocker, which are independently meaningful (CHAOS-3377 HIGH 3) --
    still yields two distinct claims, never collapsed into one.
    """

    seen: set[str] = set()
    deduplicated: list[_OutstandingFact] = []
    for fact in facts:
        if fact.claim_id_suffix in seen:
            continue
        seen.add(fact.claim_id_suffix)
        deduplicated.append(fact)
    return deduplicated


def outstanding_facts(
    actual: DevActualCompletion, status_result: DevToolResult
) -> list[_OutstandingFact]:
    """Every typed "why not ready" fact across ALL blocker-producing
    categories (CHAOS-3377 HIGH 3), never only required children.

    A NOT_READY verdict can be caused by an open blocker, an unmerged/
    unreviewed/changes-requested pull request, failing/skipped required CI,
    an undeployed release, or an active blocking incident -- ``_assess``
    (``status_change_service.py``) computes a reason code for every one of
    these independently. A blocker list built from ``required_children``
    alone can be completely empty while the verdict itself is NOT_READY,
    silently omitting the exact work making it so. This reads the same
    typed fact lists ``_assess`` reads (``DevToolResult.pull_requests``/
    ``.ci_checks``/``.deployments``/``.incidents``, plus
    ``actual.required_children``/``.blockers``) and applies the exact same
    predicates (imported from ``status_change_service``, never re-derived)
    to each.

    Never interpolates a fact's own raw ``status``/``conclusion``/etc.
    string into claim text (CHAOS-3377 MEDIUM 4, codex adversarial review: a
    provider-supplied status string is unconstrained and can coincidentally
    equal an internal denylisted token -- e.g. a literal ``not_ready`` --
    which would make ``orchestrator.finish()``'s fail-closed token scan
    convert an otherwise-valid answer into ``internal_error``). Every phrase
    here is a fixed, closed-vocabulary sentence naming only the fact's
    ``display_label`` (an identifying name, already rendered as safe content
    elsewhere in this contract, e.g. ``DevEvidenceRef.display_label``) --
    never its raw status field.
    """

    facts: list[_OutstandingFact] = []
    for child in open_required_children(actual):
        facts.append(
            _OutstandingFact(
                claim_id_suffix=f"child:{child.fact_id}",
                text=f"Blocked: {child.text}.",
                evidence_ref_ids=tuple(child.evidence_ref_ids),
            )
        )
    for blocker in open_blockers(actual):
        facts.append(
            _OutstandingFact(
                claim_id_suffix=f"blocker:{blocker.fact_id}",
                text=f"Blocked: {blocker.text}.",
                evidence_ref_ids=tuple(blocker.evidence_ref_ids),
            )
        )
    facts.extend(_pull_request_facts(status_result.pull_requests))
    facts.extend(_ci_facts(status_result.ci_checks))
    facts.extend(_deployment_facts(status_result.deployments))
    facts.extend(_incident_facts(status_result.incidents))
    return _deduplicated_facts(facts)


def build_deterministic_status_claims(
    *,
    actual: DevActualCompletion,
    status_result: DevToolResult,
    validity_scope: DevScope,
    canonical_evidence_ids: frozenset[str],
    tool_results: Sequence[DevToolResult] = (),
) -> list[DevClaim]:
    """Server-rendered §10 claims: one verdict claim, then one per
    outstanding fact across every blocker-producing category -- never from
    model narrative.

    Every claim cites only evidence IDs already present in this run's
    canonical evidence set (``canonical_evidence_ids``, the same tuple
    ``Orchestrator._canonical_answer_data`` computed for this candidate), so
    the result satisfies ``DevAnswer``'s own "claim references unknown
    evidence" invariant by construction. An outstanding fact whose own
    evidence was truncated out of the tool result is skipped rather than
    emitted ungrounded -- an ``OBSERVED`` claim requires at least one
    reference (``DevClaim.validate_grounding``), and fabricating one would be
    worse than omitting the item.

    ``tool_results`` (defaulting to empty, i.e. "denominator not withheld")
    drives the same positive-disclosure obligation ``render_verdict_summary``
    documents -- applied to every claim here, not only the verdict one, since
    ``answer_validator`` checks each claim independently.
    """

    denominator_withheld = any_tool_result_withheld_its_completion_denominator(
        tuple(tool_results)
    )
    disclosure_suffix = (
        f" {INCOMPLETE_DENOMINATOR_DISCLOSURE.capitalize()}."
        if denominator_withheld
        else ""
    )
    claims: list[DevClaim] = []
    verdict_evidence = [
        ref for ref in actual.evidence_ref_ids if ref in canonical_evidence_ids
    ][:25]
    claims.append(
        DevClaim(
            schema_version="dev_claim.v1",
            claim_id=f"status-verdict:{actual.rule_id}:{actual.rule_version}",
            kind=ClaimKind.OBSERVED if verdict_evidence else ClaimKind.INFERRED,
            text=render_verdict_summary(
                actual, denominator_withheld=denominator_withheld
            ),
            confidence=1.0 if verdict_evidence else 0.999999,
            evidence_ref_ids=verdict_evidence,
            metric_ref_ids=[],
            validity_scope=validity_scope,
            flags=DevClaimFlags(),
        )
    )
    # CHAOS-3368 step 2 (Codex HIGH, delta review): both the CLAIM here and
    # the SUMMARY clause (``render_declared_project_summary``, called by
    # ``orchestrator._deterministic_status_render`` against this exact same
    # ``canonical_evidence_ids``) read ``_grounded_declared_project_evidence``
    # -- the identical computation -- so the two can never disagree about
    # whether this content is grounded. Never folded into
    # ``actual.evidence_ref_ids`` (the declared state is deliberately never
    # an input to ``_assess``'s completion verdict; see
    # ``RawStatusSnapshot.declared_project_state``).
    declared_project_evidence = _grounded_declared_project_evidence(
        status_result, canonical_evidence_ids
    )
    if declared_project_evidence:
        declared_project_summary = render_declared_project_summary(
            status_result, canonical_evidence_ids
        )
        # Cannot be None here: the same grounding check that made
        # ``declared_project_evidence`` non-empty is the only evidence-based
        # gate ``render_declared_project_summary`` applies; the remaining
        # gate (state/target_date both absent) would have kept
        # ``declared_project_evidence_ref_ids`` empty in the first place
        # (production_runtime.py only ever mints declared-state evidence
        # alongside a non-None state/target_date).
        if declared_project_summary is not None:
            claims.append(
                DevClaim(
                    schema_version="dev_claim.v1",
                    claim_id=f"status-declared-project:{status_result.tool_call_id}",
                    kind=ClaimKind.OBSERVED,
                    text=declared_project_summary,
                    confidence=1.0,
                    evidence_ref_ids=declared_project_evidence,
                    metric_ref_ids=[],
                    validity_scope=validity_scope,
                    flags=DevClaimFlags(),
                )
            )
    for fact in outstanding_facts(actual, status_result):
        evidence_ids = [
            ref for ref in fact.evidence_ref_ids if ref in canonical_evidence_ids
        ][:25]
        if not evidence_ids:
            continue
        claims.append(
            DevClaim(
                schema_version="dev_claim.v1",
                claim_id=f"status-blocker:{fact.claim_id_suffix}",
                kind=ClaimKind.OBSERVED,
                text=f"{fact.text}{disclosure_suffix}",
                confidence=1.0,
                evidence_ref_ids=evidence_ids,
                metric_ref_ids=[],
                validity_scope=validity_scope,
                flags=DevClaimFlags(),
            )
        )
    # DevAnswer.claims caps at 100 (contracts.py); each contributing list is
    # independently capped upstream (required_children/blockers at 100,
    # pull_requests/ci_checks/deployments/incidents at 100 each on
    # DevToolResult), so this bound is defensive rather than expected to
    # trim anything in practice.
    return claims[:100]
