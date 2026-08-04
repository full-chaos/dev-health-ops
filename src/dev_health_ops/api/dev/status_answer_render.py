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

This module is the fix: build the verdict sentence and the blocker list
directly from ``DevActualCompletion``, using closed, fail-closed translation
tables, so a STATUS-class answer's §10 content is server-rendered rather than
model-narrated. ``orchestrator.py`` calls this to OVERWRITE the model's
``status``/``direct_summary``/``claims`` for a run whose tool results include
an ``actual_completion`` assessment -- the model's own prose for that content
never reaches the wire; the deterministic sections cannot be authored,
contradicted, or polluted by it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .contracts import (
    AnswerStatus,
    ClaimKind,
    DevActualCompletion,
    DevClaim,
    DevClaimFlags,
    DevCoverage,
    DevRequiredChildFact,
    DevScope,
    DevToolResult,
)
from .status_completion_copy import (
    INCOMPLETE_DENOMINATOR_DISCLOSURE,
    any_tool_result_withheld_its_completion_denominator,
    translate_reason_code,
)

__all__ = [
    "build_deterministic_status_claims",
    "deterministic_answer_status",
    "is_open_child_status",
    "open_required_children",
    "render_verdict_summary",
    "status_snapshot_result",
    "translate_completion_state",
    "translate_reason_code",
]

#: Closed, total vocabulary for ``DevActualCompletion.state`` (the wire type
#: is itself a 3-member ``Literal``, so this dict is exhaustive by
#: construction -- ``test_status_answer_render.py`` pins totality against
#: that ``Literal``'s own ``get_args``).
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

#: A required child (or blocker -- both are wired onto ``required_children``
#: as ``DevRequiredChildFact`` by ``production_runtime.status_snapshot``'s
#: ``actual_completion`` construction) is CLOSED when its own ``status``
#: reads as done. Mirrors the predicate ``status_change_service._assess``
#: already uses to decide ``required_child_incomplete``/``open_blocker``
#: (that function's own two closed-vocabulary sets, unioned): a status this
#: module would call "open" can never be one ``_assess`` itself called
#: "complete" for the *same* reason-code decision, and vice versa -- the two
#: cannot disagree about which items are still outstanding.
_CLOSED_CHILD_STATUS_TOKENS: frozenset[str] = frozenset(
    {
        "complete",
        "completed",
        "done",
        "closed",
        "canceled",
        "cancelled",
        "resolved",
        "merged",
    }
)


def translate_completion_state(state: str) -> str:
    """The safe, closed-vocabulary sentence for a raw ``ActualCompletion.state``.

    Fail-closed: an unrecognized state (never possible for a validated
    ``DevActualCompletion`` today, since the wire field is itself a 3-member
    ``Literal``, but kept total rather than partial so a future wire
    relaxation cannot reintroduce a raw-token leak here) renders as the
    generic ``_DEFAULT_STATE_COPY``, never the input string.
    """

    return _COMPLETION_STATE_COPY.get(state, _DEFAULT_STATE_COPY)


def is_open_child_status(status: str) -> bool:
    """Whether a required-child/blocker's own ``status`` reads as outstanding."""

    return status.strip().casefold() not in _CLOSED_CHILD_STATUS_TOKENS


def open_required_children(
    actual: DevActualCompletion,
) -> list[DevRequiredChildFact]:
    """The frame's own prioritized blocker list (CHAOS-3377 defect 5).

    Every ``DevRequiredChildFact`` on ``actual.required_children`` whose own
    ``status`` field is NOT one of the closed-vocabulary "done" tokens.
    Built entirely from the server-computed ``status`` field on each fact --
    never from narrative -- so an item the frame itself marked
    complete/done/closed/etc can never appear here, closing the exact
    self-contradiction (a "completed" item listed under "Current blockers")
    the ticket reported. Order is preserved from ``required_children``,
    which ``status_change_service._ordered_status`` already produces in a
    stable, deterministic priority order.
    """

    return [
        child
        for child in actual.required_children
        if is_open_child_status(child.status)
    ]


def render_verdict_summary(
    actual: DevActualCompletion, *, denominator_withheld: bool = False
) -> str:
    """One deterministic sentence describing the completion verdict.

    Never the raw ``state``/``reason_codes`` tokens (PRD §12) -- every
    dynamic piece is either a plain integer (the completion fraction) or
    routed through the closed-vocabulary translation tables above.

    ``denominator_withheld`` mirrors ``answer_validator``'s own
    ``any_tool_result_withheld_its_completion_denominator`` /
    ``INCOMPLETE_DENOMINATOR_DISCLOSURE`` positive-disclosure obligation
    (CHAOS-3297 s2 round 8): when the required-child source was truncated,
    every user-visible piece of completion language -- deterministic or
    model-authored -- must carry that exact sentence, so a server-rendered
    verdict is not exempt from the same honesty requirement a model-authored
    one is held to.
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
    return " ".join(parts)


def status_snapshot_result(
    tool_results: Sequence[DevToolResult],
) -> DevToolResult | None:
    """The first executed tool result carrying a completion assessment, if any.

    A run may call more than one tool; this is the seam
    ``orchestrator.py`` uses to decide whether THIS run has server-owned §10
    material to render deterministically at all -- a non-STATUS question
    (whose tool results never set ``actual_completion``) is untouched by this
    module.
    """

    for result in tool_results:
        if result.actual_completion is not None:
            return result
    return None


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


def build_deterministic_status_claims(
    *,
    actual: DevActualCompletion,
    validity_scope: DevScope,
    canonical_evidence_ids: frozenset[str],
    tool_results: Sequence[DevToolResult] = (),
) -> list[DevClaim]:
    """Server-rendered §10 claims: one verdict claim, then one per open
    required item/blocker -- never from model narrative.

    Every claim cites only evidence IDs already present in this run's
    canonical evidence set (``canonical_evidence_ids``, the same tuple
    ``Orchestrator._canonical_answer_data`` computed for this candidate), so
    the result satisfies ``DevAnswer``'s own "claim references unknown
    evidence" invariant by construction. A blocker whose own evidence was
    truncated out of the tool result is skipped rather than emitted
    ungrounded -- an ``OBSERVED`` claim requires at least one reference
    (``DevClaim.validate_grounding``), and fabricating one would be worse
    than omitting the item.

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
    for child in open_required_children(actual):
        evidence_ids = [
            ref for ref in child.evidence_ref_ids if ref in canonical_evidence_ids
        ][:25]
        if not evidence_ids:
            continue
        claims.append(
            DevClaim(
                schema_version="dev_claim.v1",
                claim_id=f"status-blocker:{child.fact_id}",
                kind=ClaimKind.OBSERVED,
                text=f"Blocked: {child.text} ({child.status}){disclosure_suffix}",
                confidence=1.0,
                evidence_ref_ids=evidence_ids,
                metric_ref_ids=[],
                validity_scope=validity_scope,
                flags=DevClaimFlags(),
            )
        )
    # DevAnswer.claims caps at 100 (contracts.py); required_children itself
    # is already capped at 100 and the verdict adds one more, so this bound
    # is defensive rather than expected to trim anything in practice.
    return claims[:100]
