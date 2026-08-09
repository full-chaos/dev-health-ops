"""The sweep: every case, both arms, both legs, through the real seam.

This module composes pieces that are each already built and tested --
dispositions, the per-case budget, the raw records, the two legs, and both
arm legs -- and adds the one thing none of them can have on their own: the
decision of what a given outcome MEANS.

Three rules govern that decision, and each exists because the alternative
produces a confident artifact from a broken sweep.

**Only a scored case carries verdicts.** Everything else records why it did
not score. ``ArmResult`` enforces this at construction, so a mistake here
raises rather than rendering.

**An arm that ran and declined is not a non-run.** ``ARM_DECLARED_GAP`` is a
result — how often the baseline must decline is one of the numbers the
comparison turns on — and it is only meaningful because the producer's
invocation is observed separately from its output.

**The seam verdict and the oracle verdict are independent columns.** A packet
can be seam-rejected and still be scored for subject, cohort and lineage
quality. Collapsing them would lose the distinction between "answered well
but bypassed canonical authority" and "answered badly", which is a central
CHAOS-3621 input: a post-3627 canonical-bypass rejection means the graph
cited authentic world evidence the native frame's canonical set does not
contain, and that is a measured architectural fact about the seam contract
rather than a defect to hack around.

The sweep never retries. A case that timed out, faulted or refused stands as
measured -- the first honest result is the result.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .budget import DEFAULT_PER_CASE_TIMEOUT_SECONDS, BudgetOutcome, hard_bound
from .dispositions import CaseDisposition
from .legs import LEG_B_NATIVE_LABEL, LegId
from .records import ArmResult, DimensionOutcome, InterpretationDisposition

__all__ = [
    "ArmAttempt",
    "classify",
    "interpretation_of",
    "score_payload",
]

#: Deterministic run ids, so a sweep re-run from the same tree produces the
#: same correlation keys and two artifacts diff cleanly.
RUN_NAMESPACE = uuid.UUID("3619dead-0000-4000-8000-000000000004")


def run_id_for(leg: LegId, case_id: str) -> str:
    """One run id per (leg, case). Distinct across legs on purpose.

    The seam rejects a packet whose trial run id names a different run, so
    sharing an id across legs would make the second leg's packets look like
    stale ones from the first.
    """

    return str(uuid.uuid5(RUN_NAMESPACE, f"{leg.value}:{case_id}"))


@dataclass(frozen=True, slots=True)
class ArmAttempt:
    """What one arm did on one case, before it is given a meaning.

    Deliberately separates ``invoked`` from ``payload``. A payload-less
    attempt where the arm was never invoked is a harness failure; a
    payload-less attempt where it WAS invoked is a capability result. Nothing
    downstream can tell them apart unless this carries both.
    """

    invoked: bool
    payload: dict[str, Any] | None
    refusal: str = ""
    fault: str = ""


def classify(
    attempt: ArmAttempt | None, budget: BudgetOutcome
) -> tuple[CaseDisposition, str]:
    """The meaning of one arm's outcome, as a disposition and a detail.

    Ordered deliberately. A timeout is decided BEFORE anything the arm
    reported, because a late arm may have produced something and using it
    would score a case already recorded as NOT RUN. A fault is decided before
    a refusal, because an arm that faulted has not made a capability
    statement and recording one for it would publish a defect as a boundary.
    """

    if budget.abandoned_thread:
        return CaseDisposition.NOT_RUN_TIMEOUT, budget.detail
    if budget.exceeded:
        return CaseDisposition.NOT_RUN_TIMEOUT, budget.detail
    if budget.fault is not None:
        return CaseDisposition.ARM_FAULT, budget.detail
    if attempt is None:
        return (
            CaseDisposition.NOT_RUN_PRECONDITION,
            "the leg produced no attempt at all, so nothing about this arm "
            "was observed on this case",
        )
    if not attempt.invoked:
        return (
            CaseDisposition.NOT_RUN_PRECONDITION,
            "the arm was never invoked; any gap recorded here would be a "
            "harness artefact wearing a capability result's clothes",
        )
    if attempt.fault:
        return CaseDisposition.ARM_FAULT, attempt.fault
    if attempt.payload is None:
        if attempt.refusal:
            return CaseDisposition.ARM_REFUSED, attempt.refusal
        return (
            CaseDisposition.ARM_DECLARED_GAP,
            "the arm ran and reported this run as unprojectable",
        )
    return CaseDisposition.SCORED, "packet emitted and scored"


def interpretation_of(
    interpretation: Any, derived_family: Any
) -> InterpretationDisposition:
    """The per-case interpreter outcome, for the native leg's records.

    Reads the floor from the interpreter module rather than accepting one,
    so a record can never state a floor the interpreter does not use.
    """

    from dev_health_ops.api.dev.question_interpreter import (
        FALLBACK_CONFIDENCE_FLOOR,
    )

    confidence = float(interpretation.intent.confidence)
    return InterpretationDisposition(
        intent_id=interpretation.intent.intent_id.value,
        confidence=confidence,
        below_fallback_floor=confidence < FALLBACK_CONFIDENCE_FLOOR,
        fallback_floor=float(FALLBACK_CONFIDENCE_FLOOR),
        derived_question_family=(
            derived_family.value if derived_family is not None else None
        ),
        # Always false, and recorded per row rather than asserted in prose:
        # production wires no classifier and neither does the trial.
        classifier_consulted=False,
    )


def score_payload(case_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Score one packet with the FROZEN oracles, and report every field.

    Returns plain data rather than the evaluation object, because the record
    is the artifact and rehydrating a dataclass on the way in would let a
    later change to that class silently alter what an old artifact means.
    """

    from dev_health_ops.api.dev.investigation_corpus.evaluate import evaluate_payload

    evaluation = evaluate_payload(case_id, payload)
    return {
        "contract_valid": evaluation.contract_valid,
        "contract_error": evaluation.contract_error,
        "outcome_permitted": evaluation.outcome_permitted,
        "outcome_detail": evaluation.outcome_detail,
        "is_clean": evaluation.is_clean,
        "authorization_summary": (
            evaluation.authorization.summary()
            if evaluation.authorization is not None
            else ""
        ),
        "dimension_outcomes": tuple(
            DimensionOutcome(
                dimension_id=result.dimension_id.value,
                verdict=result.verdict.value,
                detail=result.detail,
            )
            for result in evaluation.results
        ),
    }


def arm_result(
    *,
    arm_id: str,
    leg: LegId,
    case_id: str,
    attempt: ArmAttempt | None,
    budget: BudgetOutcome,
    shadow: dict[str, Any] | None = None,
    interpretation: InterpretationDisposition | None = None,
) -> ArmResult:
    """Assemble one row, scoring only when the disposition permits it.

    The scoring call sits INSIDE the ``SCORED`` branch rather than before the
    classification, so an unscored case cannot acquire verdicts by an
    ordering accident -- which is the shape in which a timed-out case becomes
    a column of failures.
    """

    disposition, detail = classify(attempt, budget)
    scored: dict[str, Any] = {}
    if disposition is CaseDisposition.SCORED:
        assert attempt is not None and attempt.payload is not None
        scored = score_payload(case_id, attempt.payload)
    return ArmResult(
        arm_id=arm_id,
        disposition=disposition.value,
        detail=detail,
        latency_ms=int(budget.elapsed_seconds * 1000),
        packet_emitted=bool(attempt and attempt.payload is not None),
        shadow=shadow,
        contract_valid=scored.get("contract_valid"),
        contract_error=scored.get("contract_error", ""),
        outcome_permitted=scored.get("outcome_permitted"),
        outcome_detail=scored.get("outcome_detail", ""),
        is_clean=scored.get("is_clean"),
        authorization_summary=scored.get("authorization_summary", ""),
        dimension_outcomes=scored.get("dimension_outcomes", ()),
        interpretation=interpretation,
        figure_label=(
            LEG_B_NATIVE_LABEL
            if leg is LegId.JOB_HELD_CONSTANT and arm_id == "native"
            else ""
        ),
    )


def bounded(
    call: Callable[[], Any],
    *,
    limit_seconds: float = DEFAULT_PER_CASE_TIMEOUT_SECONDS,
) -> BudgetOutcome:
    """Run one arm attempt under the runner's hard bound.

    ``hard_bound`` rather than ``enforce``: the graph arm drives FalkorDB
    over a connection with no socket timeout, so a wedged case would
    otherwise stall the whole sweep rather than being recorded as one
    NOT RUN row.
    """

    return hard_bound(call, limit_seconds=limit_seconds)


def in_worker_loop(coroutine_factory: Callable[[], Any]) -> Callable[[], Any]:
    """Adapt an async attempt to the thread-based bound.

    Each worker gets its OWN event loop. Sharing the caller's loop would
    defeat the bound entirely: the wedged coroutine would block the loop the
    runner itself needs in order to give up waiting.
    """

    def run() -> Any:
        return asyncio.run(coroutine_factory())

    return run


def deterministic_produced_at(cases: Sequence[Any]) -> datetime:
    """A fixed emission timestamp for the whole sweep.

    Wall-clock time in a packet makes two runs of the same tree differ in
    every row, so a diff between artifacts shows the clock rather than the
    measurement. The corpus pins its own trial clock; the sweep borrows it.
    """

    from dev_health_ops.api.dev.investigation_corpus import world

    return world.TRIAL_NOW
