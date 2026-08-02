"""The bounded plan executor (CHAOS-3295 Amendment TRD v2 §5).

Runs a :class:`~.plan_documents`'s ``DevInvestigationPlan`` deterministically:
independent steps concurrently, dependent steps blocked until every
prerequisite *succeeds*, and a prerequisite that never completes stops only
the steps that actually depend on it -- never the whole plan. This module
never talks to a provider and never chooses which steps to run: that is
entirely the plan document plus each step's own ``applicable`` predicate
(:mod:`.steps`).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from ..contracts_v2.base import SourceClass, SourceRequirementState
from ..contracts_v2.plan import DevInvestigationPlan, DevSourceRequirement
from ..contracts_v2.result import DevInvestigationResult, DevSourceObservation
from .steps import (
    PlanRegistryError,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)

__all__ = ["PlanExecutionError", "PlanExecutor"]

#: Fixed namespace for every CHAOS-3295-minted id. A constant, not a secret --
#: it exists only so ``uuid5`` output cannot collide with a UUID minted by an
#: unrelated namespace elsewhere in the platform. Determinism (P3, CHAOS-3297
#: dependency): the same ``(run_id, plan_id[, step_id])`` triple always mints
#: the same handle, so two executions over identical inputs produce
#: byte-identical ``DevInvestigationResult`` objects -- required for 3297's
#: frame-purity property (F2) and for any differential oracle over the
#: builder.
_MINT_NAMESPACE = uuid.UUID("f91838e6-6d11-5c43-8fb6-10d72f647684")


def _mint(*parts: str) -> str:
    return str(uuid.uuid5(_MINT_NAMESPACE, ":".join(parts)))


class PlanExecutionError(RuntimeError):
    """Raised only for a structural defect the registry validator should
    already have caught (e.g. a dependency cycle surviving construction).
    Never raised for an ordinary step failure -- those are recorded, not
    thrown.
    """


@dataclass(slots=True)
class _Attempt:
    step_id: str
    outcome: StepOutcome | None
    failed: bool


class PlanExecutor:
    def __init__(
        self,
        *,
        registry: StepRegistry,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._registry = registry
        self._now = now

    async def run(
        self,
        *,
        plan: DevInvestigationPlan,
        context: StepContext,
        run_id: str,
        subject_entity_id: str | None = None,
        subject_set_fingerprint: str | None = None,
    ) -> DevInvestigationResult:
        registered = self._registry.for_plan(plan.plan_id)
        known_steps = set(plan.mandatory_steps) | set(plan.conditional_steps)
        missing = known_steps - registered.keys()
        if missing:
            # Registry-construction validation (registry_validation.
            # validate_registry) should already reject this at import time;
            # this is a defensive belt, never the primary gate.
            raise PlanRegistryError(
                f"plan {plan.plan_id!r} declares unregistered steps: {sorted(missing)}"
            )

        applicable_conditional = {
            step_id
            for step_id in plan.conditional_steps
            if registered[step_id].applicable(context)
        }
        #: Conditional steps whose applicability predicate said "no" --
        #: distinct from ``blocked`` below (a mandatory or applicable-
        #: conditional step that never ran because a prerequisite failed).
        #: The two share ``skipped_steps`` on the wire (the contract does
        #: not distinguish them at the step level) but need different
        #: terminal source states: NOT_APPLICABLE vs UNAVAILABLE.
        not_applicable: set[str] = set(plan.conditional_steps) - applicable_conditional
        blocked: set[str] = set()
        runnable = set(plan.mandatory_steps) | applicable_conditional

        # Codex finding (MEDIUM, 2026-08-01): dependencies were previously
        # filtered to `runnable` here, which *deleted* the edge to any
        # conditional prerequisite whose applicability predicate said "no" --
        # a mandatory step depending on an inapplicable conditional gate then
        # saw no dependency at all and ran immediately, instead of being
        # blocked. Retain every declared edge unfiltered; `not_applicable`
        # (like `failed`/`blocked`) is already folded into `unresolved` and
        # `blocked_now` below, so an inapplicable prerequisite still blocks
        # its dependents correctly -- it is simply never itself scheduled.
        dependencies = {
            dep.step_id: set(dep.depends_on) for dep in plan.step_dependencies
        }
        remaining = {step_id: dependencies.get(step_id, set()) for step_id in runnable}
        completed: dict[str, _Attempt] = {}
        failed: set[str] = set()

        while remaining:
            unresolved = completed.keys() | failed | not_applicable | blocked
            ready = [
                step_id for step_id, deps in remaining.items() if deps <= unresolved
            ]
            if not ready:
                # A dependency cycle survived plan validation. Treat every
                # leftover step as blocked rather than looping forever or
                # raising mid-run -- the run still terminates with a correct,
                # if degraded, disclosed result.
                blocked |= set(remaining)
                break
            blocked_now = {
                step_id
                for step_id in ready
                if dependencies.get(step_id, set())
                & (failed | not_applicable | blocked)
            }
            blocked |= blocked_now
            runnable_now = [step_id for step_id in ready if step_id not in blocked_now]
            outcomes = await asyncio.gather(
                *(
                    self._run_one(registered[step_id], context, plan)
                    for step_id in runnable_now
                ),
                return_exceptions=True,
            )
            for step_id, outcome in zip(runnable_now, outcomes, strict=True):
                if isinstance(outcome, BaseException):
                    failed.add(step_id)
                    continue
                completed[step_id] = _Attempt(step_id, outcome, failed=False)
            for step_id in ready:
                remaining.pop(step_id, None)

        # One observation per *declared* source requirement, not per step
        # that happened to run: "every required source category has exactly
        # one terminal state" (acceptance criterion) means a conditional step
        # that was inapplicable, or one blocked because its prerequisite
        # failed, still needs a typed terminal observation -- silently
        # omitting it is exactly the "the model cannot claim a source was
        # checked" gap this plan closes, from the other direction.
        steps_by_source: dict[tuple[SourceClass, str], list[str]] = {}
        for step_id, definition in registered.items():
            steps_by_source.setdefault(
                (definition.source_class, definition.adapter_id), []
            ).append(step_id)

        observations = tuple(
            self._observation_for_requirement(
                requirement,
                sorted(
                    steps_by_source.get(
                        (requirement.source_class, requirement.adapter_id), ()
                    )
                ),
                completed=completed,
                failed=failed,
                not_applicable=not_applicable,
                blocked=blocked,
                run_id=run_id,
                plan_id=plan.plan_id,
            )
            for requirement in plan.source_requirements
        )

        return DevInvestigationResult(
            schema_version="dev_investigation_result.v1",
            result_id=_mint("result", run_id, plan.plan_id),
            plan_id=plan.plan_id,
            plan_version=plan.plan_version,
            # ``ServerHandle`` requires the canonical minted-UUID grammar,
            # which the orchestrator's own ``run_id`` string is not
            # guaranteed to satisfy (it is a correlation key, not a wire
            # contract) -- folded through the same deterministic mint as
            # every other id here. The DB-level correlation to the real run
            # is ``PersistenceRunRecorder``'s constructor-bound run UUID,
            # never this field.
            run_id=_mint("run", run_id),
            subject_set_fingerprint=subject_set_fingerprint,
            subject_entity_id=subject_entity_id,
            observations=observations,
            completed_steps=tuple(sorted(completed)),
            skipped_steps=tuple(sorted(not_applicable | blocked)),
            failed_steps=tuple(sorted(failed)),
            # 3296 populates DevSourceObservation.relationship_paths and owns
            # the actual closure check; this executor never claims a check it
            # did not run.
            relationship_closure_verified=False,
            completed_at=self._now(),
        )

    async def _run_one(
        self,
        definition: PlanStepDefinition,
        context: StepContext,
        plan: DevInvestigationPlan,
    ) -> StepOutcome:
        return await asyncio.wait_for(
            definition.run(context), timeout=plan.per_step_timeout_seconds
        )

    def _observation_for_requirement(
        self,
        requirement: DevSourceRequirement,
        step_ids: list[str],
        *,
        completed: dict[str, _Attempt],
        failed: set[str],
        not_applicable: set[str],
        blocked: set[str],
        run_id: str,
        plan_id: str,
    ) -> DevSourceObservation:
        # A deterministic representative step_id for this requirement, used
        # only to seed the observation's minted id -- ``step_ids`` is already
        # sorted by the caller, so this is stable across runs regardless of
        # dict/registration order. registry_validation.validate_registry
        # rejects a declared requirement with no matching registration before
        # any run reaches this branch; the requirement's own (source_class,
        # adapter_id) is folded into the fallback seed as defense in depth
        # (codex finding, MEDIUM, 2026-08-01) so two different unregistered
        # requirements in the same plan can never mint colliding observation
        # ids from an identical literal "unregistered" seed.
        identity_step_id = (
            step_ids[0]
            if step_ids
            else f"unregistered:{requirement.source_class.value}:{requirement.adapter_id}"
        )
        observation_id = _mint("observation", run_id, plan_id, identity_step_id)

        completed_step = next((s for s in step_ids if s in completed), None)
        if completed_step is not None:
            outcome = completed[completed_step].outcome
            assert outcome is not None
            return self._to_observation(requirement, outcome, observation_id)
        if step_ids and all(step_id in not_applicable for step_id in step_ids):
            return self._unmeasured_observation(
                requirement,
                SourceRequirementState.NOT_APPLICABLE,
                "step_not_applicable",
                observation_id,
            )
        if step_ids and any(step_id in failed for step_id in step_ids):
            return self._unmeasured_observation(
                requirement,
                SourceRequirementState.UNAVAILABLE,
                "step_execution_failed",
                observation_id,
            )
        if step_ids and any(step_id in blocked for step_id in step_ids):
            return self._unmeasured_observation(
                requirement,
                SourceRequirementState.UNAVAILABLE,
                "step_blocked_by_prerequisite",
                observation_id,
            )
        # No step is registered for this declared requirement at all.
        # registry_validation.validate_registry rejects this at import time;
        # this branch only guards a runtime plan built outside that path.
        return self._unmeasured_observation(
            requirement,
            SourceRequirementState.UNAVAILABLE,
            "step_unregistered",
            observation_id,
        )

    def _to_observation(
        self,
        requirement: DevSourceRequirement,
        outcome: StepOutcome,
        observation_id: str,
    ) -> DevSourceObservation:
        return DevSourceObservation(
            schema_version="dev_source_observation.v1",
            observation_id=observation_id,
            source_class=requirement.source_class,
            adapter_id=requirement.adapter_id,
            requirement_level=requirement.requirement_level,
            observed_state=outcome.observed_state,
            data_semantics=outcome.data_semantics,
            watermark=outcome.watermark,
            subject_coverage=outcome.subject_coverage,
            usable_fact_count=outcome.usable_fact_count,
            sample_count=outcome.sample_count,
            relationship_paths=(),
            evidence_ref_ids=(),
            limitation=outcome.limitation,
            observed_at=self._now(),
            query_version=outcome.query_version,
            content=outcome.content,
        )

    def _unmeasured_observation(
        self,
        requirement: DevSourceRequirement,
        state: SourceRequirementState,
        limitation: str,
        observation_id: str,
    ) -> DevSourceObservation:
        return DevSourceObservation(
            schema_version="dev_source_observation.v1",
            observation_id=observation_id,
            source_class=requirement.source_class,
            adapter_id=requirement.adapter_id,
            requirement_level=requirement.requirement_level,
            observed_state=state,
            data_semantics="not_measured",
            watermark=None,
            subject_coverage=0.0,
            usable_fact_count=0,
            sample_count=None,
            relationship_paths=(),
            evidence_ref_ids=(),
            limitation=limitation,
            observed_at=self._now(),
            query_version="unversioned",
            content=None,
        )
