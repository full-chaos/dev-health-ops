"""The plan-step extension API (CHAOS-3295).

This module is the seam CHAOS-3303/3304/3305 register their portfolio/
project-health/team-health/workload/operational-deficiency plan steps
through *without changing the orchestrator*: a new step type is a new
``PlanStepDefinition`` registered against :class:`StepRegistry`, keyed by
``(plan_id, step_id)``. Nothing in ``orchestrator.py`` or
:mod:`.executor` names a concrete step -- they only walk whatever the
registry holds for a given plan.

A step's ``run`` callable receives a :class:`StepContext` (the committed,
server-owned scope/identity for this run) and returns a
:class:`StepOutcome`: the raw ingredients the executor needs to build one
``dev_source_observation.v1`` -- never the observation itself, so the
executor remains the single place that mints observation IDs, enforces the
zero-vs-no-data contract, and appends immutable attempts.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime

from ..contracts import DevScope
from ..contracts_v2.base import SourceClass, SourceRequirementState
from ..contracts_v2.plan import RequirementLevel
from ..contracts_v2.result import DevSourceContent

__all__ = [
    "PlanRegistryError",
    "DuplicateStepError",
    "StepContext",
    "StepOutcome",
    "StepRunner",
    "PlanStepDefinition",
    "StepRegistry",
]


class PlanRegistryError(RuntimeError):
    """Base class for deterministic, safe plan/step registration failures."""


class DuplicateStepError(PlanRegistryError):
    pass


class UnknownStepError(PlanRegistryError):
    pass


@dataclass(frozen=True, slots=True)
class StepContext:
    """Everything a step is allowed to see. No provider, no raw request text."""

    org_id: str
    permission_fingerprint: str
    scope: DevScope
    run_id: str
    now: datetime
    #: Canonical metric IDs the caller explicitly requested (``dev_question_
    #: intent.v1.requested_metric_ids``). Only ``metric.comparison.v1``'s
    #: step reads this; every other plan ignores it.
    requested_metric_ids: tuple[str, ...] = ()
    #: CHAOS-3393: the several committed, per-subject ``DevScope``s a
    #: PLURAL_COHORT/ORGANIZATION_WIDE plan step batches over -- e.g.
    #: ``status.portfolio.v1``'s ``portfolio_status_evaluation`` step, which
    #: needs one committed ``DirectScope.PROJECT`` scope per project in the
    #: batch. ``scope`` above remains the single org-level authorized scope
    #: for every step (the executor's authorization-scope snapshot anchors
    #: on it, unchanged); this is purely additive, empty for every plan that
    #: does not read it -- mirrors ``requested_metric_ids``'s own posture.
    subject_set_scopes: tuple[DevScope, ...] = ()


@dataclass(frozen=True, slots=True)
class StepOutcome:
    """What a step handler reports back to the executor.

    ``observed_state``/``data_semantics``/``usable_fact_count`` are the raw
    ingredients of ``dev_source_observation.v1`` -- the executor is the only
    place that mints ``observation_id``, stamps ``observed_at``, and applies
    :meth:`DevSourceObservation.validate_zero_semantics`, so a step cannot
    construct a contract object that skips that check.
    """

    observed_state: SourceRequirementState
    data_semantics: str
    usable_fact_count: int
    subject_coverage: float = 1.0
    sample_count: int | None = None
    watermark: datetime | None = None
    limitation: str | None = None
    query_version: str = "unversioned"
    #: CHAOS-3295 (ratified 3297 dependency): the typed domain content this
    #: step actually found, keyed by the observation's own ``source_class``.
    #: ``None`` for an unmeasured outcome (the executor's
    #: ``DevSourceObservation.validate_content_semantics`` rejects content on
    #: an unmeasured state); may be an all-empty ``DevSourceContent`` for a
    #: queried-but-empty result -- that is still content, just none found.
    content: DevSourceContent | None = None


StepRunner = Callable[[StepContext], Awaitable[StepOutcome]]


@dataclass(frozen=True, slots=True)
class PlanStepDefinition:
    """One registered, runnable plan step.

    ``source_class``/``adapter_id``/``requirement_level`` mirror the plan
    document's own ``DevSourceRequirement`` for this step, so the executor
    can cross-check a step's registration against what the plan declared it
    would use (a step registered for a plan that never declared a matching
    source requirement is a registry-construction error, not a runtime one).
    """

    step_id: str
    plan_id: str
    source_class: SourceClass
    adapter_id: str
    requirement_level: RequirementLevel
    run: StepRunner
    applicable: Callable[[StepContext], bool] = field(default=lambda _ctx: True)


class StepRegistry:
    """Allowlist of runnable steps, keyed by ``(plan_id, step_id)``.

    Construction-time totality is proven by :func:`registry_validation.
    validate_registry`, not here -- this class only enforces that
    registration itself cannot silently overwrite an existing entry, which
    is the structural half of "registering a new step type requires no
    orchestrator changes": a collision is a loud, immediate error at import
    time, not a step quietly replaced at run time.
    """

    def __init__(self) -> None:
        self._steps: dict[tuple[str, str], PlanStepDefinition] = {}

    def register(self, definition: PlanStepDefinition) -> None:
        key = (definition.plan_id, definition.step_id)
        if key in self._steps:
            raise DuplicateStepError(
                f"step {definition.step_id!r} already registered for plan "
                f"{definition.plan_id!r}"
            )
        self._steps[key] = definition

    def get(self, plan_id: str, step_id: str) -> PlanStepDefinition:
        try:
            return self._steps[(plan_id, step_id)]
        except KeyError as exc:
            raise UnknownStepError(
                f"no step {step_id!r} registered for plan {plan_id!r}"
            ) from exc

    def for_plan(self, plan_id: str) -> Mapping[str, PlanStepDefinition]:
        return {
            step_id: definition
            for (owner, step_id), definition in self._steps.items()
            if owner == plan_id
        }

    def __contains__(self, key: tuple[str, str]) -> bool:
        return key in self._steps
