"""Registry-construction validation (CHAOS-3295 Amendment TRD v2 §4.3):

"Registry construction must fail on duplicate IDs, dependency cycles,
missing tools/services, impossible bounds, unknown source categories, or a
core question class with no plan."

Most of these are already enforced at the *contract* level by
``DevInvestigationPlan.validate_plan_invariants`` (duplicate/self-referencing
steps, unknown source classes -- ``SourceClass`` is a closed pydantic enum)
and ``DevSourceRequirement.validate_applicability``. What survives
construction of a single plan document but only shows up once every plan is
assembled into a registry is: a plan ID outside ``PLAN_REGISTRY``, a step a
plan declares but nothing registered to run, a multi-node dependency cycle
(the per-plan validator only rejects a step depending on itself), and a core
question class with no plan at all. This module is the one place that runs
those *cross-plan* checks, at import time, so a bad registration fails
loudly before any run reaches it -- never a silent no-op plan.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..contracts_v2.base import QuestionIntentID
from ..contracts_v2.plan import PLAN_REGISTRY, DevInvestigationPlan
from .steps import PlanRegistryError, StepRegistry

__all__ = ["validate_registry"]


class DuplicatePlanIDError(PlanRegistryError):
    pass


class UnknownPlanIDError(PlanRegistryError):
    pass


class MissingStepImplementationError(PlanRegistryError):
    pass


class DependencyCycleError(PlanRegistryError):
    pass


class MissingCorePlanError(PlanRegistryError):
    pass


def validate_registry(
    *,
    plans_by_intent: Mapping[QuestionIntentID, DevInvestigationPlan],
    steps: StepRegistry,
    core_intents: frozenset[QuestionIntentID],
) -> None:
    plan_ids = [plan.plan_id for plan in plans_by_intent.values()]
    if len(plan_ids) != len(set(plan_ids)):
        raise DuplicatePlanIDError("two plans in the registry share a plan_id")

    missing_core = core_intents - plans_by_intent.keys()
    if missing_core:
        raise MissingCorePlanError(
            f"core question class(es) with no registered plan: {sorted(missing_core)}"
        )

    for intent_id, plan in plans_by_intent.items():
        if plan.intent_id != intent_id:
            raise UnknownPlanIDError(
                f"plan {plan.plan_id!r} is keyed under {intent_id!r} but declares "
                f"intent_id {plan.intent_id!r}"
            )
        if plan.plan_id not in PLAN_REGISTRY:
            raise UnknownPlanIDError(
                f"plan_id {plan.plan_id!r} is not a member of PLAN_REGISTRY"
            )

        declared_steps = set(plan.mandatory_steps) | set(plan.conditional_steps)
        registered_steps = set(steps.for_plan(plan.plan_id))
        missing_tools = declared_steps - registered_steps
        if missing_tools:
            raise MissingStepImplementationError(
                f"plan {plan.plan_id!r} declares steps with no registered "
                f"implementation: {sorted(missing_tools)}"
            )
        # The inverse -- a registered step the plan never declares -- is not
        # an error: CHAOS-3303/3304/3305 register their own plans' steps
        # into the *same* shared StepRegistry instance, so it legitimately
        # holds entries for plan IDs other than the one being checked here
        # (``for_plan`` already scopes the comparison, so this is stated for
        # the reader, not enforced).

        _check_acyclic(plan)


def _check_acyclic(plan: DevInvestigationPlan) -> None:
    """Kahn's algorithm over every declared step, mandatory or conditional.

    ``DevInvestigationPlan.validate_plan_invariants`` already rejects a step
    depending on itself; it cannot see a longer cycle (A depends on B, B
    depends on A) because it only inspects one ``DevPlanStepDependency`` at a
    time. This is the check that catches that case.
    """

    all_steps = set(plan.mandatory_steps) | set(plan.conditional_steps)
    edges = {dep.step_id: set(dep.depends_on) for dep in plan.step_dependencies}
    in_progress: set[str] = set()
    resolved: set[str] = set()

    def visit(step_id: str) -> None:
        if step_id in resolved:
            return
        if step_id in in_progress:
            raise DependencyCycleError(
                f"plan {plan.plan_id!r} has a dependency cycle involving {step_id!r}"
            )
        in_progress.add(step_id)
        for dependency in edges.get(step_id, ()):
            visit(dependency)
        in_progress.discard(step_id)
        resolved.add(step_id)

    for step_id in all_steps:
        visit(step_id)
