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

from ..contracts_v2.base import QuestionIntentID, SourceClass
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


class StepRequirementMismatchError(PlanRegistryError):
    pass


class OrphanStepRegistrationError(PlanRegistryError):
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

    # Codex finding (MEDIUM, 2026-08-01, second re-check): the inverse-totality
    # check below used to raise as soon as it found one plan with an unmatched
    # requirement, from *inside* this loop. With two plans each carrying their
    # own unconsumed requirement, construction stopped at the first plan and
    # the second plan's identical defect was never reported at all. Every
    # unmatched requirement, across the *entire* registry traversal, is
    # collected here and raised once at the end so no plan's defect can hide
    # behind another plan's earlier one.
    unmatched_requirements_by_plan: list[tuple[str, tuple[SourceClass, str]]] = []

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
        registered_for_plan = steps.for_plan(plan.plan_id)
        registered_steps = set(registered_for_plan)
        missing_tools = declared_steps - registered_steps
        if missing_tools:
            raise MissingStepImplementationError(
                f"plan {plan.plan_id!r} declares steps with no registered "
                f"implementation: {sorted(missing_tools)}"
            )
        # A step registered under a *different* plan_id is not an error here:
        # CHAOS-3303/3304/3305 register their own plans' steps into the same
        # shared StepRegistry instance, and ``for_plan`` already scopes the
        # comparison to this plan_id. A step registered under *this* plan_id
        # but never declared in mandatory_steps/conditional_steps is an
        # orphan registration and is rejected below (codex finding, MEDIUM,
        # 2026-08-01 -- "reject same-plan extra registrations").
        orphan_steps = registered_steps - declared_steps
        if orphan_steps:
            raise OrphanStepRegistrationError(
                f"plan {plan.plan_id!r} has step(s) registered but never "
                f"declared in mandatory_steps/conditional_steps: "
                f"{sorted(orphan_steps)}"
            )

        # Codex finding (MEDIUM, 2026-08-01): the checks above only compared
        # step *names* -- nothing verified that a registered step's own
        # (source_class, adapter_id, requirement_level) actually corresponds
        # to a declared DevSourceRequirement. A step registered against the
        # wrong adapter previously passed validation and only failed later,
        # at run time, when the executor could not find *any* requirement
        # matching its (source_class, adapter_id) and minted two colliding
        # "unregistered"-seeded observation ids for the same run.
        declared_requirements = {
            (req.source_class, req.adapter_id): req.requirement_level
            for req in plan.source_requirements
        }
        # Two different steps registered against the *same* (source_class,
        # adapter_id) is itself a mismatch, even when that pair is declared:
        # exactly one step must claim each requirement, or the executor's
        # per-requirement observation loop cannot tell which step's outcome
        # is authoritative for it (this is precisely how the codex repro --
        # two step definitions both registered to the wrong, shared adapter
        # -- slipped past a membership-only check).
        consumed_requirement_keys: dict[tuple[SourceClass, str], str] = {}
        for step_id in sorted(declared_steps):
            definition = registered_for_plan[step_id]
            key = (definition.source_class, definition.adapter_id)
            expected_level = declared_requirements.get(key)
            if expected_level is None:
                raise StepRequirementMismatchError(
                    f"plan {plan.plan_id!r} step {step_id!r} is registered "
                    f"against (source_class={definition.source_class!r}, "
                    f"adapter_id={definition.adapter_id!r}), which is not a "
                    f"declared source_requirement of this plan"
                )
            if key in consumed_requirement_keys:
                raise StepRequirementMismatchError(
                    f"plan {plan.plan_id!r} steps "
                    f"{consumed_requirement_keys[key]!r} and {step_id!r} are "
                    f"both registered against the same source_requirement "
                    f"{key!r} -- exactly one step must match each declared "
                    f"requirement"
                )
            consumed_requirement_keys[key] = step_id
            declared_attribution = (
                "mandatory" if step_id in plan.mandatory_steps else "conditional"
            )
            if expected_level != declared_attribution:
                raise StepRequirementMismatchError(
                    f"plan {plan.plan_id!r} step {step_id!r} is declared "
                    f"{declared_attribution!r} in the plan's step lists, but "
                    f"its source_requirement {key!r} is {expected_level!r}"
                )
            if definition.requirement_level != expected_level:
                raise StepRequirementMismatchError(
                    f"plan {plan.plan_id!r} step {step_id!r}'s own registered "
                    f"requirement_level {definition.requirement_level!r} "
                    f"disagrees with its source_requirement's "
                    f"{expected_level!r}"
                )

        # Codex finding (MEDIUM, 2026-08-01, re-check): the loop above only
        # verified the forward direction (every registered step matches a
        # declared requirement) -- never the inverse. A plan can declare a
        # mandatory source_requirement no registered step consumes; that
        # passed validate_registry and only surfaced at run time as a
        # silent UNAVAILABLE/"step_unregistered" observation instead of
        # failing at construction, where a requirement with no step to
        # satisfy it belongs.
        unmatched_requirements = set(declared_requirements) - set(
            consumed_requirement_keys
        )
        unmatched_requirements_by_plan.extend(
            (plan.plan_id, key) for key in sorted(unmatched_requirements)
        )

        _check_acyclic(plan)

    if unmatched_requirements_by_plan:
        raise StepRequirementMismatchError(
            "plan(s) declare source_requirement(s) with no registered step "
            "to satisfy them: "
            + ", ".join(
                f"{plan_id!r}: {key!r}"
                for plan_id, key in unmatched_requirements_by_plan
            )
        )


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
