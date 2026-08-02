"""Deterministic investigation plans + bounded executor (CHAOS-3295).

Public surface:

* :data:`CORE_PLANS_BY_INTENT` -- the six Wave 3.1 core plan documents,
  keyed by ``QuestionIntentID``.
* :class:`StepRegistry` / :func:`register_builtin_steps` -- the
  extension seam CHAOS-3303/3304/3305 register their own plan steps
  through, without changing this package or the orchestrator.
* :class:`PlanExecutor` -- runs one committed plan to a
  ``dev_investigation_result.v1``.
* :func:`build_default_registry` -- the registry-construction entry point
  ``production_runtime.py`` calls; raises at *import* time (module load, via
  ``registry_validation.validate_registry``) rather than on the first run.
"""

from __future__ import annotations

from .builtin_steps import PlanExecutorRuntime, register_builtin_steps
from .executor import PlanExecutionError, PlanExecutor, wrap_runtime_with_mint_receipts
from .plan_documents import CORE_PLANS_BY_INTENT, CORE_QUESTION_INTENT_IDS
from .registry_validation import validate_registry
from .steps import (
    DuplicateStepError,
    PlanRegistryError,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)

__all__ = [
    "CORE_PLANS_BY_INTENT",
    "CORE_QUESTION_INTENT_IDS",
    "DuplicateStepError",
    "PlanExecutionError",
    "PlanExecutor",
    "PlanExecutorRuntime",
    "PlanRegistryError",
    "PlanStepDefinition",
    "StepContext",
    "StepOutcome",
    "StepRegistry",
    "build_default_registry",
    "plan_registry_manifest",
    "register_builtin_steps",
    "validate_registry",
    "wrap_runtime_with_mint_receipts",
]


def build_default_registry(runtime: PlanExecutorRuntime) -> StepRegistry:
    """The six core plans' steps, registered and validated against ``runtime``.

    Raises :class:`PlanRegistryError` immediately at construction (never
    lazily, on the first run that happens to hit a bad plan) if a plan
    declares a step nothing registers, a plan ID outside ``PLAN_REGISTRY``,
    a dependency cycle, or a core question class with no plan -- see
    ``registry_validation.validate_registry``. Which step_ids exist is fixed
    by :func:`register_builtin_steps` regardless of ``runtime`` (only step
    *behavior* is runtime-bound), so this validation is redundant but cheap
    across repeated calls -- callers do not need to cache the result for
    correctness, only to avoid rebuilding it needlessly.
    """

    registry = StepRegistry()
    register_builtin_steps(registry, runtime)
    validate_registry(
        plans_by_intent=CORE_PLANS_BY_INTENT,
        steps=registry,
        core_intents=CORE_QUESTION_INTENT_IDS,
    )
    return registry


def plan_registry_manifest() -> list[dict[str, object]]:
    """``plan_registry_manifest.v1`` -- one row per core plan.

    Mirrors ``export_contracts_v2.py``'s drift-test posture: this is
    generated from ``CORE_PLANS_BY_INTENT`` itself, never hand-maintained,
    so the property-manifest / drift test can only ever compare the
    registry against itself plus ``PLAN_REGISTRY`` membership.
    """

    return [
        {
            "plan_id": plan.plan_id,
            "plan_version": plan.plan_version,
            "intent_id": plan.intent_id.value,
            "mandatory_step_count": len(plan.mandatory_steps),
            "conditional_step_count": len(plan.conditional_steps),
            "source_requirement_count": len(plan.source_requirements),
            "batch_strategy": plan.batch_strategy,
            "completion_rule_id": plan.completion_rule_id,
            "completion_rule_version": plan.completion_rule_version,
        }
        for plan in sorted(CORE_PLANS_BY_INTENT.values(), key=lambda p: p.plan_id)
    ]
