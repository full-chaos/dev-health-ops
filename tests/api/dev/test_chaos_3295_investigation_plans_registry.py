"""Registry-construction validation for CHAOS-3295's plan/step registry.

Every negative case here proves the specific guard named in Amendment TRD
v2 §4.3 fires -- not merely that *some* exception is raised.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevPlanStepDependency,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.investigation_plans import (
    CORE_PLANS_BY_INTENT,
    CORE_QUESTION_INTENT_IDS,
    PlanStepDefinition,
    StepOutcome,
    StepRegistry,
    build_default_registry,
    plan_registry_manifest,
    register_builtin_steps,
    validate_registry,
)
from dev_health_ops.api.dev.investigation_plans.registry_validation import (
    DependencyCycleError,
    MissingCorePlanError,
    MissingStepImplementationError,
    OrphanStepRegistrationError,
    StepRequirementMismatchError,
)
from dev_health_ops.api.dev.investigation_plans.steps import DuplicateStepError
from tests._chaos_3295_plan_executor import FakePlanExecutorRuntime


def test_build_default_registry_succeeds_for_the_real_six_core_plans():
    registry = build_default_registry(FakePlanExecutorRuntime())
    for plan in CORE_PLANS_BY_INTENT.values():
        registered = registry.for_plan(plan.plan_id)
        for step_id in (*plan.mandatory_steps, *plan.conditional_steps):
            assert step_id in registered


def test_totality_every_core_intent_has_a_registered_plan():
    assert CORE_QUESTION_INTENT_IDS == frozenset(CORE_PLANS_BY_INTENT)
    for intent_id in QuestionIntentID:
        if intent_id in {
            QuestionIntentID.PROJECT_HEALTH,
            QuestionIntentID.TEAM_HEALTH,
            QuestionIntentID.TEAM_WORKLOAD_BALANCE,
            QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
            QuestionIntentID.PORTFOLIO_STATUS,
            QuestionIntentID.BOUNDED_INVESTIGATION,
        }:
            # Explicitly out of CHAOS-3295 scope (3303/3304/3305, or never
            # plan-governed at all for BOUNDED_INVESTIGATION).
            continue
        assert intent_id in CORE_PLANS_BY_INTENT, f"{intent_id} has no core plan"


def test_missing_core_plan_is_rejected():
    plans = dict(CORE_PLANS_BY_INTENT)
    del plans[QuestionIntentID.DATA_TRUST]
    registry = StepRegistry()
    register_builtin_steps(registry, FakePlanExecutorRuntime())
    with pytest.raises(MissingCorePlanError):
        validate_registry(
            plans_by_intent=plans, steps=registry, core_intents=CORE_QUESTION_INTENT_IDS
        )


def test_step_declared_but_never_registered_is_rejected():
    registry = StepRegistry()  # deliberately empty -- no builtin steps registered
    with pytest.raises(MissingStepImplementationError):
        validate_registry(
            plans_by_intent=CORE_PLANS_BY_INTENT,
            steps=registry,
            core_intents=CORE_QUESTION_INTENT_IDS,
        )


def test_registering_the_same_step_twice_is_rejected():
    registry = StepRegistry()

    async def run(_ctx):
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="no_data",
            usable_fact_count=0,
        )

    definition = PlanStepDefinition(
        step_id="status_snapshot",
        plan_id="status.entity.v2",
        source_class=SourceClass.STATUS_CHANGE,
        adapter_id="status_change_service.status_snapshot.v1",
        requirement_level="mandatory",
        run=run,
    )
    registry.register(definition)
    with pytest.raises(DuplicateStepError):
        registry.register(definition)


def _minimal_plan(
    *, step_dependencies: tuple[DevPlanStepDependency, ...]
) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("a", "b"),
        conditional_steps=(),
        step_dependencies=step_dependencies,
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.a.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_ITEM,
                adapter_id="test.b.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )


def test_multi_node_dependency_cycle_is_rejected():
    """A -> B -> A is invisible to the per-plan self-loop check alone."""

    plan = _minimal_plan(
        step_dependencies=(
            DevPlanStepDependency(step_id="a", depends_on=("b",)),
            DevPlanStepDependency(step_id="b", depends_on=("a",)),
        )
    )
    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    for step_id, source_class, adapter_id in (
        ("a", SourceClass.STATUS_CHANGE, "test.a.v1"),
        ("b", SourceClass.WORK_ITEM, "test.b.v1"),
    ):
        registry.register(
            PlanStepDefinition(
                step_id=step_id,
                plan_id=plan.plan_id,
                source_class=source_class,
                adapter_id=adapter_id,
                requirement_level="mandatory",
                run=run,
            )
        )
    with pytest.raises(DependencyCycleError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )


def test_declared_requirement_with_no_matching_step_is_rejected():
    """Codex re-check finding (MEDIUM, 2026-08-01): the requirement-matching
    checks proved every *registered step* matches a declared requirement,
    but never the inverse -- a plan can declare a mandatory
    source_requirement that no step consumes at all. That passed
    ``validate_registry`` and only surfaced at run time as a silent
    UNAVAILABLE/"step_unregistered" observation instead of failing at
    construction.

    Two registered steps ("a", "b"), both correctly matched -- plus a third,
    unique mandatory requirement ("c") with no step targeting it at all.

    Kill site verified (2026-08-01) by temporarily removing the
    unmatched-requirements check: with it removed, this test's
    ``pytest.raises(StepRequirementMismatchError)`` fails with "DID NOT
    RAISE". Reverted; suite green again.
    """

    plan = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("a", "b"),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.a.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_ITEM,
                adapter_id="test.b.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_GRAPH,
                adapter_id="test.c.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )
    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    for step_id, source_class, adapter_id in (
        ("a", SourceClass.STATUS_CHANGE, "test.a.v1"),
        ("b", SourceClass.WORK_ITEM, "test.b.v1"),
    ):
        registry.register(
            PlanStepDefinition(
                step_id=step_id,
                plan_id=plan.plan_id,
                source_class=source_class,
                adapter_id=adapter_id,
                requirement_level="mandatory",
                run=run,
            )
        )
    with pytest.raises(StepRequirementMismatchError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )


def test_declared_requirements_with_no_matching_step_are_aggregated_across_plans():
    """Codex re-check finding (MEDIUM, 2026-08-01, second pass): the previous
    fix (``test_declared_requirement_with_no_matching_step_is_rejected``)
    raised ``StepRequirementMismatchError`` from *inside* the per-plan loop,
    so with two plans each carrying an unconsumed requirement, only the
    first plan's mismatch was ever reported -- construction stopped at the
    first raise, and the second plan's identical defect was silently
    unreachable.

    Two plans, each independently correct except for one unique unconsumed
    mandatory requirement. Both must appear in the single raised error's
    message, proving the check accumulates across the *entire* registry
    traversal instead of short-circuiting on the first plan.

    Kill site verified (2026-08-01) by reverting the fix to raise
    immediately inside the per-plan loop: with that reverted, this test's
    message-content assertions fail because "status.entity.v2" and
    "test.unconsumed_two.v1" never appear -- validate_registry stops at
    "investigation.bounded.v1" first. Restored; both-plans assertion passes
    again.
    """

    plan_one = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("a", "b"),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.a.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_ITEM,
                adapter_id="test.b.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_GRAPH,
                adapter_id="test.unconsumed_one.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )
    plan_two = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.0",
        intent_id=QuestionIntentID.ENTITY_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("x", "y"),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.x.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_ITEM,
                adapter_id="test.y.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_GRAPH,
                adapter_id="test.unconsumed_two.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )

    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    for step_id, plan_id, source_class, adapter_id in (
        ("a", plan_one.plan_id, SourceClass.STATUS_CHANGE, "test.a.v1"),
        ("b", plan_one.plan_id, SourceClass.WORK_ITEM, "test.b.v1"),
        ("x", plan_two.plan_id, SourceClass.STATUS_CHANGE, "test.x.v1"),
        ("y", plan_two.plan_id, SourceClass.WORK_ITEM, "test.y.v1"),
    ):
        registry.register(
            PlanStepDefinition(
                step_id=step_id,
                plan_id=plan_id,
                source_class=source_class,
                adapter_id=adapter_id,
                requirement_level="mandatory",
                run=run,
            )
        )

    with pytest.raises(StepRequirementMismatchError) as excinfo:
        validate_registry(
            plans_by_intent={
                QuestionIntentID.BOUNDED_INVESTIGATION: plan_one,
                QuestionIntentID.ENTITY_STATUS: plan_two,
            },
            steps=registry,
            core_intents=frozenset(
                {
                    QuestionIntentID.BOUNDED_INVESTIGATION,
                    QuestionIntentID.ENTITY_STATUS,
                }
            ),
        )

    message = str(excinfo.value)
    assert "investigation.bounded.v1" in message
    assert "test.unconsumed_one.v1" in message
    assert "status.entity.v2" in message
    assert "test.unconsumed_two.v1" in message


def test_step_registered_against_the_wrong_adapter_is_rejected():
    """Codex finding (MEDIUM, 2026-08-01, repro): registering both step
    definitions of a two-step plan against the *same* (wrong) adapter
    previously passed validation (only step names were checked) and then
    collided at run time, when the executor could not find a requirement
    matching step "b"'s (source_class, adapter_id) and minted both
    observation ids from the same "unregistered" fallback seed --
    "observation ids must be unique".

    Kill site: pre-fix, this test's ``validate_registry`` call raises
    nothing (registration passes); post-fix it raises
    ``StepRequirementMismatchError`` because step "b" is registered against
    (STATUS_CHANGE, "test.a.v1"), which is not a declared requirement.
    """

    plan = _minimal_plan(step_dependencies=())
    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    for step_id in ("a", "b"):
        registry.register(
            PlanStepDefinition(
                step_id=step_id,
                plan_id=plan.plan_id,
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.a.v1",  # both steps registered to "a"'s adapter
                requirement_level="mandatory",
                run=run,
            )
        )
    with pytest.raises(StepRequirementMismatchError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )


def test_step_with_mismatched_mandatory_conditional_attribution_is_rejected():
    """A step declared mandatory in the plan's step lists, but whose matching
    source_requirement is declared conditional, must be rejected -- this is
    exactly the mandatory-vs-conditional attribution mutation (b) proved
    kill-able at the executor level; this is the equivalent registry-level
    guard that stops the mismatch from being constructible in the first
    place.

    Kill site verified (2026-08-01) by temporarily reducing
    ``validate_registry`` to only the step-name/cycle checks (the
    requirement-matching block folded into finding 4's fix): with that block
    removed, this test's ``pytest.raises(StepRequirementMismatchError)``
    fails with "DID NOT RAISE". Reverted; suite green again.
    """

    plan = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("a",),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.a.v1",
                requirement_level="conditional",
                applicability_rule_id="rule.v1",
                applicability_rule_version="1",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )
    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    registry.register(
        PlanStepDefinition(
            step_id="a",
            plan_id=plan.plan_id,
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="test.a.v1",
            requirement_level="mandatory",
            run=run,
        )
    )
    with pytest.raises(StepRequirementMismatchError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )


def test_orphan_step_registered_under_the_same_plan_id_is_rejected():
    """A step registered under this plan_id but never declared in
    mandatory_steps/conditional_steps is a same-plan extra registration and
    must be rejected -- distinct from a step registered under a *different*
    plan_id (legitimate: CHAOS-3303/3304/3305 share this StepRegistry).

    Kill site verified (2026-08-01): with the same requirement-matching
    block removed as above, this test's
    ``pytest.raises(OrphanStepRegistrationError)`` fails with "DID NOT
    RAISE" (the orphan check lives in the same removed block). Reverted;
    suite green again.
    """

    plan = _minimal_plan(step_dependencies=())
    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    for step_id, source_class, adapter_id in (
        ("a", SourceClass.STATUS_CHANGE, "test.a.v1"),
        ("b", SourceClass.WORK_ITEM, "test.b.v1"),
        ("c", SourceClass.WORK_GRAPH, "test.c.v1"),  # never declared by the plan
    ):
        registry.register(
            PlanStepDefinition(
                step_id=step_id,
                plan_id=plan.plan_id,
                source_class=source_class,
                adapter_id=adapter_id,
                requirement_level="mandatory",
                run=run,
            )
        )
    with pytest.raises(OrphanStepRegistrationError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )


def test_plan_registry_manifest_matches_plan_registry_membership():
    """Drift test: the generated manifest can only ever describe real plans."""

    manifest = plan_registry_manifest()
    manifest_ids = {row["plan_id"] for row in manifest}
    assert manifest_ids == {plan.plan_id for plan in CORE_PLANS_BY_INTENT.values()}
    for row in manifest:
        assert int(row["mandatory_step_count"]) >= 1  # type: ignore[call-overload]
        assert int(row["source_requirement_count"]) >= 1  # type: ignore[call-overload]
