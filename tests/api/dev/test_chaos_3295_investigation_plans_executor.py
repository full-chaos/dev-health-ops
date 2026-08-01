"""Unit-level ``PlanExecutor`` controls (CHAOS-3295).

Constructs a ``StepContext`` directly rather than going through the full
orchestrator/preflight machinery -- these are properties of the executor's
own dependency-graph walk, independent of how a subject got committed.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
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
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)

ORG_ID = "org_fullchaos"


def _now() -> datetime:
    return datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": "project-1",
                "display_label": "Project One",
                "repository_id": None,
            }
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )


def _context() -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_scope(),
        run_id="run-1",
        now=_now(),
    )


def _test_plan(
    *,
    step_dependencies: tuple[DevPlanStepDependency, ...],
    mandatory_steps: tuple[str, ...] = ("upstream", "sibling", "downstream"),
) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=mandatory_steps,
        conditional_steps=(),
        step_dependencies=step_dependencies,
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.upstream.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_ITEM,
                adapter_id="test.sibling.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_GRAPH,
                adapter_id="test.downstream.v1",
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


def _ok_outcome(count: int = 1) -> StepOutcome:
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        usable_fact_count=count,
    )


@pytest.mark.asyncio
async def test_a_failed_prerequisite_skips_only_its_dependent():
    """downstream depends_on upstream; sibling has no dependency at all.

    Mutation kill site (clause-level, observed 2026-08-01): replacing the
    executor's ``blocked_now`` computation with an empty set -- severing the
    ``step_dependencies`` edge so a blocked step runs anyway -- fails this
    test at the ``assert "downstream" in result.skipped_steps`` line
    (observed ``downstream`` absent from ``skipped_steps`` entirely, i.e. it
    ran). Reverted; suite green again.
    """

    plan = _test_plan(
        step_dependencies=(
            DevPlanStepDependency(step_id="downstream", depends_on=("upstream",)),
        )
    )
    registry = StepRegistry()
    calls = {"downstream": 0}

    async def upstream_run(_ctx: StepContext) -> StepOutcome:
        raise RuntimeError("upstream source unavailable")

    async def sibling_run(_ctx: StepContext) -> StepOutcome:
        return _ok_outcome()

    async def downstream_run(_ctx: StepContext) -> StepOutcome:
        calls["downstream"] += 1
        return _ok_outcome()

    registry.register(
        PlanStepDefinition(
            step_id="upstream",
            plan_id=plan.plan_id,
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="test.upstream.v1",
            requirement_level="mandatory",
            run=upstream_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="sibling",
            plan_id=plan.plan_id,
            source_class=SourceClass.WORK_ITEM,
            adapter_id="test.sibling.v1",
            requirement_level="mandatory",
            run=sibling_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="downstream",
            plan_id=plan.plan_id,
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="test.downstream.v1",
            requirement_level="mandatory",
            run=downstream_run,
        )
    )

    executor = PlanExecutor(registry=registry, now=_now)
    result = await executor.run(
        plan=plan, context=_context(), run_id="run-1", subject_entity_id="project-1"
    )

    assert "upstream" in result.failed_steps
    assert "sibling" in result.completed_steps
    assert "downstream" in result.skipped_steps
    assert calls["downstream"] == 0

    by_adapter = {obs.adapter_id: obs for obs in result.observations}
    assert (
        by_adapter["test.upstream.v1"].observed_state
        == SourceRequirementState.UNAVAILABLE
    )
    assert by_adapter["test.upstream.v1"].usable_fact_count == 0
    assert (
        by_adapter["test.sibling.v1"].observed_state
        == SourceRequirementState.AVAILABLE_CURRENT
    )
    assert (
        by_adapter["test.downstream.v1"].observed_state
        == SourceRequirementState.UNAVAILABLE
    )
    assert by_adapter["test.downstream.v1"].limitation == "step_blocked_by_prerequisite"


@pytest.mark.asyncio
async def test_a_conditional_step_that_is_not_applicable_gets_a_typed_not_applicable_observation():
    plan = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("mandatory_one",),
        conditional_steps=("optional_one",),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.mandatory.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_GRAPH,
                adapter_id="test.optional.v1",
                requirement_level="conditional",
                applicability_rule_id="never.v1",
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
    registry.register(
        PlanStepDefinition(
            step_id="mandatory_one",
            plan_id=plan.plan_id,
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="test.mandatory.v1",
            requirement_level="mandatory",
            run=lambda _ctx: _async_outcome(_ok_outcome()),
        )
    )

    async def never_applicable(_ctx: StepContext) -> bool:
        return False

    registry.register(
        PlanStepDefinition(
            step_id="optional_one",
            plan_id=plan.plan_id,
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="test.optional.v1",
            requirement_level="conditional",
            run=lambda _ctx: _async_outcome(_ok_outcome()),
            applicable=lambda _ctx: False,
        )
    )

    executor = PlanExecutor(registry=registry, now=_now)
    result = await executor.run(plan=plan, context=_context(), run_id="run-1")

    assert "optional_one" in result.skipped_steps
    by_adapter = {obs.adapter_id: obs for obs in result.observations}
    assert (
        by_adapter["test.optional.v1"].observed_state
        == SourceRequirementState.NOT_APPLICABLE
    )
    assert by_adapter["test.optional.v1"].usable_fact_count == 0
    assert by_adapter["test.optional.v1"].limitation == "step_not_applicable"


@pytest.mark.asyncio
async def test_identical_inputs_produce_a_byte_identical_result():
    """P3 (CHAOS-3297 dependency): no uuid4()/datetime.now() inside the executor.

    Two independent executor instances, two independent registries, over the
    same (run_id, plan_id, step_id) triple, must mint the identical
    observation/result ids -- proving the identity is a pure function of the
    inputs, not incidental same-process determinism.
    """

    plan = _test_plan(step_dependencies=())

    def build_executor() -> PlanExecutor:
        registry = StepRegistry()
        for step_id, source_class, adapter_id in (
            ("upstream", SourceClass.STATUS_CHANGE, "test.upstream.v1"),
            ("sibling", SourceClass.WORK_ITEM, "test.sibling.v1"),
            ("downstream", SourceClass.WORK_GRAPH, "test.downstream.v1"),
        ):
            registry.register(
                PlanStepDefinition(
                    step_id=step_id,
                    plan_id=plan.plan_id,
                    source_class=source_class,
                    adapter_id=adapter_id,
                    requirement_level="mandatory",
                    run=lambda _ctx: _async_outcome(_ok_outcome(count=3)),
                )
            )
        return PlanExecutor(registry=registry, now=_now)

    result_a = await build_executor().run(
        plan=plan,
        context=_context(),
        run_id="run-shared",
        subject_entity_id="project-1",
    )
    result_b = await build_executor().run(
        plan=plan,
        context=_context(),
        run_id="run-shared",
        subject_entity_id="project-1",
    )

    assert result_a.model_dump(mode="json") == result_b.model_dump(mode="json")
    assert result_a.result_id == result_b.result_id
    assert {o.observation_id for o in result_a.observations} == {
        o.observation_id for o in result_b.observations
    }

    result_c = await build_executor().run(
        plan=plan,
        context=_context(),
        run_id="run-different",
        subject_entity_id="project-1",
    )
    assert result_c.result_id != result_a.result_id


@pytest.mark.asyncio
async def test_a_mandatory_step_depending_on_an_inapplicable_conditional_gate_is_blocked():
    """Codex finding (MEDIUM, 2026-08-01): an inapplicable prerequisite must
    still block its dependents, not vanish from the dependency graph.

    Before the fix, ``dependencies`` filtered ``depends_on`` to
    ``runnable`` (mandatory steps plus *applicable* conditional steps)
    before blocking was ever computed -- so a mandatory step depending on an
    inapplicable conditional "gate" saw no dependency at all and ran
    immediately. Repro (from the codex report): an accepted plan with
    mandatory ``downstream`` depending on an inapplicable conditional
    ``gate`` returned ``completed=('downstream',), skipped=('gate',)``.

    Kill site verified (2026-08-01) by temporarily reverting the
    ``dependencies`` fix (filtering back to ``{d for d in dep.depends_on if
    d in runnable}``): with the fix reverted, this test fails at
    ``assert downstream_calls["count"] == 0`` (observed ``1 == 0`` --
    ``downstream_run`` actually executed, matching the codex repro's
    ``completed=('downstream',)`` exactly). Reverted; suite green again.
    """

    plan = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("downstream",),
        conditional_steps=("gate",),
        step_dependencies=(
            DevPlanStepDependency(step_id="downstream", depends_on=("gate",)),
        ),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.downstream.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_GRAPH,
                adapter_id="test.gate.v1",
                requirement_level="conditional",
                applicability_rule_id="never.v1",
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
    downstream_calls = {"count": 0}

    async def downstream_run(_ctx: StepContext) -> StepOutcome:
        downstream_calls["count"] += 1
        return _ok_outcome()

    registry.register(
        PlanStepDefinition(
            step_id="downstream",
            plan_id=plan.plan_id,
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="test.downstream.v1",
            requirement_level="mandatory",
            run=downstream_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="gate",
            plan_id=plan.plan_id,
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="test.gate.v1",
            requirement_level="conditional",
            run=lambda _ctx: _async_outcome(_ok_outcome()),
            applicable=lambda _ctx: False,
        )
    )

    executor = PlanExecutor(registry=registry, now=_now)
    result = await executor.run(plan=plan, context=_context(), run_id="run-1")

    assert downstream_calls["count"] == 0
    assert result.completed_steps == ()
    assert "gate" in result.skipped_steps
    assert "downstream" in result.skipped_steps

    by_adapter = {obs.adapter_id: obs for obs in result.observations}
    assert (
        by_adapter["test.gate.v1"].observed_state
        == SourceRequirementState.NOT_APPLICABLE
    )
    assert (
        by_adapter["test.downstream.v1"].observed_state
        == SourceRequirementState.UNAVAILABLE
    )
    assert by_adapter["test.downstream.v1"].limitation == "step_blocked_by_prerequisite"


async def _async_outcome(outcome: StepOutcome) -> StepOutcome:
    return outcome
