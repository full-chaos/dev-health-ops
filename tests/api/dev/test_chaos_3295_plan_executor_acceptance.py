"""CHAOS-3295 acceptance controls: the plan-governed investigation seam.

Driven through the real orchestrator seam (``DevOrchestrator.run`` via
``tests._chaos_3292_preflight.run_preflight_orchestrator``), never a
diagnostic that inspects internals without exercising the seam. Every test
asserts *executed behaviour*: which steps ran, what observation states were
recorded, and -- for the negative controls -- what did NOT happen.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import (
    QuestionIntentID,
    SourceRequirementState,
)
from dev_health_ops.api.dev.data_health_service import DataHealthState
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.status_change_service import StatusResultState
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    run_preflight_orchestrator,
    status_then_answer,
)
from tests._chaos_3295_plan_executor import (
    FakePlanExecutorRuntime,
    InvestigationRecorder,
    executor_for,
)


@pytest.mark.asyncio
async def test_positive_committed_status_plan_runs_mandatory_steps_and_records_result():
    """Mutation kill site (clause-level, observed 2026-08-01): dropping the
    last declared ``source_requirement`` from the executor's per-requirement
    observation loop (``plan.source_requirements[:-1]``) fails this test at
    ``assert len(result.observations) == len(plan.source_requirements)``
    (observed 3 vs 4) -- a measurement that did not happen must FAIL, and
    here it did. Reverted; suite green again.
    """

    runtime = FakePlanExecutorRuntime()
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-p1",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    assert runtime.status_snapshot_calls == 1
    assert runtime.data_health_calls == 1
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    assert len(recorder.results) == 1
    result = recorder.results[0]
    assert result.schema_version == "dev_investigation_result.v1"
    assert result.plan_id == "status.entity.v2"
    assert "status_snapshot" in result.completed_steps
    assert "required_source_health" in result.completed_steps
    for observation in result.observations:
        assert observation.observed_state in SourceRequirementState
    # One observation per declared source requirement -- never more.
    plan = CORE_PLANS_BY_INTENT[QuestionIntentID.ENTITY_STATUS]
    assert len(result.observations) == len(plan.source_requirements)


@pytest.mark.asyncio
async def test_negative_uncommitted_subject_never_reaches_status_snapshot():
    """(e) The named-project plan cannot call status_snapshot before commit."""

    runtime = FakePlanExecutorRuntime()
    output = await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-n-uncommitted",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    assert runtime.status_snapshot_calls == 0
    assert runtime.data_health_calls == 0
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    assert recorder.results == []


@pytest.mark.asyncio
async def test_negative_stale_required_source_yields_available_stale_not_unavailable():
    runtime = FakePlanExecutorRuntime(data_health_state=DataHealthState.STALE)
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-n-stale",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    result = recorder.results[0]
    assert "required_source_health" in result.completed_steps
    assert "required_source_health" not in result.failed_steps
    health_observations = [
        obs
        for obs in result.observations
        if obs.adapter_id == "data_health_service.inspect.v1"
    ]
    assert len(health_observations) == 1
    assert (
        health_observations[0].observed_state == SourceRequirementState.AVAILABLE_STALE
    )


@pytest.mark.asyncio
async def test_negative_declared_source_requirements_never_produce_duplicate_observations():
    """(d) No retried/duplicated step inflates the required-source count."""

    runtime = FakePlanExecutorRuntime()
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-n-no-double-count",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    result = recorder.results[0]
    keys = [(obs.source_class, obs.adapter_id) for obs in result.observations]
    assert len(keys) == len(set(keys))


@pytest.mark.asyncio
async def test_observation_requirement_level_matches_the_plans_declared_attribution():
    """Mandatory-vs-conditional attribution survives into every observation.

    ``DevInvestigationResult.completed_steps/skipped_steps`` are flat step-id
    lists with no mandatory/conditional tag of their own -- the only place
    that attribution reaches the wire is each observation's own
    ``requirement_level``, taken from the plan's declared
    ``source_requirements`` (never the step's own registration). A failed or
    skipped *mandatory* step must be distinguishable from a skipped
    *conditional* one downstream (3297's disclosed-limitation /
    ``answered_with_gaps`` logic depends on exactly this).

    Mutation kill site (clause-level, observed 2026-08-01): hardcoding
    ``requirement_level="conditional"`` in the executor's ``_to_observation``
    (flipping every mandatory step's attribution at evaluation time) fails
    this test at ``assert observation.requirement_level == expected[key]``
    (observed ``'conditional' == 'mandatory'`` on the status_snapshot
    observation). Reverted; suite green again.
    """

    runtime = FakePlanExecutorRuntime()
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-requirement-level",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    result = recorder.results[0]
    plan = CORE_PLANS_BY_INTENT[QuestionIntentID.ENTITY_STATUS]
    expected = {
        (req.source_class, req.adapter_id): req.requirement_level
        for req in plan.source_requirements
    }
    for observation in result.observations:
        key = (observation.source_class, observation.adapter_id)
        assert observation.requirement_level == expected[key]
    # The plan's own mandatory/conditional split must actually be mixed,
    # otherwise this control cannot distinguish the two.
    assert "mandatory" in expected.values()
    assert "conditional" in expected.values()


@pytest.mark.asyncio
async def test_assessment_source_limit_reached_maps_to_truncated():
    """Recon mapping: MAX_STATUS_ASSESSMENT_ITEMS must never read as complete.

    ``status_change_service.status_snapshot`` appends the exact warning
    ``"status assessment source bound reached"`` when
    ``assessment_source_limit_reached`` fires; silently keeping the normal
    state mapping would let a truncated assessment masquerade as a complete
    one downstream.

    Mutation kill site (clause-level, observed 2026-08-01): removing the
    ``_ASSESSMENT_SOURCE_BOUND_WARNING in result.warnings`` branch from
    ``builtin_steps.status_snapshot_run`` fails this test at
    ``assert observation.observed_state == SourceRequirementState.TRUNCATED``
    (observed ``available_current`` -- the truncation silently read as a
    complete result). Reverted; suite green again.
    """

    runtime = FakePlanExecutorRuntime(
        status_snapshot_warnings=("status assessment source bound reached",)
    )
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-assessment-truncated",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    result = recorder.results[0]
    status_observations = [
        obs
        for obs in result.observations
        if obs.adapter_id == "status_change_service.status_snapshot.v1"
    ]
    assert len(status_observations) == 1
    observation = status_observations[0]
    assert observation.observed_state == SourceRequirementState.TRUNCATED
    assert observation.usable_fact_count == 0
    assert observation.limitation == "assessment_source_limit_reached"


@pytest.mark.asyncio
async def test_insufficient_evidence_status_snapshot_does_not_crash_the_run():
    """Codex finding (HIGH, 2026-08-01): a legitimate no-evidence status
    result must never abort the run.

    ``StatusResultState.INSUFFICIENT_EVIDENCE`` maps to
    ``SourceRequirementState.UNAVAILABLE`` (an unmeasured state). Before the
    fix, ``status_snapshot_run`` still reported queried semantics for it
    (usable_fact_count derived from a count, no limitation), which failed
    ``DevSourceObservation``'s own "a source that was not fully measured
    requires a bounded limitation" validator -- the orchestrator's outer
    exception handler then converted that into a user-visible
    ``internal_error`` and no investigation result was ever recorded.

    Kill site verified (2026-08-01) by temporarily reverting
    ``status_snapshot_run`` to call ``StepOutcome`` directly with queried
    semantics regardless of state (bypassing ``_status_mapped_outcome``):
    with the fix reverted, this test fails at
    ``assert output.result.error is None or output.result.error.code !=
    "internal_error"`` -- the run terminates ``RunState.FAILED`` with
    ``error.code == "internal_error"``, exactly the codex-reported defect
    (``DevSourceObservation``'s own zero-semantics validator raises inside
    the executor; the orchestrator's outer exception handler then converts
    that into the generic internal_error rather than ever calling
    ``record_investigation_result``). Reverted; suite green again. **Kill site
    re-verified on 2026-08-03** after the script change below, by planting the
    same mutation again: the test still fails with
    ``error.code == "internal_error"``, so the extra answer steps did not
    weaken what this case measures.

    Script note (CHAOS-3334): the default two-step script (one tool call, one
    answer) is no longer enough here. ``INSUFFICIENT_EVIDENCE`` maps to an
    unmeasured state on a *mandatory* plan source, which CHAOS-3334 now folds
    into answer coverage -- so the fixture's ``complete`` answer is correctly
    refused once, and the orchestrator spends a provider round asking for a
    repair. With only one answer step the script would empty and the run would
    end ``INVALID_RESPONSE``/``internal_error``, which reads exactly like the
    defect this test guards while actually being a starved fixture. The spare
    steps let the run reach its honest terminal
    (``answer_validation_failed``), which is what "does not crash the run"
    means now that a required-source failure is judged rather than discarded.
    """

    runtime = FakePlanExecutorRuntime(
        status_state=StatusResultState.INSUFFICIENT_EVIDENCE
    )

    def script(script_id: str) -> list[ScriptedStep]:
        steps = list(status_then_answer(script_id))
        steps.extend(steps[-1:] * 5)
        return steps

    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3295-insufficient-evidence",
        script=script,
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    assert output.result.error is None or output.result.error.code != "internal_error"
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    assert len(recorder.results) == 1
    result = recorder.results[0]
    status_observations = [
        obs
        for obs in result.observations
        if obs.adapter_id == "status_change_service.status_snapshot.v1"
    ]
    assert len(status_observations) == 1
    observation = status_observations[0]
    assert observation.observed_state == SourceRequirementState.UNAVAILABLE
    assert observation.usable_fact_count == 0
    assert observation.data_semantics == "not_measured"
    assert observation.limitation == "status_snapshot_insufficient_evidence"


@pytest.mark.asyncio
async def test_registered_metric_deltas_skips_not_applicable_on_empty_catalog():
    """Codex finding (MEDIUM, 2026-08-01): an empty metric catalog must skip
    ``registered_metric_deltas`` as NOT_APPLICABLE, never run it and report
    UNAVAILABLE/``all_requested_metrics_failed`` (which would misreport an
    absent optional source as an answer-completeness gap).

    Driven directly against the real ``change.observed.v1`` plan and the
    real registered builtin steps (not the orchestrator seam -- this is a
    property of one step's applicability predicate, independent of how the
    subject was committed).

    Kill site verified by temporarily reverting
    ``registered_metrics_present_applicable`` to ``lambda ctx: True``: with
    the fix reverted, this test fails because the step actually runs
    (``query_metric_calls`` becomes nonzero via the empty-definitions
    gather, and the observed state is UNAVAILABLE/all_requested_metrics_failed
    rather than NOT_APPLICABLE/step_not_applicable). Reverted; suite green
    again.
    """

    from dev_health_ops.api.dev.investigation_plans import (
        PlanExecutor,
        StepRegistry,
        register_builtin_steps,
    )
    from tests._chaos_3295_plan_executor import fixed_now, step_context_for

    runtime = FakePlanExecutorRuntime(metric_definitions=())
    registry = StepRegistry()
    register_builtin_steps(registry, runtime)
    executor = PlanExecutor(registry=registry, now=fixed_now)
    plan = CORE_PLANS_BY_INTENT[QuestionIntentID.OBSERVED_CHANGE]

    result = await executor.run(
        plan=plan, context=step_context_for(), run_id="run-empty-catalog"
    )

    assert "registered_metric_deltas" in result.skipped_steps
    metric_observations = [
        obs
        for obs in result.observations
        if obs.adapter_id == "metrics.query_metric.v1"
    ]
    assert len(metric_observations) == 1
    observation = metric_observations[0]
    assert observation.observed_state == SourceRequirementState.NOT_APPLICABLE
    assert observation.limitation == "step_not_applicable"
    assert runtime.query_metric_calls == 0
