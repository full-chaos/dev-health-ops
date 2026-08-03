"""CHAOS-3334: a failed mandatory plan step must prevent a COMPLETE answer.

Found by codex adversarial review of CHAOS-3332's branch; **pre-existing**,
not introduced there. The plan-governed investigation result was persisted to
``dev_runs.plan_step_partition`` and then discarded for answer validation: the
legacy model loop starts with empty ``tool_results``, and coverage was
computed only from the tools the *model* chose to call. A run whose mandatory
``required_source_health`` step failed could therefore return
``status="complete"`` over a coverage of 1/1 — a confident answer standing on
a required production source that was never read. That is the laundering
sibling of CHAOS-3332's crash class: the failure is recorded, then spent
nowhere.

The fix merges the plan's mandatory observations into the server-owned
coverage, so the refusal comes from the answer contract's *existing*
completeness invariant (``DevAnswer.validate_status_consistency``) rather than
from a second completeness rule that could drift away from the first.

Every test drives the real ``DevOrchestrator.run`` with a real plan and the
real ``PlanExecutor`` — never a hand-built ``DevCoverage`` — so what is
asserted is the terminal an actual run reaches.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts import AnswerStatus, DevError, ToolID
from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.investigation_plans.state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
)
from dev_health_ops.api.dev.orchestrator import DevOrchestrator
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from dev_health_ops.metrics.prometheus import ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    answer_payload,
    run_preflight_orchestrator,
)
from tests._chaos_3295_plan_executor import (
    FakePlanExecutorRuntime,
    InvestigationRecorder,
    executor_for,
)

#: ``status.entity.v2``'s two mandatory steps, and the ``source_class`` label
#: each one's observation carries into coverage. Both are exercised: the
#: ticket asks for proof on *both* mandatory steps, because a merge keyed on
#: one of them would pass a single-step test while leaving the other laundered.
STATUS_SOURCE_LABEL = "status_change"
HEALTH_SOURCE_LABEL = "source_health"
WORK_GRAPH_SOURCE_LABEL = "work_graph"

QUESTION = "What's the status of the Ask Dev project?"

#: An issue subject, because ``_work_graph_applicable`` only fires for issue
#: and pull-request scopes -- a project subject can never exercise the
#: applicable-conditional arm at all. Single-word label: the interpreter binds
#: a name to the adjacent kind noun, and a multi-word label does not resolve
#: through "<name> issue" phrasing.
NIGHTFALL_ISSUE = AuthorizedEntity(EntityKind.ISSUE, "issue-4242", "Nightfall")


class WorkGraphFailingRuntime(FakePlanExecutorRuntime):
    """The shared double, with the applicable ``work_graph_expansion`` failing.

    Distinct from a *skipped* conditional step: this one's applicability
    predicate says yes, so it runs and then fails.
    """

    async def work_graph_neighbors(self, **kwargs: Any) -> Any:
        self.work_graph_calls += 1
        raise RuntimeError("work graph source unavailable")


class HealthFailingRuntime(FakePlanExecutorRuntime):
    """The shared double, with the ``required_source_health`` step failing.

    Subclassed rather than adding a flag to the shared runtime: several lanes
    are editing that file concurrently, and this is the only suite that needs
    the behaviour.
    """

    async def data_health(self, **kwargs: Any) -> Any:
        self.data_health_calls += 1
        raise RuntimeError("source health probe unavailable")


def _script(status: str, *, tool: ToolID = ToolID.STATUS_SNAPSHOT) -> Any:
    """One model tool call, then answer with ``status``.

    Spare answer steps are appended because a refused answer is *repairable*:
    the orchestrator feeds the validator's reason back and asks for another,
    which consumes further provider rounds. Without them the script would
    empty and the run would end as ``INVALID_RESPONSE``/``internal_error``,
    hiding the honest refusal this suite exists to measure behind an opaque
    one.
    """

    def build(script_id: str) -> list[ScriptedStep]:
        steps: list[ScriptedStep] = [
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id=tool.value,
                    arguments={"limit": 25, "include_comparison": False},
                    call_id="tool_call_01",
                ),
                usage=AgentUsage(input_tokens=100, output_tokens=10),
            )
        ]
        payload = deepcopy(answer_payload(script_id=script_id))
        payload["status"] = status
        steps.extend(
            ScriptedStep(decision=AgentFinalAnswer(deepcopy(payload))) for _ in range(6)
        )
        return steps

    return build


def _complete_then_partial_script(script_id: str) -> list[ScriptedStep]:
    """One tool call, a ``complete`` answer (refused), then a ``partial`` one."""

    steps: list[ScriptedStep] = [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id=ToolID.STATUS_SNAPSHOT.value,
                arguments={"limit": 25, "include_comparison": False},
                call_id="tool_call_01",
            ),
            usage=AgentUsage(input_tokens=100, output_tokens=10),
        )
    ]
    for status in ("complete", "partial"):
        payload = deepcopy(answer_payload(script_id=script_id))
        payload["status"] = status
        steps.append(ScriptedStep(decision=AgentFinalAnswer(payload)))
    return steps


async def _plan_run(
    *, runtime: FakePlanExecutorRuntime, status: str, script_id: str
) -> Any:
    return await run_preflight_orchestrator(
        question=QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id=script_id,
        script=_script(status),
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )


@pytest.mark.parametrize(
    ("label", "runtime_factory", "failed_step", "failed_label"),
    [
        pytest.param(
            "required_source_health",
            HealthFailingRuntime,
            "required_source_health",
            HEALTH_SOURCE_LABEL,
            id="required_source_health",
        ),
        pytest.param(
            "status_snapshot",
            lambda: FakePlanExecutorRuntime(status_snapshot_fails=True),
            "status_snapshot",
            STATUS_SOURCE_LABEL,
            id="status_snapshot",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_failed_mandatory_plan_step_refuses_a_complete_answer(
    label: str,
    runtime_factory: Any,
    failed_step: str,
    failed_label: str,
) -> None:
    """The headline control, proven on BOTH of the plan's mandatory steps.

    The model is *allowed* to try: it calls ``status_snapshot.v1``, that call
    succeeds, and it returns a confident ``complete``. The refusal has to come
    from the server's own knowledge that a required source failed — which is
    exactly the knowledge that used to be thrown away.

    Mutation kill site: reverting ``"coverage"`` at the answer-candidate site
    to the bare ``_coverage_from_tool_results(...)`` call (the pre-fix code)
    fails this at the terminal assertion — observed ``RunState.COMPLETED``
    with ``answer.status is AnswerStatus.COMPLETE`` and coverage ``1/1``,
    which is the ticket's filed repro verbatim.
    """

    runtime = runtime_factory()
    output = await _plan_run(
        runtime=runtime, status="complete", script_id=f"chaos3334-refuse-{label}"
    )

    result = output.result
    assert result.state is RunState.FAILED
    assert result.error is not None
    # Typed and honest -- never the internal_error bucket CHAOS-3332 removed.
    assert result.error.code == "answer_validation_failed"
    assert result.answer is None

    # The run really did investigate and really did record the failure --
    # otherwise the refusal could be coming from something unrelated.
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    investigation = recorder.results[0]
    assert failed_step in investigation.failed_steps
    assert any(
        observation.source_class.value == failed_label
        and observation.requirement_level == "mandatory"
        and observation.observed_state in UNMEASURED_REQUIREMENT_STATES
        for observation in investigation.observations
    )


@pytest.mark.asyncio
async def test_an_honest_partial_answer_survives_and_discloses_the_failed_source() -> (
    None
):
    """The fix must refuse over-claiming, not refuse answering.

    A run that lost one required source can still say something true about
    what it did read. This is the control that stops the fix from degrading
    into "any plan failure kills the run" — and it pins the disclosure: the
    failed source is named in ``unavailable_required_sources``, so a reader of
    the answer can see *which* source is missing rather than only that the
    count fell short.
    """

    output = await _plan_run(
        runtime=HealthFailingRuntime(),
        status="partial",
        script_id="chaos3334-partial-allowed",
    )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.answer is not None
    assert result.answer.status is AnswerStatus.PARTIAL

    coverage = result.answer.coverage
    assert HEALTH_SOURCE_LABEL in coverage.unavailable_required_sources
    # One model tool call plus the plan's two mandatory sources; the failed
    # one is required-but-not-available.
    assert coverage.required_source_count == 3
    assert coverage.available_source_count == 2


@pytest.mark.asyncio
async def test_a_healthy_plan_run_still_allows_a_complete_answer() -> None:
    """Negative control bounding the merge's blast radius.

    Every mandatory source resolved, so ``complete`` must still be reachable.
    Without this, the fix could have been "always append an unavailable
    source", which would refuse every plan-governed complete answer and pass
    both headline tests above.

    It also proves the ``conditional`` requirements are excluded: this plan's
    ``work_graph`` and ``work_item`` requirements are skipped and observed
    ``not_applicable`` — a state inside ``UNMEASURED_REQUIREMENT_STATES`` — so
    counting non-mandatory observations would make this run refuse.
    """

    output = await _plan_run(
        runtime=FakePlanExecutorRuntime(),
        status="complete",
        script_id="chaos3334-healthy-complete",
    )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.answer is not None
    assert result.answer.status is AnswerStatus.COMPLETE

    coverage = result.answer.coverage
    assert coverage.unavailable_required_sources == []
    assert coverage.stale_required_sources == []
    assert coverage.required_source_count == 3
    assert coverage.available_source_count == 3

    # The conditional requirements really were observed unmeasured -- so the
    # exclusion above is doing work, not passing vacuously.
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    conditional = [
        observation
        for observation in recorder.results[0].observations
        if observation.requirement_level != "mandatory"
    ]
    assert conditional
    assert all(
        observation.observed_state in UNMEASURED_REQUIREMENT_STATES
        for observation in conditional
    )


@pytest.mark.asyncio
async def test_a_plan_free_run_keeps_its_tool_only_coverage() -> None:
    """The flag-off path must be untouched.

    ``investigation_result`` is ``None`` for every run the plan seam does not
    govern, which is still the majority of production traffic. Coverage there
    must remain exactly what the model's own tool calls produced.
    """

    output = await run_preflight_orchestrator(
        question=QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3334-no-plan",
        script=_script("complete"),
    )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.status is AnswerStatus.COMPLETE
    coverage = result.answer.coverage
    assert coverage.required_source_count == 1
    assert coverage.available_source_count == 1
    assert coverage.unavailable_required_sources == []


@pytest.mark.asyncio
async def test_an_applicable_conditional_that_failed_also_refuses_complete() -> None:
    """The same laundering one requirement level over (team-lead review, Q2).

    A ``conditional`` requirement is not "optional" -- it is *required if
    applicable*. ``_work_graph_applicable`` fires for issue and pull-request
    subjects, so on an issue subject ``work_graph_expansion`` is deemed
    needed, actually runs, and can fail. Before this arm landed, such a run
    still answered ``complete`` at coverage 3/3 with an empty unavailable
    list -- the plan asked for a source, attempted it, did not get it, and
    the answer claimed completeness anyway.

    Note what makes this test meaningful: ``work_graph_calls == 1``. The
    conditional step must have *run*. A version of this case where the
    predicate declined and the step never executed would prove nothing --
    that is the ``not_applicable`` path, which the control below requires to
    stay non-blocking.

    Mutation kill site: restoring the ``!= "mandatory"`` filter (the
    mandatory-only arm) fails this at the terminal assertion -- observed
    ``RunState.COMPLETED`` with ``AnswerStatus.COMPLETE`` and coverage 3/3,
    which is the repro exactly as reported.
    """

    runtime = WorkGraphFailingRuntime()
    output = await run_preflight_orchestrator(
        question="What's the status of the Nightfall issue?",
        entities=[(ORG_ID, NIGHTFALL_ISSUE)],
        script_id="chaos3334-conditional-failed",
        script=_script("complete"),
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    # The conditional step was applicable and really executed.
    assert runtime.work_graph_calls == 1

    result = output.result
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "answer_validation_failed"
    assert result.answer is None

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    investigation = recorder.results[0]
    assert "work_graph_expansion" in investigation.failed_steps
    work_graph = [
        observation
        for observation in investigation.observations
        if observation.source_class.value == WORK_GRAPH_SOURCE_LABEL
    ]
    assert len(work_graph) == 1
    # Measured and failed -- not merely "never applicable".
    assert work_graph[0].requirement_level == "conditional"
    assert work_graph[0].observed_state is not SourceRequirementState.NOT_APPLICABLE
    assert work_graph[0].observed_state in UNMEASURED_REQUIREMENT_STATES


@pytest.mark.asyncio
async def test_the_merged_coverage_survives_a_repair_turn() -> None:
    """Coverage must be re-merged on every candidate, not just the first.

    The answer candidate is rebuilt inside the model-round loop, so a
    refused-then-repaired answer goes through the merge a second time. That
    is correct today only because ``investigation_result`` is a loop-
    invariant free variable and the candidate dict is rebuilt rather than
    mutated -- properties a later refactor could quietly break, at which
    point the *second* answer would silently revert to tool-only coverage
    and the repair turn would become a way to launder exactly what the first
    turn refused.

    The script answers ``complete`` first (refused, costing a repair round)
    and ``partial`` second (accepted), so the assertions below are read off
    an answer produced on a repair turn.
    """

    output = await run_preflight_orchestrator(
        question=QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3334-repair-turn",
        script=_complete_then_partial_script,
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(HealthFailingRuntime()),
    )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.status is AnswerStatus.PARTIAL
    # The provider really was asked twice -- otherwise no repair turn
    # happened and this test would be measuring the first candidate.
    assert output.provider is not None
    assert len(output.provider.user_texts) >= 3

    coverage = result.answer.coverage
    assert HEALTH_SOURCE_LABEL in coverage.unavailable_required_sources
    assert coverage.required_source_count == 3
    assert coverage.available_source_count == 2


def test_every_source_requirement_state_is_classified() -> None:
    """Totality over the closed vocabulary.

    ``_coverage_with_plan_sources`` splits every *participating* observation
    -- mandatory, plus conditional ones that were applicable -- three ways:
    unavailable, stale, or available. A state added to
    ``SourceRequirementState`` later must land in exactly one of those buckets
    deliberately -- this test fails the moment a new member appears without
    someone deciding which it is, rather than letting it silently default to
    "available" (the fall-through branch) and quietly re-open the laundering.

    ``NOT_APPLICABLE`` sits in the unavailable bucket here and still never
    blocks a conditional, because participation is decided *before* this
    classification: the merge skips a conditional observation carrying that
    state outright. Both facts are load-bearing and separately pinned --
    ``test_a_healthy_plan_run_still_allows_a_complete_answer`` for the skip,
    this test for the classification.
    """

    stale_states = {SourceRequirementState.AVAILABLE_STALE}
    available_states = {
        SourceRequirementState.AVAILABLE_CURRENT,
        SourceRequirementState.AVAILABLE_UNKNOWN,
    }
    classified = UNMEASURED_REQUIREMENT_STATES | stale_states | available_states
    assert set(SourceRequirementState) == classified, (
        "a SourceRequirementState member is unclassified for coverage merging; "
        "decide whether it blocks a complete answer instead of inheriting the "
        "available fall-through"
    )
    # The three buckets must be disjoint, or a state's treatment would depend
    # on branch order rather than on the vocabulary.
    assert not (UNMEASURED_REQUIREMENT_STATES & stale_states)
    assert not (UNMEASURED_REQUIREMENT_STATES & available_states)
    assert not (stale_states & available_states)


@pytest.mark.asyncio
async def test_an_unserializable_terminal_error_rewrite_is_never_silent(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3332's folded-in sibling: ``finish()``'s invalid-DevError guard.

    Like the answer-write rewrite beside it, this branch is handled *locally*
    and never reaches ``run()``'s catch-all, so it rewrote a run to
    ``internal_error`` with no log, no counter, and no exception type. Its own
    comment calls it unreachable in practice -- which is exactly why it needed
    a signal: an unreachable branch that fires unlogged is how a defect stays
    invisible for a whole wave.

    Reaching it needs fault injection, because a constructed ``DevError``
    round-trips through ``model_validate`` by construction. Patching that one
    classmethod to reject plants precisely the defect the guard exists to
    catch -- a ``DevError`` this module cannot serialize -- rather than
    approximating it.

    Mutation kill site: removing the new ``logger.exception`` fails this at
    ``len(records) == 1`` (observed 0) while the terminal assertions still
    pass.
    """

    real_model_validate = DevError.model_validate

    # A *genuine* pydantic ValidationError, produced by the real validator on
    # a real invalid payload -- not a hand-rolled stand-in. The guard catches
    # ValidationError specifically, so raising any other exception type here
    # would sail straight past it and prove nothing about this branch (it did,
    # on the first attempt: a plain ValueError propagated to the catch-all).
    try:
        real_model_validate({"schema_version": "not_a_dev_error"})
    except ValidationError as genuine:
        planted = genuine
    else:  # pragma: no cover - defensive; the payload above is invalid
        raise AssertionError("DevError.model_validate accepted an invalid payload")

    def rejecting_model_validate(*args: Any, **kwargs: Any) -> DevError:
        raise planted

    before = _counter_value(exception_type="ValidationError")

    # Patched only for the duration of the run; `finish()` still *constructs*
    # DevError instances (a different code path) so the replacement terminal
    # error is built normally.
    monkeypatch.setattr(DevError, "model_validate", rejecting_model_validate)
    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question="What's the status of the Nightfall project?",
            entities=[(ORG_ID, ASK_DEV_PROJECT)],
            script_id="chaos3334-terminal-error",
            script=_script("complete"),
        )
    monkeypatch.setattr(DevError, "model_validate", real_model_validate)

    result = output.result
    # The run still terminates coherently -- visibly, not differently.
    assert result.state in {RunState.FAILED, RunState.INSUFFICIENT_EVIDENCE}
    assert result.error is not None
    assert result.error.code == "internal_error"

    records = [
        record
        for record in caplog.records
        if record.message == "ask_dev.orchestrator.terminal_error_rewrite"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].exc_info is not None
    assert getattr(records[0], "run_id") == result.run_id
    # The code that was rejected is recorded, so an operator can tell which
    # producer built the unserializable error.
    assert getattr(records[0], "rejected_code")

    after = _counter_value(exception_type="ValidationError")
    if after == after:  # False only for the no-op-counter build (NaN)
        assert after == before + 1


def _counter_value(**labels: str) -> float:
    labelled = ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(**labels)
    value = getattr(labelled, "_value", None)
    if value is None:
        return float("nan")
    return float(value.get())


def test_the_merge_helper_is_reached_from_the_answer_path() -> None:
    """Guards against the helper being correct but unwired.

    A pure-function fix that nothing calls passes every unit test written
    about it. The behavioural tests above are the real proof, but this one
    fails fast and unambiguously if the call site is deleted, rather than
    leaving four behavioural failures to be diagnosed.
    """

    import inspect

    source = inspect.getsource(DevOrchestrator.run)
    assert "_coverage_with_plan_sources" in source
