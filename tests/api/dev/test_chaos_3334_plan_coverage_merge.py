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

import ast
import logging
import tempfile
import uuid
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev import orchestrator as orchestrator_module
from dev_health_ops.api.dev import terminal_frames as tf
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    DevAnswer,
    DevCoverage,
    DevError,
    FreshnessState,
    ToolID,
)
from dev_health_ops.api.dev.contracts_v2 import (
    DevInvestigationResult,
    DevSourceObservation,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.data_health_service import DataHealthState
from dev_health_ops.api.dev.investigation_plans import state_mapping
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.investigation_plans.state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
)
from dev_health_ops.api.dev.metrics.definitions import METRIC_REGISTRY, MetricID
from dev_health_ops.api.dev.metrics.service import MetricDataState, MetricQueryResult
from dev_health_ops.api.dev.orchestrator import DevOrchestrator
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.api.dev.status_change_service import StatusResultState
from dev_health_ops.api.dev.terminal_frames import wrap_legacy_answer_as_frame
from dev_health_ops.api.dev.work_graph_neighbors_service import WorkGraphResultState
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


UNKNOWN_STATE = SourceRequirementState.AVAILABLE_UNKNOWN
_FRESHNESS_MAPPER = "freshness_state_to_requirement_state"

#: Every canonical-result-state mapper, paired with the enum it consumes.
#: Enumerated rather than sampled: the producer trace below is only a closure
#: argument if it covers all of them.
_STATE_MAPPERS: tuple[
    tuple[str, Callable[[Any], SourceRequirementState], type[Enum]], ...
] = (
    (
        "data_health_state_to_requirement_state",
        state_mapping.data_health_state_to_requirement_state,
        DataHealthState,
    ),
    (
        "status_result_state_to_requirement_state",
        state_mapping.status_result_state_to_requirement_state,
        StatusResultState,
    ),
    (
        "metric_data_state_to_requirement_state",
        state_mapping.metric_data_state_to_requirement_state,
        MetricDataState,
    ),
    (
        "work_graph_result_state_to_requirement_state",
        state_mapping.work_graph_result_state_to_requirement_state,
        WorkGraphResultState,
    ),
    (
        "freshness_state_to_requirement_state",
        state_mapping.freshness_state_to_requirement_state,
        FreshnessState,
    ),
)

_METRIC_WINDOW = datetime(2026, 8, 1, tzinfo=UTC)


class _MetricRuntime(FakePlanExecutorRuntime):
    """The shared double with ``query_metric`` returning a chosen data state."""

    def __init__(self, state: MetricDataState) -> None:
        super().__init__()
        self._metric_state = state

    def sample_result(self) -> MetricQueryResult:
        """The exact result ``query_metric`` returns, for assertions."""
        return MetricQueryResult(
            definition=METRIC_REGISTRY[MetricID.ITEMS_COMPLETED],
            state=self._metric_state,
            freshness=FreshnessState.FRESH,
            values=(),
            coverage=1.0,
            current_window_start=_METRIC_WINDOW,
            current_window_end=_METRIC_WINDOW,
            comparison_window_start=None,
            comparison_window_end=None,
            watermark=_METRIC_WINDOW,
            source_refs=(),
        )

    async def query_metric(self, **kwargs: Any) -> MetricQueryResult:
        self.query_metric_calls += 1
        return MetricQueryResult(
            definition=METRIC_REGISTRY[MetricID.ITEMS_COMPLETED],
            state=self._metric_state,
            freshness=FreshnessState.FRESH,
            values=(),
            coverage=1.0,
            current_window_start=_METRIC_WINDOW,
            current_window_end=_METRIC_WINDOW,
            comparison_window_start=None,
            comparison_window_end=None,
            watermark=_METRIC_WINDOW,
            source_refs=(),
        )


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


@pytest.mark.asyncio
async def test_a_degraded_mandatory_status_snapshot_refuses_complete() -> None:
    """Known degradation must not launder into COMPLETE (codex finding 2).

    ``StatusResultState.DEGRADED`` maps to ``AVAILABLE_UNKNOWN``, and its own
    mapping docstring says why: *at least one contributing source is itself
    unavailable*. The facts that did return are real, so the state is not
    "unmeasured" -- but an answer claiming **complete** over a knowingly
    degraded required source is precisely the laundering this ticket exists
    to stop.

    Mutation kill site: moving ``AVAILABLE_UNKNOWN`` back to the available
    fall-through fails this at the terminal assertion -- observed
    ``RunState.COMPLETED`` with ``AnswerStatus.COMPLETE`` and coverage 3/3.
    """

    output = await _plan_run(
        runtime=FakePlanExecutorRuntime(status_state=StatusResultState.DEGRADED),
        status="complete",
        script_id="chaos3334-degraded-status",
    )

    result = output.result
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "answer_validation_failed"

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    status = [
        observation
        for observation in recorder.results[0].observations
        if observation.source_class.value == STATUS_SOURCE_LABEL
    ]
    assert len(status) == 1
    # Degraded, not unmeasured: the step still counted real facts.
    assert status[0].observed_state is SourceRequirementState.AVAILABLE_UNKNOWN
    assert status[0].observed_state not in UNMEASURED_REQUIREMENT_STATES


@pytest.mark.parametrize(
    ("metric_state", "expected_state", "expected_requirement_state"),
    [
        pytest.param(
            MetricDataState.INSUFFICIENT_EVIDENCE,
            RunState.FAILED,
            SourceRequirementState.AVAILABLE_UNKNOWN,
            id="insufficient-evidence-refused",
        ),
        pytest.param(
            MetricDataState.VALUE,
            RunState.COMPLETED,
            SourceRequirementState.AVAILABLE_CURRENT,
            id="value-still-completes",
        ),
    ],
)
@pytest.mark.asyncio
async def test_metric_insufficient_evidence_refuses_complete(
    metric_state: MetricDataState,
    expected_state: RunState,
    expected_requirement_state: SourceRequirementState,
) -> None:
    """The second ``AVAILABLE_UNKNOWN`` producer, on a different plan.

    ``metric.comparison.v1``'s mandatory ``registered_metric_query`` reaches
    the same state through ``metric_data_state_to_requirement_state``, so
    proving only the status-snapshot arm would leave a second live producer
    unpinned on a plan the first test never touches.

    The ``VALUE`` arm is the non-vacuity control: without it, a fix that
    refused *every* metric-plan run would satisfy the first arm.
    """

    runtime = _MetricRuntime(metric_state)
    output = await run_preflight_orchestrator(
        question=(
            "How does items completed compare to the previous period "
            "for the Ask Dev project?"
        ),
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id=f"chaos3334-metric-{metric_state.value}",
        script=_script("complete"),
        requested_metric_ids=["items_completed"],
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    # The metric step really ran, on the metric plan -- not the status plan.
    assert runtime.query_metric_calls == 1
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    investigation = recorder.results[0]
    assert investigation.plan_id == "metric.comparison.v1"
    assert [
        observation.observed_state for observation in investigation.observations
    ] == [expected_requirement_state]
    assert output.result.state is expected_state


def _production_callers_of(root: Path, symbol: str) -> list[str]:
    """Modules under ``root`` that import or reference ``symbol``.

    Alias-proof by keying on ``ImportFrom``'s ``alias.name`` -- the AST
    records the *imported* symbol even when ``as`` rebinds it locally -- plus
    direct ``Name``/``Attribute`` references for module-qualified calls
    (``state_mapping.symbol(...)``). The module that defines the symbol is
    excluded; every other reference counts, because importing it at all is
    the coupling this pin cares about.
    """

    callers: list[str] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        defines = any(
            isinstance(node, ast.FunctionDef) and node.name == symbol
            for node in ast.walk(tree)
        )
        if defines:
            continue
        referenced = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == symbol for alias in node.names
            ):
                referenced = True
                break
            if isinstance(node, ast.Name) and node.id == symbol:
                referenced = True
                break
            if isinstance(node, ast.Attribute) and node.attr == symbol:
                referenced = True
                break
        if referenced:
            callers.append(path.relative_to(root).as_posix())
    return callers


def test_available_unknown_has_no_healthy_producer() -> None:
    """The trace behind choosing BLOCK over split-by-cause (codex finding 2).

    Team-lead's ruling allowed splitting ``AVAILABLE_UNKNOWN`` by cause *if*
    some legitimately-healthy run could produce it on a participating source.
    Executing all five ``state_mapping`` functions over their full source
    enums says none can:

    * ``status_result_state_to_requirement_state``: ``degraded`` only
    * ``metric_data_state_to_requirement_state``: ``partial`` and
      ``insufficient_evidence`` only
    * ``freshness_state_to_requirement_state``: ``unknown`` -- the one
      "healthy data, freshness merely unproven" producer, and it has no
      callers anywhere in the repository

    so blocking needs no split. This test is what makes that conclusion
    maintained rather than a one-time observation: a new producer, or the
    freshness mapper being wired up, fails it and forces the split decision
    to be re-made instead of silently inherited.
    """

    producers = {
        name: sorted(member.value for member in enum if fn(member) is UNKNOWN_STATE)
        for name, fn, enum in _STATE_MAPPERS
    }
    assert {name: values for name, values in producers.items() if values} == {
        "status_result_state_to_requirement_state": ["degraded"],
        "metric_data_state_to_requirement_state": [
            "insufficient_evidence",
            "partial",
        ],
        "freshness_state_to_requirement_state": ["unknown"],
    }

    # The freshness mapper is the only one whose AVAILABLE_UNKNOWN would mean
    # "healthy", so its reachability is the whole basis for blocking. Assert
    # it is still uncalled across the ENTIRE src tree.
    #
    # Detection is via the AST, not a text scan: this module's own docstrings
    # name the function while explaining the decision, and a substring search
    # counts that prose as a caller (it did, on the first attempt).
    #
    # And it keys on ``alias.name`` in ImportFrom, not on the local binding.
    # A plain ``import ... as map_freshness`` rebinds the symbol, so a
    # detector that only looks for Name/Attribute nodes matching the original
    # spelling returns False on it -- verified against a synthetic module
    # below, because a pin that cannot see the most obvious evasion is not a
    # pin. The ImportFrom node still records the *imported* name regardless of
    # the alias, which is what makes this alias-proof.
    #
    # tests/ is deliberately out of scope: this suite itself calls the mapper
    # through ``_STATE_MAPPERS`` to build the producer table above. What the
    # blocking decision depends on is whether *production* can emit the state.
    src_root = Path(orchestrator_module.__file__).parents[3]
    assert src_root.name == "src", f"expected the src root, got {src_root}"
    callers = _production_callers_of(src_root, _FRESHNESS_MAPPER)

    assert callers == [], (
        "freshness_state_to_requirement_state now has callers; an "
        "AVAILABLE_UNKNOWN that means 'healthy but unproven freshness' is "
        "reachable, so _coverage_with_plan_sources must split the state by "
        "cause instead of blocking it wholesale"
    )

    # Non-vacuity: the same scan must FIND an aliased caller when one exists,
    # otherwise a broken scan (wrong root, silent parse failure) reads as
    # "no callers" forever.
    with tempfile.TemporaryDirectory() as raw:
        probe_root = Path(raw)
        package = probe_root / "some_other_package"
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "aliased.py").write_text(
            "from dev_health_ops.api.dev.investigation_plans.state_mapping "
            f"import {_FRESHNESS_MAPPER} as map_freshness\n\n"
            "def use(x):\n    return map_freshness(x)\n"
        )
        assert _production_callers_of(probe_root, _FRESHNESS_MAPPER) == [
            "some_other_package/aliased.py"
        ]


@pytest.mark.parametrize(
    ("label", "runtime_factory", "source_label"),
    [
        pytest.param(
            "degraded-status",
            lambda: FakePlanExecutorRuntime(status_state=StatusResultState.DEGRADED),
            STATUS_SOURCE_LABEL,
            id="degraded-status-snapshot",
        ),
        pytest.param(
            "insufficient-metric",
            lambda: _MetricRuntime(MetricDataState.INSUFFICIENT_EVIDENCE),
            "work_item",
            id="fresh-insufficient-evidence-metric",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_degraded_source_is_disclosed_as_degraded_not_stale(
    label: str, runtime_factory: Any, source_label: str
) -> None:
    """The disclosure must name the real failure cause (codex re-verify MED 1).

    Both producers below block a ``complete`` answer, which the tests above
    already prove. This one is about *what the client is told*: filing them
    under ``stale_required_sources`` asserts the data is old, and for neither
    of them is that true.

    * a DEGRADED status snapshot means one of its contributing sources was
      **unavailable** -- nothing about age;
    * a metric reporting INSUFFICIENT_EVIDENCE can carry
      ``FreshnessState.FRESH``, which this test pins directly rather than
      assuming, so "stale" would be flatly contradicted by the run's own
      freshness value.

    Mutation kill site: routing AVAILABLE_UNKNOWN back into
    ``stale_required_sources`` fails this at the stale-list assertion.
    """

    runtime = runtime_factory()
    is_metric = isinstance(runtime, _MetricRuntime)
    if is_metric:
        # Pinned, not assumed: the whole finding rests on this result being
        # fresh while reporting insufficient evidence.
        assert runtime.sample_result().freshness is FreshnessState.FRESH
        output = await run_preflight_orchestrator(
            question=(
                "How does items completed compare to the previous period "
                "for the Ask Dev project?"
            ),
            entities=[(ORG_ID, ASK_DEV_PROJECT)],
            script_id=f"chaos3334-truthful-{label}",
            script=_script("partial"),
            requested_metric_ids=["items_completed"],
            recorder_factory=InvestigationRecorder,
            plan_registry=CORE_PLANS_BY_INTENT,
            plan_executor=executor_for(runtime),
        )
    else:
        output = await _plan_run(
            runtime=runtime,
            status="partial",
            script_id=f"chaos3334-truthful-{label}",
        )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    coverage = result.answer.coverage
    assert source_label in coverage.degraded_required_sources
    assert source_label not in coverage.stale_required_sources
    assert source_label not in coverage.unavailable_required_sources


def test_every_coverage_bucket_survives_v1_to_v2_projection() -> None:
    """Each bucket reaches the v2 frame naming the source that actually failed.

    Codex's finding was that the *bucket* must be preserved through
    projection; measuring it turned up a second defect one layer down.
    ``_source_class_for_legacy_token`` understood only ``ToolID`` tokens, so
    every plan-sourced label -- which is a ``SourceClass`` value, not a tool
    id -- fell through to the fallback and every frame reported
    ``source_health`` regardless of which source degraded. Both grammars are
    now resolved, and this pins each bucket carrying a *distinct* class so a
    regression that collapses them cannot pass.

    Built on the canonical ``dev_answer.v1`` fixture with its content
    collections emptied: the fixture's own evidence ids are shorter than the
    v2 contract's minimum, so a content-bearing answer cannot be projected at
    all today (a pre-existing gap, reported separately -- it is what makes
    ``frame_construction_failed`` fire in this harness). Coverage projection
    is what this test measures, and it needs no facts.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["status"] = "partial"
    for collection in ("claims", "metrics", "evidence", "conflicts"):
        payload[collection] = []
    payload["coverage"]["unavailable_required_sources"] = ["work_item"]
    payload["coverage"]["stale_required_sources"] = ["work_graph"]
    payload["coverage"]["degraded_required_sources"] = ["status_change"]

    frame = wrap_legacy_answer_as_frame(
        DevAnswer.model_validate(payload),
        run_id="11111111-1111-4111-8111-111111111111",
        investigation_result=None,
    )

    assert frame.coverage.unavailable_required_sources == (SourceClass.WORK_ITEM,)
    assert frame.coverage.stale_required_sources == (SourceClass.WORK_GRAPH,)
    assert frame.coverage.degraded_required_sources == (SourceClass.STATUS_CHANGE,)

    # The pre-existing tool-id grammar still resolves through the same helper.
    tool_payload = deepcopy(payload)
    tool_payload["coverage"]["unavailable_required_sources"] = ["status_snapshot.v1"]
    tool_payload["coverage"]["stale_required_sources"] = []
    tool_payload["coverage"]["degraded_required_sources"] = []
    tool_frame = wrap_legacy_answer_as_frame(
        DevAnswer.model_validate(tool_payload),
        run_id="11111111-1111-4111-8111-111111111111",
        investigation_result=None,
    )
    assert tool_frame.coverage.unavailable_required_sources == (
        SourceClass.STATUS_CHANGE,
    )


def test_the_two_legacy_token_grammars_cannot_collide() -> None:
    """``_source_class_for_legacy_token`` tries ToolID then SourceClass.

    Ordering is only safe if no string satisfies both. It does not today, and
    the reason is structural rather than coincidental: every ``ToolID`` is
    dotted-and-versioned (``status_snapshot.v1``) and no ``SourceClass`` value
    contains a dot. This pins both facts, so adding a ``SourceClass`` that
    happens to spell an existing tool id -- the one way the resolver could
    silently return the wrong class -- fails here instead of misdisclosing in
    production.
    """

    tool_values = {member.value for member in ToolID}
    source_values = {member.value for member in SourceClass}
    assert tool_values & source_values == set()
    assert all("." in value for value in tool_values)
    assert all("." not in value for value in source_values)


def test_widening_the_token_resolver_is_a_strict_extension() -> None:
    """No token that already resolved keeps a different answer.

    The resolver gained a ``SourceClass`` arm, and the review's replay
    question is whether anything ALREADY persisted now projects differently.
    This reconstructs the pre-change implementation and diffs it across every
    token shape that can reach a stored v1 coverage list.

    The only inputs whose result changed are ``SourceClass`` values, and
    every one of them previously collapsed to the ``source_health`` fallback
    -- i.e. the widening rescues tokens that were being misreported and
    touches nothing else. Combined with the fact that the sole producer of
    ``SourceClass``-valued tokens is ``_coverage_with_plan_sources`` (added on
    this branch, so no pre-existing row can carry one) and that
    ``_coverage_from_tool_results`` emits ``tool_id.value`` exclusively,
    replay of anything stored before this branch is byte-identical.
    """

    fallback = tf._LEGACY_COVERAGE_FALLBACK

    def pre_change_resolver(token: str) -> SourceClass:
        try:
            tool_id = ToolID(token)
        except ValueError:
            return fallback
        return tf.SOURCE_CLASS_BY_TOOL_ID[tool_id]

    corpus = (
        [member.value for member in ToolID]
        + [member.value for member in SourceClass]
        + ["private/Nightfall", "Nightfall-deployments", "status_snapshot", ""]
    )
    changed = [
        (token, pre_change_resolver(token), tf._source_class_for_legacy_token(token))
        for token in corpus
        if pre_change_resolver(token) is not tf._source_class_for_legacy_token(token)
    ]

    # Nothing that previously produced a real (non-fallback) class moved.
    assert [item for item in changed if item[1] is not fallback] == []
    # And everything that did move is a SourceClass value now resolving truthfully.
    assert changed, "the widening must actually change something, or it is dead code"
    for token, _before, after in changed:
        assert token in {member.value for member in SourceClass}
        assert after is SourceClass(token)

    # The tool-id grammar is untouched, member by member.
    for member in ToolID:
        assert (
            tf._source_class_for_legacy_token(member.value)
            is (tf.SOURCE_CLASS_BY_TOOL_ID[member])
        )


def _observation(
    level: str, state: SourceRequirementState, *, source: SourceClass
) -> DevSourceObservation:
    """One contract-valid observation at an exact (level, state) coordinate.

    Hand-built rather than produced by the executor, and deliberately so: the
    closure table below has to cover coordinates no registered plan can
    currently reach (``optional`` at any state, ``mandatory`` at
    ``not_applicable``), which is precisely what a producer-driven fixture
    cannot supply. The *behavioural* tests above all drive the real plan
    executor; this one complements them with totality over the partition.
    """

    unmeasured = state in UNMEASURED_REQUIREMENT_STATES
    return DevSourceObservation(
        schema_version="dev_source_observation.v1",
        observation_id=str(uuid.uuid4()),
        source_class=source,
        adapter_id="status_change_service.status_snapshot.v1",
        requirement_level=level,
        observed_state=state,
        data_semantics="not_measured" if unmeasured else "no_data",
        usable_fact_count=0,
        subject_coverage=0.0 if unmeasured else 1.0,
        observed_at=_METRIC_WINDOW,
        query_version="v1",
        limitation=f"source_{state.value}" if unmeasured else None,
    )


def _investigation_with(observations: tuple[DevSourceObservation, ...]) -> Any:
    """Wrap observations in a contract-valid ``DevInvestigationResult``.

    ``plan_id``/``plan_version`` come from the real registry rather than
    being typed as literals. Both are pattern-constrained on the contract,
    and the constraint is not guessable: writing this by hand first produced
    ``"1"`` (rejected, ``min_length=3``) and then ``"1.0.0"`` (rejected, it
    must match ``^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)*\\.v\\d+(?:\\.\\d+)*$``).
    That is the hand-authored-fixture failure mode in miniature -- caught
    here by a validator, but the same guess in a place with no validator is
    how a fixture drifts away from the shape production actually emits. Only
    the coordinates no producer can reach are fabricated below; everything
    obtainable from the real thing is taken from the real thing.
    """

    plan = CORE_PLANS_BY_INTENT[QuestionIntentID.ENTITY_STATUS]
    return DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=str(uuid.uuid4()),
        plan_id=plan.plan_id,
        plan_version=plan.plan_version,
        run_id=str(uuid.uuid4()),
        subject_entity_id=None,
        observations=observations,
        completed_steps=(),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=True,
        completed_at=_METRIC_WINDOW,
    )


#: The closure table. Rows are ``requirement_level``; the value is the set of
#: ``observed_state`` members that must NOT block a complete answer at that
#: level. Every other (level, state) pair must block.
#:
#: This exists because CHAOS-3334 took three rounds of the same defect --
#: mandatory-only, then applicable-conditional, then AVAILABLE_UNKNOWN -- each
#: found by someone else after the previous fix was called done. A fourth
#: patch is not a closure argument; a partition where every cell is stated and
#: checked is.
_NON_BLOCKING: dict[str, set[SourceRequirementState]] = {
    "mandatory": {
        SourceRequirementState.AVAILABLE_CURRENT,
    },
    "conditional": {
        SourceRequirementState.AVAILABLE_CURRENT,
        # Never applicable to this subject -- the plan says it was not needed.
        SourceRequirementState.NOT_APPLICABLE,
    },
    "optional": set(SourceRequirementState),
    "not_applicable": set(SourceRequirementState),
}


@pytest.mark.parametrize("level", sorted(_NON_BLOCKING))
def test_the_blocking_partition_is_total_over_level_and_state(level: str) -> None:
    """Every (requirement_level, observed_state) cell, stated and checked.

    Drives the real ``_coverage_with_plan_sources`` over a clean 1/1
    tool-only coverage and asserts, per cell, whether a ``complete`` answer
    would still be permitted -- using the answer contract's own predicate
    (a source named in either disclosure list blocks it), not a
    reimplementation of it.
    """

    baseline = DevCoverage(
        required_source_count=1,
        available_source_count=1,
        unavailable_required_sources=[],
        stale_required_sources=[],
        as_of=_METRIC_WINDOW,
    )
    for state in SourceRequirementState:
        merged = DevOrchestrator._coverage_with_plan_sources(
            baseline,
            _investigation_with(
                (_observation(level, state, source=SourceClass.STATUS_CHANGE),)
            ),
        )
        blocks = bool(
            merged.unavailable_required_sources
            or merged.stale_required_sources
            or merged.degraded_required_sources
        )
        expected_block = state not in _NON_BLOCKING[level]
        assert blocks is expected_block, (
            f"({level}, {state.value}) should "
            f"{'block' if expected_block else 'permit'} a complete answer"
        )
        # An observation that participates must also be counted, or the
        # coverage ratio would understate what the server required.
        if level in {"mandatory", "conditional"} and not (
            level == "conditional" and state is SourceRequirementState.NOT_APPLICABLE
        ):
            assert merged.required_source_count == 2
        else:
            assert merged.required_source_count == 1


def test_every_registered_plan_contributes_a_blocking_source() -> None:
    """The merge must never be a silent no-op for a whole plan.

    ``_coverage_with_plan_sources`` returns coverage untouched when no
    observation participates. For every plan registered today that cannot
    happen -- each declares at least one mandatory source requirement -- so a
    plan-governed run always has something that can refuse a false
    ``complete``. A future plan with only conditional or optional
    requirements would silently opt out of this entire ticket's protection,
    which is exactly the kind of gap that gets noticed a wave later.
    """

    without_mandatory = sorted(
        plan.plan_id
        for plan in CORE_PLANS_BY_INTENT.values()
        if not any(
            requirement.requirement_level == "mandatory"
            for requirement in plan.source_requirements
        )
    )
    assert without_mandatory == [], (
        "these plans declare no mandatory source, so a failed investigation "
        "cannot refuse a complete answer for them; decide whether their "
        "conditional requirements should participate before registering them"
    )


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
    # CHAOS-3334 codex re-verify: its own bucket, not stale. A degraded status
    # snapshot means a contributor was UNAVAILABLE and an insufficient-evidence
    # metric can be FRESH -- calling either "stale" reports the wrong cause.
    degraded_states = {SourceRequirementState.AVAILABLE_UNKNOWN}
    available_states = {SourceRequirementState.AVAILABLE_CURRENT}
    classified = (
        UNMEASURED_REQUIREMENT_STATES
        | stale_states
        | degraded_states
        | available_states
    )
    assert set(SourceRequirementState) == classified, (
        "a SourceRequirementState member is unclassified for coverage merging; "
        "decide whether it blocks a complete answer instead of inheriting the "
        "available fall-through"
    )
    # The three buckets must be disjoint, or a state's treatment would depend
    # on branch order rather than on the vocabulary.
    buckets = (
        UNMEASURED_REQUIREMENT_STATES,
        stale_states,
        degraded_states,
        available_states,
    )
    for index, first in enumerate(buckets):
        for second in buckets[index + 1 :]:
            assert not (first & second)


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
    assert after == before + 1


def _counter_value(**labels: str) -> float:
    labelled = ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(**labels)
    value = getattr(labelled, "_value", None)
    if value is None:
        # prometheus_client is a declared test dependency; an unobservable
        # counter means this test is not measuring anything.
        pytest.fail(
            "ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL is the no-op build; the "
            "counter assertion cannot run in this environment"
        )
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
