"""CHAOS-3332: a tool executor that breaks its contract must not kill the run.

The live defect: every status question naming a real TEAM subject terminated
as ``internal_error`` with **zero** log lines. A committed team is a real v1
direct scope since CHAOS-3301, but no registered metric lists
``DirectScope.TEAM`` in its ``supported_scopes``, so
``MetricQueryService._validate_request`` raised a bare ``ValueError`` on the
run's first ``query_metric.v1`` call. ``AskDevToolRegistry.execute`` re-raises
an executor's exception verbatim, the orchestrator's executor call site caught
only ``ToolRequestRejected``/``ToolExecutionTimedOut``, and the catch-all at
the bottom of ``run()`` mapped whatever arrived to ``internal_error`` while
logging nothing at all.

Every test here drives the real ``DevOrchestrator.run`` through the shared
CHAOS-3292 harness -- never a diagnostic that pokes at internals -- and
asserts executed behaviour: which terminal the run reached, what the model was
handed about the failed call, and what an operator can see afterwards.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    DevScope,
    DevToolRequest,
    ToolID,
)
from dev_health_ops.api.dev.metrics.service import (
    MetricQueryRequest,
    MetricQueryService,
    RawMetricResult,
)
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.tool_registry import (
    AskDevToolRegistry,
    ToolExecution,
    ToolResultRejected,
)
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from dev_health_ops.metrics.prometheus import (
    ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL,
    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    PLATFORM_TEAM,
    Recorder,
    answer_payload,
    run_preflight_orchestrator,
    stock_executor,
)

#: The literal message ``MetricQueryService._validate_request`` raises for a
#: direct scope outside a metric's ``supported_scopes``
#: (``api/dev/metrics/service.py``). Pinned so this suite keeps reproducing the
#: *observed* production exception rather than a stand-in that merely happens
#: to be an exception too.
UNSUPPORTED_SCOPE_MESSAGE = "Metric does not support the resolved direct scope"

TEAM_QUESTION = "What's the status of the Platform team?"
TEAM_ENTITIES = [(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, PLATFORM_TEAM)]


class ToolResultRecorder(Recorder):
    """Captures what ``record_tool`` was handed for each executed call.

    The degraded result the orchestrator synthesises is exactly what goes into
    ``provider_continuation``, and therefore into the next prompt, so this is
    the seam where its prompt-visible content can be asserted directly rather
    than inferred.
    """

    def __init__(self) -> None:
        super().__init__()
        self.executions: list[ToolExecution] = []

    async def record_tool(self, **values: Any) -> None:
        self.executions.append(values["execution"])


def _registry_with_fault(
    faulting_tool: ToolID, exc: BaseException
) -> Callable[[list[DevToolRequest]], AskDevToolRegistry]:
    """The stock registry with exactly one executor raising ``exc``.

    Only one tool faults, because the claim under test is that a *single*
    broken call degrades while the rest of the run proceeds. A registry where
    everything raises could not distinguish "the run continued" from "the run
    happened to end anyway".
    """

    def factory(calls: list[DevToolRequest]) -> AskDevToolRegistry:
        stock = stock_executor(calls)

        async def faulting(_context: Any, request: DevToolRequest) -> Any:
            calls.append(request)
            raise exc

        return AskDevToolRegistry(
            {
                tool_id: (faulting if tool_id is faulting_tool else stock)
                for tool_id in ToolID
            }
        )

    return factory


def _script(
    *tool_ids: ToolID, status: str = "partial"
) -> Callable[[str], list[ScriptedStep]]:
    """Call each named tool once, then answer with ``status``.

    ``status`` defaults to ``partial`` because that is the truthful answer
    shape once one of the run's tool calls failed: the answer validator
    rejects a ``complete`` answer whose required sources are not all fresh and
    available, and ``test_a_complete_answer_is_still_rejected_after_a_fault``
    below is the control proving the degraded call really does reach coverage
    rather than being laundered away.

    Extra final-answer steps are appended because a rejected-then-repaired
    answer consumes a further provider round; without them the script would
    empty and the run would fail as ``INVALID_RESPONSE``, which would mask
    what is actually being measured.
    """

    def build(script_id: str) -> list[ScriptedStep]:
        steps = [
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id=tool_id.value,
                    arguments=(
                        # query_metric.v1's registered max_items is 12, below
                        # DevToolRequest.limit's default of 25 -- an omitted
                        # limit is rejected by the registry before the
                        # executor ever runs, which would silently move the
                        # test's throw site.
                        {"metric_id": "items_completed", "limit": 12}
                        if tool_id is ToolID.QUERY_METRIC
                        else {"limit": 25, "include_comparison": False}
                    ),
                    call_id=f"tool_call_{index:02d}",
                ),
                usage=AgentUsage(input_tokens=100, output_tokens=10),
            )
            for index, tool_id in enumerate(tool_ids, start=1)
        ]
        payload = deepcopy(answer_payload(script_id=script_id))
        payload["status"] = status
        steps.extend(
            ScriptedStep(decision=AgentFinalAnswer(deepcopy(payload))) for _ in range(4)
        )
        return steps

    return build


def _committed_team_scope() -> DevScope:
    """The exact TEAM scope production commits, built by the real producer.

    ``ScopeResolutionService.committed_resolution_for`` is the one place an
    exact-match committed scope is constructed -- it is what the subject
    preflight itself calls -- so this is the real wire shape, not a
    hand-authored ``DevScope`` free to drift from it.
    """

    base = DevScope.model_validate(
        {
            "schema_version": "dev_scope.v1",
            "organization_id": ORG_ID,
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "team_ids": [],
            "time_range": {
                "start": "2026-07-01T00:00:00Z",
                "end": "2026-08-01T00:00:00Z",
                "timezone": "UTC",
            },
        }
    )
    service = ScopeResolutionService.__new__(ScopeResolutionService)
    resolution = service.committed_resolution_for(
        AuthorizedEntity(
            EntityKind.TEAM, PLATFORM_TEAM.canonical_id, PLATFORM_TEAM.label
        ),
        org_id=ORG_ID,
        base_scope=base,
        resolved_at=datetime.now(UTC),
    )
    scope = resolution.resolved_scope
    assert scope is not None
    return scope


def _records(caplog: pytest.LogCaptureFixture, message: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.message == message]


def _extra(record: logging.LogRecord, field: str) -> Any:
    """One structured ``extra=`` field off a log record.

    ``logging.LogRecord`` carries ``extra`` keys as dynamically-set
    attributes, which mypy cannot see; going through ``getattr`` keeps the
    assertions honest (a missing field still raises) without an ignore
    comment that would also hide a real typo.
    """

    return getattr(record, field)


def _counter(counter: Any, **labels: str) -> float:
    """Read one labelled counter, tolerating the no-op build.

    ``prometheus_client`` is an optional dependency; ``metrics.prometheus``
    substitutes no-op counters without it, and a test that silently read 0.0
    from a no-op would report coverage for a signal it never measured. This
    returns ``float('nan')`` there instead, and the callers skip rather than
    assert -- a measurement that did not happen must not look like a pass.
    """

    labelled = counter.labels(**labels)
    value = getattr(labelled, "_value", None)
    if value is None:
        return float("nan")
    return float(value.get())


@pytest.mark.asyncio
async def test_faulting_tool_call_degrades_and_the_run_still_answers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The headline control: one broken call, and the run still reaches an answer.

    Mutation kill site: deleting the ``except Exception`` executor-fault
    handler from ``orchestrator.run`` -- so the exception falls through to the
    catch-all, which is exactly the pre-fix code -- fails this at the
    terminal-state assertion with ``RunState.FAILED``/``internal_error``
    against the expected ``RunState.COMPLETED``. That observed pair is the
    CHAOS-3332 production symptom.
    """

    before = _counter(
        ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL,
        tool_id=ToolID.DATA_HEALTH.value,
        exception_type="ValueError",
    )
    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id="chaos3332-degrade",
            script=_script(ToolID.DATA_HEALTH, ToolID.STATUS_SNAPSHOT),
            recorder_factory=ToolResultRecorder,
            registry_factory=_registry_with_fault(
                ToolID.DATA_HEALTH, ValueError(UNSUPPORTED_SCOPE_MESSAGE)
            ),
        )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.answer is not None
    # Honest, not laundered: the run answers, and says so partially.
    assert result.answer.status is AnswerStatus.PARTIAL

    # The run continued *past* the fault -- the second call actually executed.
    # This is the direct evidence that the fault was per-call rather than
    # run-fatal; a terminal-state assertion alone could not tell the two apart.
    assert result.tool_call_count == 2
    recorder = output.recorder
    assert isinstance(recorder, ToolResultRecorder)
    degraded, succeeded = recorder.executions
    assert degraded.result.status == "error"
    assert degraded.result.error is not None
    assert degraded.result.error.code == "source_unavailable"
    assert succeeded.result.status == "success"

    faults = _records(caplog, "ask_dev.orchestrator.tool_executor_fault")
    assert len(faults) == 1
    assert faults[0].levelno == logging.ERROR
    assert _extra(faults[0], "exception_type") == "ValueError"
    assert _extra(faults[0], "tool_id") == ToolID.DATA_HEALTH.value
    assert _extra(faults[0], "run_id") == "run_01"
    # logger.exception, not logger.error: without the traceback an operator
    # still cannot find the throw site, and that was half of CHAOS-3332.
    assert faults[0].exc_info is not None
    # The run reached a normal terminal, so the catch-all must not have fired.
    assert _records(caplog, "ask_dev.orchestrator.unhandled_run_fault") == []

    after = _counter(
        ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL,
        tool_id=ToolID.DATA_HEALTH.value,
        exception_type="ValueError",
    )
    if after == after:  # False only for the no-op-counter build (NaN)
        assert after == before + 1


@pytest.mark.asyncio
async def test_a_complete_answer_is_still_rejected_after_a_fault(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Anti-laundering control for the degrade path.

    Degrading a broken call must not make it *disappear*: the failed result
    still counts against required-source coverage, so an answer claiming
    ``complete`` is refused exactly as it would be after any other failed
    tool call. Without this, the fix above could have been satisfied by
    dropping the faulted call entirely -- which would let a run assert a
    complete answer over evidence it never obtained.

    It also pins the second half of the CHAOS-3332 improvement: the run that
    does fail now fails with a *typed* reason, never the opaque
    ``internal_error`` bucket.
    """

    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id="chaos3332-complete-rejected",
            script=_script(
                ToolID.DATA_HEALTH, ToolID.STATUS_SNAPSHOT, status="complete"
            ),
            registry_factory=_registry_with_fault(
                ToolID.DATA_HEALTH, ValueError(UNSUPPORTED_SCOPE_MESSAGE)
            ),
        )

    result = output.result
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "answer_validation_failed"
    assert result.answer is None
    assert len(_records(caplog, "ask_dev.orchestrator.tool_executor_fault")) == 1


@pytest.mark.asyncio
async def test_degraded_tool_result_never_echoes_the_exception_text() -> None:
    """The model is told the call failed, never *why* in server terms.

    Exception strings routinely carry DSNs, SQL, and internal identifiers.
    The degraded result is serialised straight into the next prompt, so its
    ``safe_message`` must be fixed, server-authored text.
    """

    secret = "clickhouse dsn user=svc_askdev password=hunter2"
    output = await run_preflight_orchestrator(
        question=TEAM_QUESTION,
        entities=TEAM_ENTITIES,
        script_id="chaos3332-no-echo",
        script=_script(ToolID.DATA_HEALTH, ToolID.STATUS_SNAPSHOT),
        recorder_factory=ToolResultRecorder,
        registry_factory=_registry_with_fault(ToolID.DATA_HEALTH, RuntimeError(secret)),
    )

    recorder = output.recorder
    assert isinstance(recorder, ToolResultRecorder)
    serialized = recorder.executions[0].result.model_dump_json()
    assert secret not in serialized
    assert "hunter2" not in serialized
    assert "svc_askdev" not in serialized


@pytest.mark.asyncio
async def test_registry_level_faults_stay_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative control bounding the new handler's blast radius.

    ``ToolResultRejected`` means the executor produced output the registry
    could not trust -- a run-level defect that is fatal *by design* (see
    ``_rejected_tool_execution``'s docstring). Were the new catch swallowing
    ``ToolRegistryError`` subclasses too, a malformed tool result would be
    laundered into an ordinary degraded call and the run would answer over
    it. This test is why the handler names that class explicitly rather than
    relying on clause order alone.

    It stays fatal under its own pre-existing, *typed* terminal
    (``tool_unavailable``, from ``run()``'s ``except ToolRegistryError``
    clause) rather than the ``internal_error`` bucket -- which is why it never
    needed the new catch-all logging and must not acquire it: an operator can
    already tell these runs apart by error code alone.
    """

    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id="chaos3332-fatal",
            script=_script(ToolID.DATA_HEALTH, ToolID.STATUS_SNAPSHOT),
            registry_factory=_registry_with_fault(
                ToolID.DATA_HEALTH,
                ToolResultRejected("tool result type does not match request"),
            ),
        )

    result = output.result
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "tool_unavailable"
    assert result.answer is None
    # The run died on the first call: nothing was degraded, and the second
    # scripted tool never ran.
    assert result.tool_call_count == 0
    assert _records(caplog, "ask_dev.orchestrator.tool_executor_fault") == []
    assert _records(caplog, "ask_dev.orchestrator.unhandled_run_fault") == []


@pytest.mark.asyncio
async def test_catch_all_internal_error_is_never_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Fix #2 in isolation, via a fault nowhere near the tool registry.

    A persistence failure inside ``record_tool`` is neither an executor fault
    nor a registry fault, so it reaches the catch-all by a completely
    different route. It must still terminate the run *loudly*.

    Mutation kill site: deleting the ``logger.exception`` call from the
    catch-all fails this at ``len(unhandled) == 1`` (observed 0) -- a
    measurement that did not happen must FAIL, and this one does.
    """

    class PersistenceBoom(Exception):
        pass

    class BrokenRecorder(Recorder):
        async def record_tool(self, **values: Any) -> None:
            del values
            raise PersistenceBoom("dev_run_tools insert failed")

    before = _counter(
        ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL, exception_type="PersistenceBoom"
    )
    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id="chaos3332-catchall",
            script=_script(ToolID.STATUS_SNAPSHOT),
            recorder_factory=BrokenRecorder,
        )

    result = output.result
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "internal_error"
    unhandled = _records(caplog, "ask_dev.orchestrator.unhandled_run_fault")
    assert len(unhandled) == 1
    assert _extra(unhandled[0], "exception_type") == "PersistenceBoom"
    assert unhandled[0].exc_info is not None
    assert _extra(unhandled[0], "terminal_written") is False

    after = _counter(
        ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL, exception_type="PersistenceBoom"
    )
    if after == after:  # False only for the no-op-counter build (NaN)
        assert after == before + 1


@pytest.mark.asyncio
async def test_committed_team_subject_survives_the_real_metric_service(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The filed regression, with no stub standing in for the throw.

    ``query_metric.v1`` is wired to the **real** ``MetricQueryService`` and
    handed the **real** committed TEAM scope, so this reproduces the
    production exception instead of imitating it. The assertion is the durable
    invariant -- a committed team subject never takes the run down -- which
    stays honest if TEAM is later added to the metric registry's
    ``supported_scopes`` and the call simply starts succeeding.
    """

    scope = _committed_team_scope()

    class UnreachableSource:
        """A ``MetricSource`` that fails loudly if it is ever consulted.

        ``_validate_request`` must reject the unsupported TEAM direct scope
        before any read, so both protocol members are tripwires: a silently
        returning stub would let this test pass even if the rejection moved
        after the source call.
        """

        async def watermark(self, *args: Any, **kwargs: Any) -> str:
            raise AssertionError("scope validation must reject before any source read")

        async def query(self, *args: Any, **kwargs: Any) -> RawMetricResult:
            raise AssertionError("scope validation must reject before any source read")

    metric_service = MetricQueryService(UnreachableSource())

    def factory(calls: list[DevToolRequest]) -> AskDevToolRegistry:
        stock = stock_executor(calls)

        async def query_metric(_context: Any, request: DevToolRequest) -> Any:
            calls.append(request)
            return await metric_service.query(
                ORG_ID,
                "permissions_01",
                MetricQueryRequest(
                    metric_id="items_completed",
                    scope=scope,
                    include_comparison=False,
                ),
            )

        return AskDevToolRegistry(
            {
                tool_id: (query_metric if tool_id is ToolID.QUERY_METRIC else stock)
                for tool_id in ToolID
            }
        )

    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id="chaos3332-real-metric-service",
            script=_script(ToolID.QUERY_METRIC, ToolID.STATUS_SNAPSHOT),
            recorder_factory=ToolResultRecorder,
            registry_factory=factory,
        )

    result = output.result
    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert [request.tool_id for request in output.calls] == [
        ToolID.QUERY_METRIC,
        ToolID.STATUS_SNAPSHOT,
    ]
    recorder = output.recorder
    assert isinstance(recorder, ToolResultRecorder)
    assert recorder.executions[0].result.status == "error"
    faults = _records(caplog, "ask_dev.orchestrator.tool_executor_fault")
    assert [_extra(record, "exception_type") for record in faults] == ["ValueError"]
    assert [_extra(record, "tool_id") for record in faults] == [
        ToolID.QUERY_METRIC.value
    ]
