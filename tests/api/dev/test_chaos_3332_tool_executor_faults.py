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
import traceback
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
    ToolExecutionCancelled,
    ToolRegistryError,
    ToolResultRejected,
    UnknownToolError,
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

    Mutation kill site (observed 2026-08-02): reverting ``orchestrator.run``
    to the exact pre-fix shape -- no executor-fault handler, no
    ``ToolRegistryError`` re-raise, no catch-all logging or counters -- and
    replaying *this* TEAM run through the harness produces::

        pre-fix   state=failed     code=internal_error  tools=0  log_records=0
        post-fix  state=completed  code=None            tools=2  log_records=2

    The pre-fix row is the CHAOS-3332 field signature exactly as the ticket
    recorded it, and it fails this test at the terminal-state assertion.
    (The second post-fix record is a pre-existing
    ``frame_construction_failed`` from the harness's answer fixture; it
    reproduces identically on a clean no-fault baseline and is unrelated to
    this fix.)
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
    assert _extra(faults[0], "run_id") == result.run_id
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


class ClickHouseDsnLeak(RuntimeError):
    """An executor fault whose *class name* is as sensitive as its message.

    Named so the containment test below can prove neither the exception text
    nor its type name escapes into anything the model or the user sees --
    ``exception_type`` is deliberately carried on the log record and the
    metric label, and nowhere else.
    """


@pytest.mark.asyncio
async def test_no_exception_detail_reaches_any_model_or_user_visible_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Containment control: only the safe code crosses the boundary.

    Exception strings routinely carry DSNs, SQL, and internal identifiers, and
    the exception *class name* leaks implementation shape on its own. The
    degraded tool result is serialised straight into the next prompt, and the
    answer and frame are persisted and replayed to the user, so none of the
    three may contain either.

    The same two details must simultaneously be *present* in the operator
    channel -- otherwise "nothing leaked" would also be satisfied by a fix
    that threw the diagnosis away entirely, which is the CHAOS-3332 defect
    wearing different clothes.
    """

    secret = "clickhouse dsn user=svc_askdev password=hunter2"
    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id="chaos3332-no-echo",
            script=_script(ToolID.DATA_HEALTH, ToolID.STATUS_SNAPSHOT),
            recorder_factory=ToolResultRecorder,
            registry_factory=_registry_with_fault(
                ToolID.DATA_HEALTH, ClickHouseDsnLeak(secret)
            ),
        )

    recorder = output.recorder
    assert isinstance(recorder, ToolResultRecorder)
    degraded = recorder.executions[0].result
    assert degraded.error is not None
    # The whole user/model-visible surface: the tool result handed to the
    # provider, plus every persisted answer and frame the user can replay.
    surfaces = {
        "degraded tool result": degraded.model_dump_json(),
        **{
            f"persisted answer {index}": answer.model_dump_json()
            for index, answer in enumerate(recorder.answers)
        },
        **{
            f"persisted frame {index}": frame.model_dump_json()
            for index, frame in enumerate(recorder.frames)
        },
    }
    # An empty surface set would make every assertion below vacuous.
    assert len(surfaces) > 1, "the run must have persisted an answer or a frame"
    for name, payload in surfaces.items():
        for forbidden in (secret, "hunter2", "svc_askdev", "ClickHouseDsnLeak"):
            assert forbidden not in payload, f"{forbidden!r} leaked into {name}"

    # Fixed, server-authored text -- not a formatted exception.
    assert degraded.error.code == "source_unavailable"
    assert degraded.error.safe_message == "The tool did not produce a result."

    # ...and the detail really did reach the operator channel.
    faults = _records(caplog, "ask_dev.orchestrator.tool_executor_fault")
    assert len(faults) == 1
    assert _extra(faults[0], "exception_type") == "ClickHouseDsnLeak"
    assert faults[0].exc_info is not None
    assert secret in "".join(traceback.format_exception(*faults[0].exc_info))


#: Every registry-level exception class that is fatal *by design* (see
#: ``_rejected_tool_execution``'s docstring), with the terminal each one must
#: still reach. Enumerated rather than sampled: the new degrade path sits
#: directly above these in the same try, so one of them silently becoming
#: degradable is the exact regression this fix could introduce.
FATAL_REGISTRY_FAULTS = [
    pytest.param(
        ToolExecutionCancelled("tool execution cancelled"),
        RunState.CANCELLED,
        "cancelled",
        id="cancelled",
    ),
    pytest.param(
        ToolResultRejected("tool result type does not match request"),
        RunState.FAILED,
        "tool_unavailable",
        id="result-rejected",
    ),
    pytest.param(
        UnknownToolError("tool is not registered"),
        RunState.FAILED,
        "tool_unavailable",
        id="unknown-tool",
    ),
    pytest.param(
        ToolRegistryError("registry failure"),
        RunState.FAILED,
        "tool_unavailable",
        id="registry-error",
    ),
]


@pytest.mark.parametrize(
    ("fault", "expected_state", "expected_code"), FATAL_REGISTRY_FAULTS
)
@pytest.mark.asyncio
async def test_registry_level_faults_stay_fatal(
    fault: BaseException,
    expected_state: RunState,
    expected_code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative control bounding the new handler's blast radius.

    A malformed tool result, an unregistered tool, or a cancellation are
    run-level conditions that must end the run, not one degraded call. Were
    the new ``except Exception`` swallowing ``ToolRegistryError`` subclasses
    too, every one of these would be laundered into an ordinary failed call
    and the run would answer over it. This is why the handler names that class
    explicitly and re-raises, rather than relying on clause order alone.

    Each stays fatal under its own pre-existing, *typed* terminal -- never the
    ``internal_error`` bucket -- which is also why none of them needs the new
    catch-all logging: an operator can already tell these runs apart by error
    code.

    Mutation kill site: deleting the ``except ToolRegistryError: raise``
    clause fails every parameter here -- each run reaches ``COMPLETED`` with a
    degraded call instead of its terminal.
    """

    with caplog.at_level(logging.ERROR):
        output = await run_preflight_orchestrator(
            question=TEAM_QUESTION,
            entities=TEAM_ENTITIES,
            script_id=f"chaos3332-fatal-{expected_code}",
            script=_script(ToolID.DATA_HEALTH, ToolID.STATUS_SNAPSHOT),
            recorder_factory=ToolResultRecorder,
            registry_factory=_registry_with_fault(ToolID.DATA_HEALTH, fault),
        )

    result = output.result
    assert result.state is expected_state
    assert result.error is not None
    assert result.error.code == expected_code
    assert result.answer is None
    # The run died on the first call: nothing was degraded, and the second
    # scripted tool never ran.
    assert result.tool_call_count == 0
    recorder = output.recorder
    assert isinstance(recorder, ToolResultRecorder)
    assert recorder.executions == []
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
    # Correlated to *this* run, not merely "a log line existed somewhere":
    # an internal_error an operator cannot tie back to the run that produced
    # it is the same dead end CHAOS-3332 was, one step removed.
    assert _extra(unhandled[0], "run_id") == result.run_id
    assert _extra(unhandled[0], "exception_type") == "PersistenceBoom"
    assert unhandled[0].exc_info is not None
    assert _extra(unhandled[0], "terminal_written") is False
    # The exception really is the one that terminated the run.
    assert unhandled[0].exc_info[0] is PersistenceBoom

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
