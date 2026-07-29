from __future__ import annotations

import asyncio
import hashlib
from copy import deepcopy

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevContractVersions,
    DevMessageRequest,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.orchestrator import (
    DevOrchestrator,
    DevRunLimits,
    OrchestratorResult,
    RunState,
)
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent.contracts import (
    AgentDisambiguation,
    AgentFinalAnswer,
    AgentRefusal,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.scripted import ScriptedAgentProvider, ScriptedStep


class Recorder:
    def __init__(self, *, fail_answer_write: bool = False) -> None:
        self.transitions: list[RunState] = []
        self.tools: list[DevToolRequest] = []
        self.answers: list[DevAnswer] = []
        self.terminals: list[RunState] = []
        self.fail_answer_write = fail_answer_write

    async def transition(self, state: RunState) -> None:
        self.transitions.append(state)

    async def record_tool(self, **values) -> None:
        self.tools.append(values["request"])

    async def record_answer(self, answer: DevAnswer) -> None:
        if self.fail_answer_write:
            raise RuntimeError("storage unavailable")
        self.answers.append(answer)

    async def terminal(self, **values) -> None:
        self.terminals.append(values["state"])


def _request() -> DevMessageRequest:
    return DevMessageRequest.model_validate(
        positive_fixtures()["dev_message_request.v1"]
    )


def _resolution() -> DevScopeResolution:
    return DevScopeResolution.model_validate(
        positive_fixtures()["dev_scope_resolution.v1"]
    )


def _versions() -> DevContractVersions:
    return DevContractVersions.model_validate(
        positive_fixtures()["dev_answer.v1"]["versions"]
    )


def _fingerprint(script_id: str) -> str:
    return hashlib.sha256(script_id.encode()).hexdigest()[:24]


def _answer(*, script_id: str, invalid_schema: bool = False) -> dict:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["model"] = {
        "provider_source": "platform",
        "provider_family": "scripted",
        "model_fingerprint": _fingerprint(script_id),
    }
    if invalid_schema:
        payload.pop("direct_summary")
    return payload


def _registry(*, calls: list[DevToolRequest] | None = None) -> AskDevToolRegistry:
    async def execute(_context, request: DevToolRequest) -> DevToolResult:
        if calls is not None:
            calls.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


def _orchestrator(
    steps: list[ScriptedStep],
    *,
    script_id: str,
    recorder: Recorder | None = None,
    registry: AskDevToolRegistry | None = None,
    limits: DevRunLimits | None = None,
) -> DevOrchestrator:
    async def resolve(**_values) -> DevScopeResolution:
        return _resolution()

    return DevOrchestrator(
        provider=ScriptedAgentProvider(steps, script_id=script_id),
        provider_source="platform",
        provider_family="scripted",
        registry=registry or _registry(),
        scope_resolver=resolve,
        versions=_versions(),
        recorder=recorder,
        limits=limits,
    )


async def _run(orchestrator: DevOrchestrator, cancellation=None) -> OrchestratorResult:
    return await orchestrator.run(
        request=_request(),
        org_id="org_fullchaos",
        user_id="user_01",
        permission_fingerprint="permissions_01",
        run_id="run_01",
        conversation_id="conversation_01",
        answer_id="answer_01",
        cancellation=cancellation or asyncio.Event(),
    )


@pytest.mark.asyncio
async def test_scripted_tool_to_validated_answer_exercises_the_state_machine() -> None:
    script_id = "valid-tool-answer"
    recorder = Recorder()
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={"metric_id": "items_completed", "limit": 12},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(decision=AgentFinalAnswer(_answer(script_id=script_id))),
            ],
            script_id=script_id,
            recorder=recorder,
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.tool_call_count == 1
    assert result.usage.input_tokens == 100
    assert recorder.transitions == [event.state for event in result.events[:-1]]
    assert RunState.TOOL_VALIDATION in recorder.transitions
    assert RunState.TOOL_EXECUTION in recorder.transitions
    assert RunState.ANSWER_VALIDATION in recorder.transitions
    assert recorder.terminals == [RunState.COMPLETED]
    assert len(recorder.answers) == 1


@pytest.mark.asyncio
async def test_schema_only_failure_gets_exactly_one_repair_attempt() -> None:
    script_id = "schema-repair"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={"metric_id": "items_completed", "limit": 12},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer(script_id=script_id, invalid_schema=True)
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(_answer(script_id=script_id))),
            ],
            script_id=script_id,
        )
    )
    assert result.state is RunState.COMPLETED
    assert [event.state for event in result.events].count(
        RunState.ANSWER_VALIDATION
    ) == 2


@pytest.mark.asyncio
async def test_retryable_provider_failure_is_retried_once_without_spending_a_round() -> (
    None
):
    script_id = "retry-once"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    error=AgentProviderError(
                        AgentProviderErrorCode.TIMEOUT, retryable=True
                    )
                ),
                ScriptedStep(decision=AgentRefusal(code="unsupported", message="no")),
            ],
            script_id=script_id,
            limits=DevRunLimits(model_rounds=1),
        )
    )
    assert result.state is RunState.REFUSED
    assert result.error is not None
    assert [event.state for event in result.events].count(RunState.MODEL_DECISION) == 1


@pytest.mark.asyncio
async def test_third_identical_tool_request_trips_loop_guard_before_execution() -> None:
    script_id = "loop-guard"
    calls: list[DevToolRequest] = []
    repeated = [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id="query_metric.v1",
                arguments={"metric_id": "items_completed", "limit": 12},
                call_id=f"tool_call_{index}",
            )
        )
        for index in range(3)
    ]
    result = await _run(
        _orchestrator(
            repeated,
            script_id=script_id,
            registry=_registry(calls=calls),
        )
    )
    assert result.state is RunState.FAILED
    assert result.error is not None and result.error.code == "tool_limit_reached"
    assert len(calls) == 2
    assert result.tool_call_count == 2


@pytest.mark.asyncio
async def test_unknown_tool_and_arguments_never_reach_an_executor() -> None:
    calls: list[DevToolRequest] = []
    for decision in (
        AgentToolRequest(tool_id="shell.v1", arguments={}, call_id="call_01"),
        AgentToolRequest(
            tool_id="query_metric.v1",
            arguments={"metric_id": "items_completed", "sql": "drop table"},
            call_id="call_02",
        ),
    ):
        result = await _run(
            _orchestrator(
                [ScriptedStep(decision=decision)],
                script_id=f"reject-{decision.call_id}",
                registry=_registry(calls=calls),
            )
        )
        assert result.state is RunState.FAILED
    assert calls == []


@pytest.mark.asyncio
async def test_pre_cancelled_request_has_one_terminal_write_and_no_provider_call() -> (
    None
):
    recorder = Recorder()
    cancellation = asyncio.Event()
    cancellation.set()
    result = await _run(
        _orchestrator([], script_id="cancelled", recorder=recorder),
        cancellation,
    )
    assert result.state is RunState.CANCELLED
    assert recorder.terminals == [RunState.CANCELLED]
    assert result.tool_call_count == 0


@pytest.mark.asyncio
async def test_answer_storage_failure_becomes_one_safe_failed_terminal() -> None:
    script_id = "answer-write-failure"
    recorder = Recorder(fail_answer_write=True)
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={"metric_id": "items_completed", "limit": 12},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(_answer(script_id=script_id))),
            ],
            script_id=script_id,
            recorder=recorder,
        )
    )
    assert result.state is RunState.FAILED
    assert result.answer is None
    assert result.error is not None and result.error.code == "internal_error"
    assert recorder.terminals == [RunState.FAILED]


@pytest.mark.asyncio
async def test_provider_usage_cannot_cross_the_server_owned_cost_budget() -> None:
    script_id = "cost-budget"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentRefusal(code="unsupported", message="no"),
                    usage=AgentUsage(estimated_cost_microusd=2_000_000),
                )
            ],
            script_id=script_id,
            limits=DevRunLimits(
                max_estimated_cost_microusd=1_500_000,
                estimated_cost_per_call_microusd=1_000_000,
            ),
        )
    )
    assert result.state is RunState.FAILED
    assert result.error is not None and result.error.code == "cost_limit_reached"


@pytest.mark.asyncio
async def test_budget_exhaustion_after_grounded_tool_data_returns_bounded_partial() -> (
    None
):
    script_id = "grounded-budget-partial"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={"metric_id": "items_completed", "limit": 12},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(estimated_cost_microusd=600_000),
                )
            ],
            script_id=script_id,
            limits=DevRunLimits(
                max_estimated_cost_microusd=1_500_000,
                estimated_cost_per_call_microusd=1_000_000,
            ),
        )
    )
    assert result.state is RunState.COMPLETED
    assert result.answer is not None and result.answer.status.value == "partial"
    assert result.answer.metrics[0].metric_id.value == "items_completed"
    assert result.answer.claims == []


@pytest.mark.asyncio
async def test_disambiguation_is_a_typed_insufficient_evidence_terminal() -> None:
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentDisambiguation(
                        prompt="Which repository?", candidates=("repo_a", "repo_b")
                    )
                )
            ],
            script_id="disambiguation",
        )
    )
    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None and result.error.code == "scope_ambiguous"


@pytest.mark.asyncio
async def test_provider_timeout_is_caller_enforced_and_terminal_once() -> None:
    recorder = Recorder()
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentRefusal(code="late", message="late"),
                    delay_seconds=1,
                )
            ],
            script_id="provider-timeout",
            recorder=recorder,
            limits=DevRunLimits(provider_seconds=0.01, provider_retries=0),
        )
    )
    assert result.state is RunState.FAILED
    assert result.error is not None and result.error.code == "provider_unavailable"
    assert recorder.terminals == [RunState.FAILED]


@pytest.mark.asyncio
async def test_operator_downward_per_tool_byte_limit_is_enforced() -> None:
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={"metric_id": "items_completed", "limit": 12},
                        call_id="tool_call_01",
                    )
                )
            ],
            script_id="small-tool-budget",
            limits=DevRunLimits(per_tool_bytes=1),
        )
    )
    assert result.state is RunState.FAILED
    assert result.error is not None and result.error.code == "tool_limit_reached"


def test_hard_defaults_can_only_be_configured_downward() -> None:
    assert DevRunLimits().model_rounds == 4
    assert DevRunLimits(model_rounds=2).model_rounds == 2
    with pytest.raises(ValueError, match="configured downward"):
        DevRunLimits(model_rounds=5)
