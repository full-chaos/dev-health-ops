from __future__ import annotations

import asyncio
import hashlib
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

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
from dev_health_ops.api.dev.prompts import PromptConversationTurn
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry, ToolRequestRejected
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
        self.executions: list[Any] = []
        self.answers: list[DevAnswer] = []
        self.terminals: list[RunState] = []
        self.fail_answer_write = fail_answer_write

    async def transition(self, state: RunState) -> None:
        self.transitions.append(state)

    async def record_tool(self, **values) -> None:
        self.tools.append(values["request"])
        self.executions.append(values["execution"])

    async def record_answer(self, answer: DevAnswer) -> None:
        if self.fail_answer_write:
            raise RuntimeError("storage unavailable")
        self.answers.append(answer)

    async def terminal(self, **values) -> None:
        self.terminals.append(values["state"])


class RecordingProvider:
    """Wraps ScriptedAgentProvider to capture the exact messages sent on
    each decide() call, so a test can assert on the repair-prompt content
    the orchestrator built (CHAOS-3288)."""

    def __init__(self, steps: list[ScriptedStep], *, script_id: str) -> None:
        self._inner = ScriptedAgentProvider(steps, script_id=script_id)
        self.calls: list[tuple[Any, ...]] = []

    @property
    def capabilities(self):
        return self._inner.capabilities

    @property
    def provider_fingerprint(self) -> str:
        return self._inner.provider_fingerprint

    @property
    def model_fingerprint(self) -> str:
        return self._inner.model_fingerprint

    async def decide(self, **kwargs):
        self.calls.append(tuple(kwargs["messages"]))
        return await self._inner.decide(**kwargs)

    async def aclose(self) -> None:
        await self._inner.aclose()


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


def test_provider_tool_schema_exposes_only_registered_model_arguments() -> None:
    metric = DevOrchestrator._provider_tool_input_schema(ToolID.QUERY_METRIC, 12)
    evidence = DevOrchestrator._provider_tool_input_schema(ToolID.SEARCH_EVIDENCE, 25)

    assert set(metric["properties"]) == {
        "metric_id",
        "include_comparison",
        "limit",
    }
    assert metric["properties"]["limit"]["enum"] == list(range(1, 13))
    assert set(evidence["properties"]) == {"query", "limit"}
    for server_owned in {
        "schema_version",
        "run_id",
        "tool_call_id",
        "tool_id",
        "scope",
    }:
        assert server_owned not in metric["properties"]
        assert server_owned not in evidence["properties"]


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


def _tool_result(**overrides: object) -> DevToolResult:
    payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
    payload.update(overrides)
    return DevToolResult.model_validate(payload)


def _tool_request(**overrides: object) -> DevToolRequest:
    payload = deepcopy(positive_fixtures()["dev_tool_request.v1"])
    payload.update(overrides)
    return DevToolRequest.model_validate(payload)


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
    provider: Any | None = None,
    scope_resolver: Any | None = None,
) -> DevOrchestrator:
    async def resolve(**_values) -> DevScopeResolution:
        return _resolution()

    return DevOrchestrator(
        provider=provider or ScriptedAgentProvider(steps, script_id=script_id),
        provider_source="platform",
        provider_family="scripted",
        registry=registry or _registry(),
        scope_resolver=scope_resolver or resolve,
        versions=_versions(),
        recorder=recorder,
        limits=limits,
    )


async def _run(
    orchestrator: DevOrchestrator,
    cancellation=None,
    *,
    request: DevMessageRequest | None = None,
    prior_turns: tuple = (),
) -> OrchestratorResult:
    return await orchestrator.run(
        request=request or _request(),
        org_id="org_fullchaos",
        user_id="user_01",
        permission_fingerprint="permissions_01",
        run_id="run_01",
        conversation_id="conversation_01",
        answer_id="answer_01",
        cancellation=cancellation or asyncio.Event(),
        prior_turns=prior_turns,
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
async def test_orchestrator_overwrites_all_server_owned_answer_metadata() -> None:
    script_id = "server-owned-answer-metadata"
    candidate = _answer(script_id=script_id)
    candidate.update(
        {
            "schema_version": "attacker_schema",
            "answer_id": "attacker_answer",
            "conversation_id": "attacker_conversation",
            "resolved_scope": positive_fixtures()["dev_scope_resolution.v1"]
            | {"outcome": "unresolved", "resolved_scope": None},
            "versions": {
                "prompt_version": "attacker",
                "tool_contract_version": "attacker",
                "metric_definition_version": "attacker",
                "query_version": "attacker",
            },
            "model": {
                "provider_source": "byo",
                "provider_family": "attacker",
                "model_fingerprint": "attacker",
            },
            "claims": [],
            "metrics": [],
            "evidence": [],
            "conflicts": [],
            "coverage": {
                "required_source_count": 0,
                "available_source_count": 0,
                "unavailable_required_sources": [],
                "stale_required_sources": [],
                "as_of": candidate["as_of"],
            },
            # Not "complete"/"partial": this test makes zero tool calls and
            # is about server-owned metadata overwriting, not about whether
            # an empty answer is trustworthy -- the CHAOS-3290 grounding
            # floor (which gates complete/substantive-partial answers with
            # nothing else in the payload) has its own dedicated tests.
            "status": "degraded",
        }
    )

    result = await _run(
        _orchestrator(
            [ScriptedStep(decision=AgentFinalAnswer(candidate))],
            script_id=script_id,
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.answer_id == "answer_01"
    assert result.answer.conversation_id == "conversation_01"
    assert result.answer.resolved_scope == _resolution()
    assert result.answer.versions == _versions()
    assert result.answer.model.provider_source == "platform"
    assert result.answer.model.provider_family == "scripted"
    assert result.answer.model.model_fingerprint == _fingerprint(script_id)


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
    assert result.usage.input_tokens > 0
    assert result.usage.estimated_cost_microusd == 2_000_000


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
async def test_unknown_provider_cost_retains_the_pre_call_reservation() -> None:
    script_id = "unknown-cost-reservation"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentRefusal(code="unsupported", message="no"),
                    usage=AgentUsage(input_tokens=10, output_tokens=5),
                )
            ],
            script_id=script_id,
            limits=DevRunLimits(
                max_estimated_cost_microusd=1_500_000,
                estimated_cost_per_call_microusd=1_000_000,
            ),
        )
    )

    assert result.state is RunState.REFUSED
    assert result.usage.estimated_cost_microusd == 1_000_000


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
async def test_provider_contract_violation_is_a_safe_error_not_internal_error() -> None:
    """CHAOS-3254: a defensive multiple-tool-call provider response must not
    surface as the opaque application ``internal_error`` bucket. It must be a
    stable, distinguishable safe error with useful operator remediation.
    """
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    error=AgentProviderError(
                        AgentProviderErrorCode.PROVIDER_CONTRACT_VIOLATION,
                        retryable=False,
                    )
                )
            ],
            script_id="provider-contract-violation",
        )
    )
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "provider_contract_violation"
    assert result.error.code != "internal_error"
    assert result.error.retryable is False
    assert result.error.remediation


@pytest.mark.asyncio
async def test_multi_metric_question_completes_through_sequential_tool_rounds() -> None:
    """CHAOS-3254 acceptance: a naturally multi-metric question completes
    through one tool request per round -- through a final answer -- and
    stays within the existing tool/round budgets. This is the scripted
    deterministic acceptance path for the literal reproduction question:
    "What is the current delivery metric and project timing across the org
    look like compared the previous period?"
    """
    script_id = "multi-metric-sequential"
    executed: list[DevToolRequest] = []
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={
                            "metric_id": "items_completed",
                            "include_comparison": True,
                            "limit": 12,
                        },
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={
                            "metric_id": "cycle_time_p50_hours",
                            "include_comparison": True,
                            "limit": 12,
                        },
                        call_id="tool_call_02",
                    )
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={
                            "metric_id": "avg_wip",
                            "include_comparison": True,
                            "limit": 12,
                        },
                        call_id="tool_call_03",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(_answer(script_id=script_id))),
            ],
            script_id=script_id,
            registry=_registry(calls=executed),
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.tool_call_count == 3
    assert [request.metric_id for request in executed] == [
        "items_completed",
        "cycle_time_p50_hours",
        "avg_wip",
    ]
    assert all(request.include_comparison is True for request in executed)
    # One model decision per round, through the final answer: no batching of
    # multiple tool requests into a single round.
    model_decision_rounds = [
        event for event in result.events if event.state is RunState.MODEL_DECISION
    ]
    assert len(model_decision_rounds) == 4
    # Well within the six-call/four-round budgets (TRD 12).
    assert result.tool_call_count <= DevRunLimits().tool_calls
    assert len(model_decision_rounds) <= DevRunLimits().model_rounds


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


@pytest.mark.asyncio
async def test_in_flight_scope_resolution_is_cancelled_by_the_caller() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_scope(**_values):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    cancellation = asyncio.Event()
    run_task = asyncio.create_task(
        _run(
            _orchestrator(
                [],
                script_id="scope-cancel",
                scope_resolver=slow_scope,
            ),
            cancellation,
        )
    )
    await entered.wait()
    cancellation.set()
    result = await asyncio.wait_for(run_task, timeout=1)
    assert result.state is RunState.CANCELLED
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_scope_resolution_is_bounded_by_the_total_wall_clock() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def stalled_scope(**_values):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    result = await asyncio.wait_for(
        _run(
            _orchestrator(
                [],
                script_id="scope-timeout",
                scope_resolver=stalled_scope,
                limits=DevRunLimits(wall_seconds=0.01),
            )
        ),
        timeout=1,
    )

    assert entered.is_set()
    assert cancelled.is_set()
    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "tool_limit_reached"


@pytest.mark.asyncio
async def test_noncooperative_provider_is_cancelled_by_the_orchestrator() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    class IgnoringProvider:
        async def decide(self, **_values):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async def aclose(self) -> None:
            return None

    cancellation = asyncio.Event()
    run_task = asyncio.create_task(
        _run(
            _orchestrator(
                [],
                script_id="provider-cancel",
                provider=IgnoringProvider(),
            ),
            cancellation,
        )
    )
    await entered.wait()
    cancellation.set()
    result = await asyncio.wait_for(run_task, timeout=1)
    assert result.state is RunState.CANCELLED
    assert cancelled.is_set()


# --- CHAOS-3256: resolve named entities before executing status tools ---


def _scope_dict(
    *,
    direct_scope: str,
    repositories: list[str] | None = None,
    entity_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    from dev_health_ops.api.dev.contract_fixtures import END, PREVIOUS_START, START

    return {
        "schema_version": "dev_scope.v1",
        "organization_id": "org_fullchaos",
        "direct_scope": direct_scope,
        "repositories": repositories or [],
        "entity_refs": entity_refs or [],
        "team_ids": [],
        "time_range": {"start": START, "end": END, "timezone": "America/Los_Angeles"},
        "comparison_range": {
            "start": PREVIOUS_START,
            "end": START,
            "timezone": "America/Los_Angeles",
        },
        "surface_context": None,
    }


def _organization_resolution() -> DevScopeResolution:
    from dev_health_ops.api.dev.contract_fixtures import NOW

    scope = _scope_dict(direct_scope="organization")
    return DevScopeResolution.model_validate(
        {
            "schema_version": "dev_scope_resolution.v1",
            "requested_scope": scope,
            "resolved_scope": scope,
            "outcome": "inherited",
            "authorized_repository_ids": [],
            "authorized_entity_ids": [],
            "candidates": [],
            "fallbacks": [],
            "warnings": [],
            "resolved_at": NOW,
        }
    )


def _project_resolution() -> DevScopeResolution:
    from dev_health_ops.api.dev.contract_fixtures import NOW

    scope = _scope_dict(
        direct_scope="project",
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": "project-ask-dev",
                "display_label": "Ask Dev",
                "repository_id": None,
            }
        ],
    )
    return DevScopeResolution.model_validate(
        {
            "schema_version": "dev_scope_resolution.v1",
            "requested_scope": scope,
            "resolved_scope": scope,
            "outcome": "exact",
            "authorized_repository_ids": [],
            "authorized_entity_ids": ["project-ask-dev"],
            "candidates": [],
            "fallbacks": [],
            "warnings": [],
            "resolved_at": NOW,
        }
    )


def _not_found_resolution() -> DevScopeResolution:
    from dev_health_ops.api.dev.contract_fixtures import NOW

    scope = _scope_dict(direct_scope="organization")
    return DevScopeResolution.model_validate(
        {
            "schema_version": "dev_scope_resolution.v1",
            "requested_scope": scope,
            "resolved_scope": None,
            "outcome": "forbidden_or_not_found",
            "authorized_repository_ids": [],
            "authorized_entity_ids": [],
            "candidates": [],
            "fallbacks": [],
            "warnings": ["No authorized entity matched the requested query."],
            "resolved_at": NOW,
        }
    )


def _answer_with_no_claims(*, script_id: str) -> dict:
    """The stock answer fixture's claim hardcodes a repository validity_scope;
    these tests commit a different (project/organization) scope mid-run, so
    drop claims to avoid an unrelated validity_scope mismatch and keep the
    assertions focused on which scope the status tool received."""
    payload = _answer(script_id=script_id)
    payload["claims"] = []
    return payload


def _resolve_scope_registry(
    *, calls: list[DevToolRequest], resolve_scope_result: DevScopeResolution
) -> AskDevToolRegistry:
    async def execute(_context, request: DevToolRequest) -> DevToolResult:
        calls.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        if request.tool_id is ToolID.RESOLVE_SCOPE:
            payload["scope_resolution"] = resolve_scope_result.model_dump(mode="json")
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


@pytest.mark.asyncio
async def test_resolved_project_scope_is_committed_for_the_status_tool() -> None:
    """The literal question "What's the status of the Ask Dev project" asked
    from inherited organization scope must have status_snapshot.v1 receive
    project scope, not the stale organization scope the run started with."""

    script_id = "resolve-then-status"
    calls: list[DevToolRequest] = []

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "Ask Dev project", "limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_02",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_no_claims(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            registry=_resolve_scope_registry(
                calls=calls, resolve_scope_result=_project_resolution()
            ),
            scope_resolver=resolve,
        )
    )

    assert result.state is RunState.COMPLETED
    assert [request.tool_id.value for request in calls] == [
        "resolve_scope.v1",
        "status_snapshot.v1",
    ]
    assert calls[0].scope.direct_scope.value == "organization"
    # The status tool must receive the newly-committed project scope, not
    # the organization scope the run inherited.
    assert calls[1].scope.direct_scope.value == "project"
    assert calls[1].scope.entity_refs[0].entity_id == "project-ask-dev"
    assert result.answer is not None
    committed_scope = result.answer.resolved_scope.resolved_scope
    assert committed_scope is not None
    assert committed_scope.direct_scope.value == "project"


@pytest.mark.asyncio
async def test_unresolved_named_entity_never_falls_back_to_organization_scope() -> None:
    """An explicit reference that resolves to nothing must never leave the
    subsequent tool calls silently widened back to organization scope."""

    script_id = "resolve-not-found"
    calls: list[DevToolRequest] = []

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "some-other-tenant-project", "limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_02",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_no_claims(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            registry=_resolve_scope_registry(
                calls=calls, resolve_scope_result=_not_found_resolution()
            ),
            scope_resolver=resolve,
        )
    )

    assert result.state is RunState.COMPLETED
    # The not-found result must not clobber the previously authorized scope;
    # the status tool keeps executing against the last committed scope
    # rather than silently regressing to an unauthorized/ambiguous one.
    assert calls[1].scope.direct_scope.value == "organization"


# --- CHAOS-3289: questions about nonexistent entities must not be answered
# --- as if the named entity exists.


def _status_request() -> DevMessageRequest:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    payload["question_class"] = "status"
    return DevMessageRequest.model_validate(payload)


def _ambiguous_resolution() -> DevScopeResolution:
    from dev_health_ops.api.dev.contract_fixtures import NOW

    scope = _scope_dict(direct_scope="organization")
    candidate_scope = _scope_dict(
        direct_scope="project",
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": "project-a",
                "display_label": "Ask Dev A",
                "repository_id": None,
            }
        ],
    )
    return DevScopeResolution.model_validate(
        {
            "schema_version": "dev_scope_resolution.v1",
            "requested_scope": scope,
            "resolved_scope": None,
            "outcome": "ambiguous",
            "authorized_repository_ids": [],
            "authorized_entity_ids": [],
            "candidates": [
                {
                    "entity_ref": candidate_scope["entity_refs"][0],
                    "repository_id": None,
                    "reason": "Multiple authorized entities match the requested query.",
                }
            ],
            "fallbacks": [],
            "warnings": [],
            "resolved_at": NOW,
        }
    )


@pytest.mark.asyncio
async def test_status_answer_after_not_found_resolution_is_insufficient_evidence() -> (
    None
):
    """CHAOS-3289: resolve_scope.v1 correctly reports not-found for a named
    entity that does not exist, but nothing previously stopped the model
    from still narrating an organization-wide answer under that entity's
    name. The run must terminate insufficient_evidence instead."""

    script_id = "status-not-found"
    calls: list[DevToolRequest] = []

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "Ask Dev project", "limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_02",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_no_claims(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            registry=_resolve_scope_registry(
                calls=calls, resolve_scope_result=_not_found_resolution()
            ),
            scope_resolver=resolve,
        ),
        request=_status_request(),
    )

    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None
    assert result.error.code == "scope_not_found"
    assert result.answer is None


@pytest.mark.asyncio
async def test_status_answer_after_ambiguous_resolution_is_insufficient_evidence() -> (
    None
):
    """Same shape as the not-found case, but the model's last resolve_scope.v1
    attempt came back ambiguous. Presenting an organization-wide narrative
    under the (unpicked) entity's name is exactly as unsafe as not-found."""

    script_id = "status-ambiguous"
    calls: list[DevToolRequest] = []

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "Ask Dev", "limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_02",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_no_claims(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            registry=_resolve_scope_registry(
                calls=calls, resolve_scope_result=_ambiguous_resolution()
            ),
            scope_resolver=resolve,
        ),
        request=_status_request(),
    )

    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None
    assert result.error.code == "scope_ambiguous"
    assert result.answer is None


@pytest.mark.asyncio
async def test_status_answer_skipping_resolve_scope_with_no_claims_is_insufficient_evidence() -> (
    None
):
    """The literal reported defect: from inherited organization scope, the
    model skips resolve_scope.v1 entirely, calls status_snapshot.v1 directly,
    and would otherwise narrate a nonexistent named entity's status under an
    ungrounded (zero-claim) summary. The run must not complete/partial that."""

    script_id = "status-skip-resolve"
    calls: list[DevToolRequest] = []

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_no_claims(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            registry=_resolve_scope_registry(
                calls=calls, resolve_scope_result=_not_found_resolution()
            ),
            scope_resolver=resolve,
        ),
        request=_status_request(),
    )

    assert [request.tool_id.value for request in calls] == ["status_snapshot.v1"]
    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None
    assert result.error.code == "insufficient_evidence"
    assert result.answer is None


@pytest.mark.asyncio
async def test_status_answer_after_errored_resolve_scope_attempt_is_insufficient_evidence() -> (
    None
):
    """CHAOS-3289 regression: a resolve_scope.v1 call that is rejected as
    malformed (or times out) never produces a scope_resolution, so
    execution.result.scope_resolution is None -- distinct from a resolution
    that came back and definitively said ambiguous/not-found. Naively
    flipping resolve_scope_attempted=True for this call (without a real
    outcome to judge) would leave last_resolve_scope_outcome=None, which
    matches NEITHER the ambiguous/not-found branch NOR the
    not-resolve_scope_attempted branch -- silently disarming every guard
    check and letting a fabricated-name narrative straight through. An
    errored resolve_scope.v1 attempt must be treated the same as never
    calling it at all."""

    script_id = "status-resolve-errored"
    calls: list[DevToolRequest] = []

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    async def execute(_context, request: DevToolRequest) -> DevToolResult:
        calls.append(request)
        if request.tool_id is ToolID.RESOLVE_SCOPE:
            raise ToolRequestRejected("scope resolution fields are invalid")
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry({tool_id: execute for tool_id in ToolID})

    # The rejected resolve_scope.v1 call leaves one required source
    # unavailable, so dev_answer.v1's own coverage invariant requires a
    # degraded (not complete) status here -- orthogonal to the CHAOS-3289
    # guard under test, but required for the candidate to pass schema
    # validation and actually reach that guard.
    degraded_answer = _answer_with_no_claims(script_id=script_id)
    degraded_answer.update({"status": "degraded", "metrics": [], "evidence": []})

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "Ask Dev project", "limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_02",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(decision=AgentFinalAnswer(degraded_answer)),
            ],
            script_id=script_id,
            registry=registry,
            scope_resolver=resolve,
        ),
        request=_status_request(),
    )

    assert [request.tool_id.value for request in calls] == [
        "resolve_scope.v1",
        "status_snapshot.v1",
    ]
    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None
    assert result.error.code == "insufficient_evidence"
    assert result.answer is None


def _answer_with_organization_scoped_claim(*, script_id: str) -> dict:
    """The stock claim fixture hardcodes a repository validity_scope; this
    test commits organization scope instead, so re-point the claim's
    validity_scope at organization scope while keeping its evidence/metric
    references, which the default registry's canonical tool result already
    provides (dev_tool_result.v1 fixture: ev_01 / metric_01)."""
    payload = _answer(script_id=script_id)
    payload["claims"][0]["validity_scope"] = _scope_dict(direct_scope="organization")
    return payload


@pytest.mark.asyncio
async def test_grounded_organization_wide_status_answer_still_completes() -> None:
    """A genuinely organization-wide status answer with real evidence-backed
    claims must not be blocked just because resolve_scope.v1 was never
    called -- CHAOS-3255 made organization scope a fully valid, executable
    answer target on its own."""

    script_id = "status-grounded-org-wide"

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_organization_scoped_claim(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            scope_resolver=resolve,
        ),
        request=_status_request(),
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.claims


def _status_request_naming_entity(*, question: str) -> DevMessageRequest:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    payload["question_class"] = "status"
    payload["question"] = question
    return DevMessageRequest.model_validate(payload)


def _answer_narrating_named_entity(*, script_id: str, direct_summary: str) -> dict:
    """A genuinely grounded (evidence-backed, non-empty claims) answer whose
    own text still attributes the finding to a named entity -- the mixed
    variant of CHAOS-3289: the empty-claims backstop alone cannot catch this,
    since claims are real; only the narration itself gives it away."""
    payload = _answer_with_organization_scoped_claim(script_id=script_id)
    payload["direct_summary"] = direct_summary
    return payload


@pytest.mark.asyncio
async def test_status_answer_narrating_unresolved_named_entity_with_real_claims_is_blocked() -> (
    None
):
    """CHAOS-3289 mixed variant: the model never calls resolve_scope.v1,
    produces genuine organization-wide evidence-backed claims (so the
    empty-claims check does not fire), but still narrates those claims as
    if they were about the named entity the question asked about. This must
    be caught by the question-text/answer-text cross-check, not just the
    claims-emptiness heuristic."""

    script_id = "status-mixed-fabrication"
    question = "What's the status of the Ask Dev project"

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_narrating_named_entity(
                            script_id=script_id,
                            direct_summary=(
                                "The status of the Ask Dev project is currently "
                                "partial based on recent deployment activity."
                            ),
                        )
                    )
                ),
            ],
            script_id=script_id,
            scope_resolver=resolve,
        ),
        request=_status_request_naming_entity(question=question),
    )

    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None
    assert result.error.code == "insufficient_evidence"
    assert result.answer is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "question",
    [
        # noun+status trails the name instead of a leading preposition.
        "What's the Ask Dev project status?",
        # the noun leads the name instead of trailing a preposition.
        "Can you check project Ask Dev status?",
        # a connector phrase the original preposition list did not cover.
        "What's going on with the Ask Dev project?",
    ],
)
async def test_status_answer_narrating_unresolved_named_entity_other_phrasings_is_blocked(
    question: str,
) -> None:
    """CHAOS-3289 hardening: a Codex adversarial review of this same guard
    found that the original preposition-led regex (``status of/about/
    regarding/for the <Name> <noun>``) missed common real-world phrasings
    where the noun trails the name+status, or leads the name, or uses a
    connector outside the original preposition list -- letting the mixed
    fabrication variant through under ordinary rewording. Each of these
    must still be blocked."""

    script_id = "status-mixed-fabrication-phrasing"

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_narrating_named_entity(
                            script_id=script_id,
                            direct_summary=(
                                "The status of the Ask Dev project is currently "
                                "partial based on recent deployment activity."
                            ),
                        )
                    )
                ),
            ],
            script_id=script_id,
            scope_resolver=resolve,
        ),
        request=_status_request_naming_entity(question=question),
    )

    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.error is not None
    assert result.error.code == "insufficient_evidence"
    assert result.answer is None


@pytest.mark.asyncio
async def test_status_answer_about_a_different_topic_with_real_claims_still_completes() -> (
    None
):
    """The narrow name-shape backstop must not fire on a genuinely
    organization-wide answer just because the question happens to name an
    entity elsewhere in its phrasing -- only when the answer's own text
    echoes that name."""

    script_id = "status-mixed-unrelated"
    question = "What's the status of the Ask Dev project"

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    ),
                    usage=AgentUsage(input_tokens=100, output_tokens=10),
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_narrating_named_entity(
                            script_id=script_id,
                            direct_summary=(
                                "Twelve work items completed across the organization "
                                "in the selected period."
                            ),
                        )
                    )
                ),
            ],
            script_id=script_id,
            scope_resolver=resolve,
        ),
        request=_status_request_naming_entity(question=question),
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None


def test_hard_defaults_can_only_be_configured_downward() -> None:
    assert DevRunLimits().model_rounds == 4
    assert DevRunLimits(model_rounds=2).model_rounds == 2
    with pytest.raises(ValueError, match="configured downward"):
        DevRunLimits(model_rounds=5)


# --- CHAOS-3257: coverage must reflect usable evidence, not tool completion ---


def test_coverage_excludes_empty_partial_results_from_availability() -> None:
    """Literal CHAOS-3257 repro: a partial status snapshot with zero facts and
    a missing-repository-set warning must read as unavailable, not covered.
    """
    request = _tool_request(tool_id="status_snapshot.v1", metric_id=None)
    result = _tool_result(
        tool_id="status_snapshot.v1",
        status="partial",
        metric_definitions=[],
        metrics=[],
        evidence=[],
        status_facts=[],
        graph_edges=[],
        data_health=[],
        warnings=["authorized_repository_set_unavailable"],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (request,), (result,), datetime.now(UTC)
    )
    assert coverage.required_source_count == 1
    assert coverage.available_source_count == 0
    assert coverage.unavailable_required_sources == ["status_snapshot.v1"]


def test_coverage_counts_partial_with_usable_evidence_as_available() -> None:
    """A genuinely partial result that still carries usable facts is coverage,
    unlike an empty partial (contrast with the test above).
    """
    request = _tool_request(tool_id="status_snapshot.v1", metric_id=None)
    result = _tool_result(
        tool_id="status_snapshot.v1",
        status="partial",
        metric_definitions=[],
        metrics=[],
        # Every evidence ID a status fact cites must exist in this same
        # result's evidence array (DevToolResult.validate_evidence_closure,
        # CHAOS-3259) -- the fact below cites "evidence_01".
        evidence=[
            {
                **positive_fixtures()["dev_evidence_ref.v1"],
                "evidence_ref_id": "evidence_01",
            }
        ],
        status_facts=[
            {
                "fact_id": "fact_01",
                "text": "1 blocker open",
                "evidence_ref_ids": ["evidence_01"],
            }
        ],
        graph_edges=[],
        data_health=[],
        warnings=["some_children_unavailable"],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (request,), (result,), datetime.now(UTC)
    )
    assert coverage.available_source_count == 1
    assert coverage.unavailable_required_sources == []


def test_coverage_marks_error_and_unavailable_status_as_unavailable() -> None:
    error_request = _tool_request(
        tool_id="search_evidence.v1", metric_id=None, query="meridian/web-app"
    )
    error_result = _tool_result(
        tool_id="search_evidence.v1",
        status="error",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        error={
            "schema_version": "dev_error.v1",
            "request_id": "run_01",
            "code": "source_unavailable",
            "safe_message": "The evidence source could not be reached.",
            "retryable": True,
        },
    )
    unavailable_request = _tool_request(tool_id="data_health.v1", metric_id=None)
    unavailable_result = _tool_result(
        tool_id="data_health.v1",
        status="unavailable",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        warnings=["provider_unreachable"],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (error_request, unavailable_request),
        (error_result, unavailable_result),
        datetime.now(UTC),
    )
    assert coverage.required_source_count == 2
    assert coverage.available_source_count == 0
    assert coverage.unavailable_required_sources == [
        "data_health.v1",
        "search_evidence.v1",
    ]


def test_coverage_flags_stale_data_health_as_stale() -> None:
    request = _tool_request(tool_id="data_health.v1", metric_id=None)
    result = _tool_result(
        tool_id="data_health.v1",
        status="success",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        data_health=[
            {
                "source_system": "work_items",
                "freshness": "stale",
                "last_successful_at": positive_fixtures()["dev_evidence_ref.v1"][
                    "observed_at"
                ],
                "coverage": 0.5,
                "warning": "watermark behind SLA",
            }
        ],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (request,), (result,), datetime.now(UTC)
    )
    assert coverage.available_source_count == 1
    assert coverage.stale_required_sources == ["data_health.v1"]


def test_coverage_flags_stale_metric_payload_as_stale() -> None:
    """Staleness embedded in a returned metric (not just data_health.v1) must
    also be surfaced -- a stale metric answering a query is not fresh
    coverage even though the tool call itself succeeded.
    """
    request = _tool_request(tool_id="query_metric.v1")
    stale_metric = deepcopy(positive_fixtures()["dev_metric_ref.v1"])
    stale_metric["freshness"] = "stale"
    result = _tool_result(
        tool_id="query_metric.v1",
        status="success",
        evidence=[],
        metric_definitions=[],
        metrics=[stale_metric],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (request,), (result,), datetime.now(UTC)
    )
    assert coverage.available_source_count == 1
    assert coverage.stale_required_sources == ["query_metric.v1"]


def test_coverage_data_health_with_only_unavailable_entries_is_not_usable() -> None:
    """A partial data_health.v1 result whose every entry reports
    unavailable is the "nothing is available" case, not evidence that the
    source responded -- a non-empty list alone must not count as coverage.
    """
    request = _tool_request(tool_id="data_health.v1", metric_id=None)
    result = _tool_result(
        tool_id="data_health.v1",
        status="partial",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        data_health=[
            {
                "source_system": "work_items",
                "freshness": "unavailable",
                "last_successful_at": None,
                "coverage": 0.0,
                "warning": "provider not configured",
            },
            {
                "source_system": "repositories",
                "freshness": "unavailable",
                "last_successful_at": None,
                "coverage": 0.0,
                "warning": "provider not configured",
            },
        ],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (request,), (result,), datetime.now(UTC)
    )
    assert coverage.available_source_count == 0
    assert coverage.unavailable_required_sources == ["data_health.v1"]


def test_coverage_data_health_with_one_available_entry_is_usable() -> None:
    """The mirror of the previous test: if at least one data_health entry is
    not unavailable, the source did answer with something real.
    """
    request = _tool_request(tool_id="data_health.v1", metric_id=None)
    result = _tool_result(
        tool_id="data_health.v1",
        status="partial",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        data_health=[
            {
                "source_system": "work_items",
                "freshness": "fresh",
                "last_successful_at": positive_fixtures()["dev_evidence_ref.v1"][
                    "observed_at"
                ],
                "coverage": 1.0,
                "warning": None,
            },
            {
                "source_system": "repositories",
                "freshness": "unavailable",
                "last_successful_at": None,
                "coverage": 0.0,
                "warning": "provider not configured",
            },
        ],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (request,), (result,), datetime.now(UTC)
    )
    assert coverage.available_source_count == 1
    assert coverage.unavailable_required_sources == []


def test_coverage_dedupes_identical_retried_requests() -> None:
    """required_source_count counts required source *classes*: a retry of
    the identical request (same tool, same discriminating arguments) is one
    required source, not two.
    """
    first = _tool_request(
        tool_id="query_metric.v1",
        metric_id="items_completed",
        tool_call_id="tool_call_01",
    )
    second = _tool_request(
        tool_id="query_metric.v1",
        metric_id="items_completed",
        tool_call_id="tool_call_02",
    )
    first_result = _tool_result(tool_id="query_metric.v1", tool_call_id="tool_call_01")
    second_result = _tool_result(tool_id="query_metric.v1", tool_call_id="tool_call_02")
    coverage = DevOrchestrator._coverage_from_tool_results(
        (first, second), (first_result, second_result), datetime.now(UTC)
    )
    assert coverage.required_source_count == 1
    assert coverage.available_source_count == 1


def test_coverage_treats_different_arguments_to_the_same_tool_as_distinct_sources() -> (
    None
):
    """A model calling the same tool for two genuinely different asks (e.g.
    two different metric_ids) must not collapse into one required source:
    if one succeeds and the other is unavailable, coverage must show a
    real gap, not a false "1 of 1".
    """
    completed = _tool_request(
        tool_id="query_metric.v1",
        metric_id="items_completed",
        tool_call_id="tool_call_01",
    )
    completed_result = _tool_result(
        tool_id="query_metric.v1", tool_call_id="tool_call_01"
    )
    cycle_time = _tool_request(
        tool_id="query_metric.v1",
        metric_id="cycle_time_p50_hours",
        tool_call_id="tool_call_02",
    )
    cycle_time_result = _tool_result(
        tool_id="query_metric.v1",
        tool_call_id="tool_call_02",
        status="unavailable",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        warnings=["metric_source_unavailable"],
    )
    coverage = DevOrchestrator._coverage_from_tool_results(
        (completed, cycle_time),
        (completed_result, cycle_time_result),
        datetime.now(UTC),
    )
    assert coverage.required_source_count == 2
    assert coverage.available_source_count == 1
    assert coverage.unavailable_required_sources == ["query_metric.v1"]


def test_coverage_sorts_sources_when_one_discriminator_is_unset_and_one_is_not() -> (
    None
):
    """Regression: sorting required-source keys directly raised
    ``TypeError: '<' not supported between instances of 'NoneType' and
    'str'`` as soon as two sources shared a tool_id label but differed in
    which discriminating field (query/metric_id) was set vs. None -- e.g.
    list_metrics.v1 called once with a (rejected) query and once bare. This
    crashed the whole run as an uncaught internal_error rather than
    producing any answer, discovered rebasing CHAOS-3262 against the
    CHAOS-3257 coverage fix.
    """
    with_query = _tool_request(
        tool_id="list_metrics.v1",
        metric_id=None,
        query="which metrics",
        tool_call_id="tool_call_01",
    )
    with_query_result = _tool_result(
        tool_id="list_metrics.v1",
        tool_call_id="tool_call_01",
        status="error",
        metrics=[],
        evidence=[],
        metric_definitions=[],
        error={
            "schema_version": "dev_error.v1",
            "request_id": "run_01",
            "code": "invalid_request",
            "safe_message": "The tool request did not match the tool's contract.",
            "retryable": True,
        },
    )
    bare = _tool_request(
        tool_id="list_metrics.v1", metric_id=None, tool_call_id="tool_call_02"
    )
    bare_result = _tool_result(tool_id="list_metrics.v1", tool_call_id="tool_call_02")
    coverage = DevOrchestrator._coverage_from_tool_results(
        (with_query, bare), (with_query_result, bare_result), datetime.now(UTC)
    )
    assert coverage.required_source_count == 2
    assert coverage.available_source_count == 1
    assert coverage.unavailable_required_sources == ["list_metrics.v1"]


@pytest.mark.asyncio
async def test_status_question_run_never_shows_full_coverage_with_zero_evidence() -> (
    None
):
    """Full-stack (backend-half) CHAOS-3257 regression: the observed status
    question, answered from a single empty-partial status snapshot, must
    complete with coverage that reflects zero usable evidence, never a full
    "1 of 1" count. (Web copy is out of this lane's scope; see PR notes.)
    """
    script_id = "empty-partial-status-run"

    async def empty_partial_status(
        _context: Any, request: DevToolRequest
    ) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "status": "partial",
                "scope_resolution": None,
                "metric_definitions": [],
                "metrics": [],
                "evidence": [],
                "status_facts": [],
                "graph_edges": [],
                "data_health": [],
                "warnings": ["authorized_repository_set_unavailable"],
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry({tool_id: empty_partial_status for tool_id in ToolID})
    degraded_answer = _answer(script_id=script_id)
    degraded_answer.update(
        {
            "status": "degraded",
            "direct_summary": (
                "Status could not be established because the authorized "
                "repository set was unavailable."
            ),
            "claims": [],
            "metrics": [],
            "evidence": [],
            "warnings": ["authorized_repository_set_unavailable"],
        }
    )

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(degraded_answer)),
            ],
            script_id=script_id,
            registry=registry,
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.status != "complete"
    assert result.answer.coverage.required_source_count == 1
    assert result.answer.coverage.available_source_count == 0
    assert result.answer.coverage.unavailable_required_sources == ["status_snapshot.v1"]


@pytest.mark.asyncio
async def test_model_claiming_complete_against_correct_coverage_gets_one_repair() -> (
    None
):
    """If the model claims `status="complete"` while the server-derived
    coverage (correctly, post-CHAOS-3257) shows a required source was
    unavailable, DevAnswer's own invariant rejects that combination. This
    must not hard-fail the whole run: the model gets one bounded repair
    attempt (same mechanism as a schema-only failure) to reissue an
    accurately-labeled answer from the same grounded data.
    """
    script_id = "repair-false-complete-claim"

    async def empty_partial_status(
        _context: Any, request: DevToolRequest
    ) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "status": "partial",
                "scope_resolution": None,
                "metric_definitions": [],
                "metrics": [],
                "evidence": [],
                "status_facts": [],
                "graph_edges": [],
                "data_health": [],
                "warnings": ["authorized_repository_set_unavailable"],
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry({tool_id: empty_partial_status for tool_id in ToolID})

    falsely_complete_answer = _answer(script_id=script_id)
    falsely_complete_answer.update(
        {"status": "complete", "claims": [], "metrics": [], "evidence": []}
    )
    corrected_answer = _answer(script_id=script_id)
    corrected_answer.update(
        {
            "status": "degraded",
            "direct_summary": "Status could not be established.",
            "claims": [],
            "metrics": [],
            "evidence": [],
        }
    )

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(falsely_complete_answer)),
                ScriptedStep(decision=AgentFinalAnswer(corrected_answer)),
            ],
            script_id=script_id,
            registry=registry,
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.status == "degraded"
    assert [event.state for event in result.events].count(
        RunState.ANSWER_VALIDATION
    ) == 2


@pytest.mark.asyncio
async def test_second_false_complete_claim_still_fails_closed() -> None:
    """The repair budget is bounded to one attempt: a model that repeats the
    same status/coverage contradiction still fails the run rather than
    looping or silently shipping an ungrounded "complete" answer.
    """
    script_id = "repair-exhausted-false-complete"

    async def empty_partial_status(
        _context: Any, request: DevToolRequest
    ) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "status": "partial",
                "scope_resolution": None,
                "metric_definitions": [],
                "metrics": [],
                "evidence": [],
                "status_facts": [],
                "graph_edges": [],
                "data_health": [],
                "warnings": ["authorized_repository_set_unavailable"],
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry({tool_id: empty_partial_status for tool_id in ToolID})
    falsely_complete_answer = _answer(script_id=script_id)
    falsely_complete_answer.update(
        {"status": "complete", "claims": [], "metrics": [], "evidence": []}
    )

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(falsely_complete_answer)),
                ScriptedStep(decision=AgentFinalAnswer(falsely_complete_answer)),
            ],
            script_id=script_id,
            registry=registry,
        )
    )

    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "answer_validation_failed"


# --- CHAOS-3262: an invalid model tool request degrades, it does not kill the run ---


@pytest.mark.asyncio
async def test_invalid_tool_request_degrades_instead_of_failing_the_run() -> None:
    """A tool call whose arguments violate the tool's own contract (here,
    list_metrics.v1 receiving a "query" it does not accept) must not fail the
    whole run as tool_unavailable. It becomes one failed tool result and the
    model recovers within the same run.
    """
    script_id = "degrade-invalid-list-metrics"
    calls: list[DevToolRequest] = []
    recorder = Recorder()
    # Two calls land against the same tool_id (one rejected, one corrected).
    # CHAOS-3257 (coverage-from-usable-evidence) is a separate, independently
    # shipped fix; this test only asserts CHAOS-3262's degrade behavior, so it
    # does not rely on that fix's required-source-class deduplication here —
    # it answers "degraded" rather than claiming full "complete" coverage.
    degraded_answer = _answer(script_id=script_id)
    degraded_answer["status"] = "degraded"

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="list_metrics.v1",
                        arguments={"query": "which metrics", "limit": 8},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="list_metrics.v1",
                        arguments={"limit": 8},
                        call_id="tool_call_02",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(degraded_answer)),
            ],
            script_id=script_id,
            registry=_registry(calls=calls),
            recorder=recorder,
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.tool_call_count == 2
    # The rejected first call never reached the tool executor.
    assert len(calls) == 1
    assert [execution.result.status for execution in recorder.executions] == [
        "error",
        "success",
    ]
    rejected = recorder.executions[0].result
    assert rejected.tool_id is ToolID.LIST_METRICS
    assert rejected.error is not None
    assert rejected.error.code == "invalid_request"


@pytest.mark.asyncio
async def test_repeated_invalid_tool_requests_still_hit_the_tool_call_limit() -> None:
    """Degrading rejected calls must not create an unbounded retry loop: the
    existing tool-call budget still terminates a model that never corrects.
    """
    script_id = "degrade-loop-guard"
    steps = [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id="list_metrics.v1",
                arguments={"query": f"attempt-{index}", "limit": 8},
                call_id=f"tool_call_{index:02d}",
            )
        )
        for index in range(8)
    ]

    result = await _run(_orchestrator(steps, script_id=script_id, registry=_registry()))

    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "tool_limit_reached"


@pytest.mark.asyncio
async def test_advertised_but_out_of_enum_metric_id_degrades_instead_of_failing() -> (
    None
):
    """The provider-facing schema advertises query_metric.v1's metric_id as
    an open string (src/dev_health_ops/api/dev/orchestrator.py's
    _provider_tool_input_schema), but dev_tool_request.v1 constrains it to
    the closed MetricID enum. A schema-compliant-looking model request for a
    metric that does not exist must degrade to a failed tool result, not
    kill the run as tool_unavailable with zero recorded tool calls -- this
    rejection happens during request construction, before
    AskDevToolRegistry.execute() is ever reached.
    """
    script_id = "degrade-unregistered-metric-id"
    recorder = Recorder()
    # Two calls land against the same tool_id (one rejected, one corrected).
    # CHAOS-3257 (coverage-from-usable-evidence, incl. required-source-class
    # dedup) is a separately shipped fix; this test only asserts CHAOS-3262's
    # degrade behavior, so it answers "degraded" rather than "complete".
    degraded_answer = _answer(script_id=script_id)
    degraded_answer["status"] = "degraded"

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={
                            "metric_id": "lead_time",  # not a registered MetricID
                            "include_comparison": False,
                            "limit": 12,
                        },
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={
                            "metric_id": "items_completed",
                            "include_comparison": False,
                            "limit": 12,
                        },
                        call_id="tool_call_02",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(degraded_answer)),
            ],
            script_id=script_id,
            registry=_registry(),
            recorder=recorder,
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.tool_call_count == 2
    assert [execution.result.status for execution in recorder.executions] == [
        "error",
        "success",
    ]
    rejected = recorder.executions[0].result
    assert rejected.tool_id is ToolID.QUERY_METRIC
    assert rejected.error is not None
    assert rejected.error.code == "invalid_request"


@pytest.mark.asyncio
async def test_tool_execution_timeout_degrades_instead_of_failing_the_run() -> None:
    """A registered tool with a valid request that simply does not answer in
    time is a per-call source-availability failure, not a registry defect:
    it must degrade to a failed tool result too, preserving any results
    already recorded, instead of failing the whole run.
    """
    script_id = "degrade-tool-timeout"

    async def stalls_forever(_context, _request):
        await asyncio.Event().wait()

    async def succeeds(_context, request: DevToolRequest) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry(
        {
            tool_id: (stalls_forever if tool_id is ToolID.STATUS_SNAPSHOT else succeeds)
            for tool_id in ToolID
        }
    )
    recorder = Recorder()
    # The only tool result is the degraded timeout; a real model would
    # ground its answer in that absence rather than claim completeness.
    degraded_answer = _answer(script_id=script_id)
    degraded_answer.update(
        {"status": "degraded", "claims": [], "metrics": [], "evidence": []}
    )

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(degraded_answer)),
            ],
            script_id=script_id,
            registry=registry,
            recorder=recorder,
            limits=DevRunLimits(tool_seconds=0.05, wall_seconds=5),
        )
    )

    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.tool_call_count == 1
    rejected = recorder.executions[0].result
    assert rejected.tool_id is ToolID.STATUS_SNAPSHOT
    assert rejected.error is not None
    assert rejected.error.code == "source_unavailable"


# --- CHAOS-3288: an informative repair prompt for a stale-coverage rejection ---


@pytest.mark.asyncio
async def test_stale_complete_repair_prompt_includes_the_actual_validation_reason() -> (
    None
):
    """Literal CHAOS-3288 repro shape: two query_metric.v1 rounds return
    real but stale data, the model answers status=complete, and the
    (correct, CHAOS-3257) coverage invariant rejects it. The old generic
    "fix your JSON" repair prompt gave the model nothing to act on; the
    repair turn must now name the actual reason so a real model has a
    chance to self-correct instead of repeating the same mistake and
    exhausting the one repair attempt.
    """
    script_id = "stale-complete-repair-detail"

    async def query_metric_executor(
        _context: Any, request: DevToolRequest
    ) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        metric = deepcopy(payload["metrics"][0])
        metric["freshness"] = "stale"
        metric["metric_ref_id"] = f"metric:{request.metric_id}"
        metric["metric_id"] = request.metric_id
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "status": "partial",
                "metrics": [metric],
                "evidence": [],
                "metric_definitions": [],
                "warnings": ["source_stale"],
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry(
        {tool_id: query_metric_executor for tool_id in ToolID}
    )
    falsely_complete = _answer(script_id=script_id)
    falsely_complete.update(
        {"status": "complete", "claims": [], "metrics": [], "evidence": []}
    )
    corrected = _answer(script_id=script_id)
    corrected.update(
        {"status": "degraded", "claims": [], "metrics": [], "evidence": []}
    )

    provider = RecordingProvider(
        [
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id="query_metric.v1",
                    arguments={
                        "metric_id": "cycle_time_p50_hours",
                        "include_comparison": False,
                        "limit": 12,
                    },
                    call_id="tool_call_01",
                )
            ),
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id="query_metric.v1",
                    arguments={
                        "metric_id": "avg_wip",
                        "include_comparison": False,
                        "limit": 12,
                    },
                    call_id="tool_call_02",
                )
            ),
            ScriptedStep(decision=AgentFinalAnswer(falsely_complete)),
            ScriptedStep(decision=AgentFinalAnswer(corrected)),
        ],
        script_id=script_id,
    )

    result = await _run(
        _orchestrator([], script_id=script_id, registry=registry, provider=provider)
    )

    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.status == "degraded"
    # 2 tool rounds + 1 rejected final answer + 1 repaired final answer.
    assert len(provider.calls) == 4
    repair_messages = provider.calls[3]
    repair_user_message = next(
        message for message in repair_messages if message.role.value == "user"
    )
    assert (
        "complete answer requires all required sources fresh and available"
        in repair_user_message.content
    )
    assert "The previous response failed validation" in repair_user_message.content


@pytest.mark.asyncio
async def test_repair_turn_overflow_is_a_classified_budget_limit_not_internal_error() -> (
    None
):
    """CHAOS-3288 review: a synthetic repair turn appended on top of an
    already-near-budget caller-supplied conversation history can push
    PromptComposer over its byte cap on the retry round. That must surface
    as a classified tool_limit_reached (the same bounded-budget family as
    every other limit in this run loop), not fall through to a generic,
    uninformative internal_error that misrepresents a repairable rejection
    as an unexpected server failure.
    """
    script_id = "repair-turn-budget-overflow"

    async def query_metric_executor(
        _context: Any, request: DevToolRequest
    ) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        metric = deepcopy(payload["metrics"][0])
        metric["freshness"] = "stale"
        metric["metric_ref_id"] = f"metric:{request.metric_id}"
        metric["metric_id"] = request.metric_id
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "status": "partial",
                "metrics": [metric],
                "evidence": [],
                "metric_definitions": [],
                "warnings": ["source_stale"],
            }
        )
        return DevToolResult.model_validate(payload)

    registry = AskDevToolRegistry(
        {tool_id: query_metric_executor for tool_id in ToolID}
    )
    falsely_complete = _answer(script_id=script_id)
    falsely_complete.update(
        {"status": "complete", "claims": [], "metrics": [], "evidence": []}
    )

    # Three large prior turns close to PromptComposer's 32,768-byte budget,
    # matching the boundary reproduced in review: an assistant/user/
    # assistant sequence whose serialized history sits a few hundred bytes
    # under the cap on its own.
    huge_prior_turns = (
        PromptConversationTurn(role="assistant", content="a" * 12_500),
        PromptConversationTurn(role="user", content="b" * 8_000),
        PromptConversationTurn(role="assistant", content="c" * 12_000),
    )

    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="query_metric.v1",
                        arguments={
                            "metric_id": "cycle_time_p50_hours",
                            "include_comparison": False,
                            "limit": 12,
                        },
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(decision=AgentFinalAnswer(falsely_complete)),
            ],
            script_id=script_id,
            registry=registry,
        ),
        prior_turns=huge_prior_turns,
    )

    assert result.state is RunState.FAILED
    assert result.error is not None
    assert result.error.code == "tool_limit_reached"
