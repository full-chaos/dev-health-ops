"""Bounded, provider-neutral Ask Dev orchestration state machine.

The model may choose only one of the normalized decisions exposed by
``AgentLLMProvider``. Authorization, scope, limits, tool execution, grounding,
and terminal persistence remain server-owned.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, Protocol

from dev_health_ops.llm.agent.contracts import (
    AgentDisambiguation,
    AgentFinalAnswer,
    AgentLLMProvider,
    AgentMessage,
    AgentMessageRole,
    AgentRefusal,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)

from .answer_validator import (
    AnswerValidationContext,
    AnswerValidationError,
    validate_answer_candidate,
)
from .contracts import (
    DevAnswer,
    DevContractVersions,
    DevError,
    DevMessageRequest,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    ScopeResolutionOutcome,
    ToolID,
)
from .prompts import PromptComposer, PromptConversationTurn
from .tool_registry import (
    AskDevToolRegistry,
    ToolExecution,
    ToolExecutionCancelled,
    ToolExecutionContext,
    ToolRegistryError,
)

_HARD_LIMIT_MAXIMA: dict[str, int | float] = {
    "model_rounds": 4,
    "tool_calls": 6,
    "identical_tool_calls": 2,
    "wall_seconds": 45.0,
    "provider_seconds": 30.0,
    "provider_retries": 1,
    "schema_repairs": 1,
    "total_tool_bytes": 256 * 1024,
    "per_tool_bytes": 64 * 1024,
    "evidence_refs": 25,
    "metrics": 12,
}


class RunState(StrEnum):
    ACCEPTED = "accepted"
    RESOLVING_SCOPE = "resolving_scope"
    MODEL_DECISION = "model_decision"
    TOOL_VALIDATION = "tool_validation"
    TOOL_EXECUTION = "tool_execution"
    ANSWER_VALIDATION = "answer_validation"
    COMPLETED = "completed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    REFUSED = "refused"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset(
    {
        RunState.COMPLETED,
        RunState.INSUFFICIENT_EVIDENCE,
        RunState.REFUSED,
        RunState.FAILED,
        RunState.CANCELLED,
    }
)


@dataclass(frozen=True, slots=True)
class DevRunLimits:
    """TRD defaults; operators may configure only stricter values."""

    model_rounds: int = 4
    tool_calls: int = 6
    identical_tool_calls: int = 2
    wall_seconds: float = 45.0
    provider_seconds: float = 30.0
    provider_retries: int = 1
    schema_repairs: int = 1
    total_tool_bytes: int = 256 * 1024
    per_tool_bytes: int = 64 * 1024
    evidence_refs: int = 25
    metrics: int = 12
    max_output_tokens_per_call: int = 4_096
    max_total_input_tokens: int = 100_000
    max_total_output_tokens: int = 16_384
    estimated_cost_per_call_microusd: int = 1_000_000
    max_estimated_cost_microusd: int = 5_000_000

    def __post_init__(self) -> None:
        for name, maximum in _HARD_LIMIT_MAXIMA.items():
            value = getattr(self, name)
            if value < 0 or value > maximum:
                raise ValueError(f"{name} may only be configured downward")
        for name in (
            "max_output_tokens_per_call",
            "max_total_input_tokens",
            "max_total_output_tokens",
            "estimated_cost_per_call_microusd",
            "max_estimated_cost_microusd",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class OrchestratorEvent:
    state: RunState
    safe_code: str | None = None


@dataclass(frozen=True, slots=True)
class OrchestratorResult:
    run_id: str
    state: RunState
    answer: DevAnswer | None
    error: DevError | None
    events: tuple[OrchestratorEvent, ...]
    usage: AgentUsage
    tool_call_count: int
    provider_fingerprint: str | None
    model_fingerprint: str | None

    def __post_init__(self) -> None:
        if self.state not in TERMINAL_STATES:
            raise ValueError("orchestrator result must be terminal")
        if (self.answer is None) == (self.error is None):
            raise ValueError(
                "orchestrator result requires exactly one terminal payload"
            )


class ScopeResolver(Protocol):
    async def __call__(
        self, *, org_id: str, user_id: str, requested_scope: DevScope
    ) -> DevScopeResolution: ...


class RunRecorder(Protocol):
    async def transition(self, state: RunState) -> None: ...

    async def record_tool(
        self,
        *,
        ordinal: int,
        request: DevToolRequest,
        canonical_input_hash: str,
        execution: ToolExecution,
    ) -> None: ...

    async def record_answer(self, answer: DevAnswer) -> None: ...

    async def terminal(
        self,
        *,
        state: RunState,
        answer: DevAnswer | None,
        error: DevError | None,
        usage: AgentUsage,
        tool_call_count: int,
        provider_fingerprint: str | None,
        model_fingerprint: str | None,
        prompt_checksum: str | None,
    ) -> None: ...


class NullRunRecorder:
    async def transition(self, state: RunState) -> None:
        del state

    async def record_tool(
        self,
        *,
        ordinal: int,
        request: DevToolRequest,
        canonical_input_hash: str,
        execution: ToolExecution,
    ) -> None:
        del ordinal, request, canonical_input_hash, execution

    async def record_answer(self, answer: DevAnswer) -> None:
        del answer

    async def terminal(
        self,
        *,
        state: RunState,
        answer: DevAnswer | None,
        error: DevError | None,
        usage: AgentUsage,
        tool_call_count: int,
        provider_fingerprint: str | None,
        model_fingerprint: str | None,
        prompt_checksum: str | None,
    ) -> None:
        del (
            state,
            answer,
            error,
            usage,
            tool_call_count,
            provider_fingerprint,
            model_fingerprint,
            prompt_checksum,
        )


class BudgetExceeded(RuntimeError):
    pass


@dataclass(slots=True)
class ProviderBudget:
    limits: DevRunLimits
    usage: AgentUsage = field(default_factory=AgentUsage)

    def require(self, *, prompt_bytes: int) -> None:
        estimated_input_tokens = max(1, (prompt_bytes + 3) // 4)
        if (
            self.usage.input_tokens + estimated_input_tokens
            > self.limits.max_total_input_tokens
        ):
            raise BudgetExceeded("input token budget exhausted")
        if (
            self.usage.output_tokens + self.limits.max_output_tokens_per_call
            > self.limits.max_total_output_tokens
        ):
            raise BudgetExceeded("output token budget exhausted")
        if (
            (self.usage.estimated_cost_microusd or 0)
            + self.limits.estimated_cost_per_call_microusd
            > self.limits.max_estimated_cost_microusd
        ):
            raise BudgetExceeded("provider cost budget exhausted")

    def add(self, usage: AgentUsage) -> None:
        if usage.input_tokens < 0 or usage.output_tokens < 0:
            raise BudgetExceeded("provider returned invalid token usage")
        if (
            usage.estimated_cost_microusd is not None
            and usage.estimated_cost_microusd < 0
        ):
            raise BudgetExceeded("provider returned invalid cost usage")
        prior_cost = self.usage.estimated_cost_microusd or 0
        added_cost = usage.estimated_cost_microusd or 0
        self.usage = AgentUsage(
            input_tokens=self.usage.input_tokens + usage.input_tokens,
            output_tokens=self.usage.output_tokens + usage.output_tokens,
            estimated_cost_microusd=prior_cost + added_cost,
        )
        if self.usage.input_tokens > self.limits.max_total_input_tokens:
            raise BudgetExceeded("input token budget exhausted")
        if self.usage.output_tokens > self.limits.max_total_output_tokens:
            raise BudgetExceeded("output token budget exhausted")
        if (
            self.usage.estimated_cost_microusd or 0
        ) > self.limits.max_estimated_cost_microusd:
            raise BudgetExceeded("provider cost budget exhausted")


class EventCancellationSignal:
    def __init__(self, event: asyncio.Event) -> None:
        self._event = event

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()


EventSink = Callable[[OrchestratorEvent], Awaitable[None]]


class DevOrchestrator:
    """Execute one Ask Dev message as a bounded state machine."""

    def __init__(
        self,
        *,
        provider: AgentLLMProvider,
        provider_source: Literal["platform", "byo"],
        provider_family: str,
        registry: AskDevToolRegistry,
        scope_resolver: ScopeResolver,
        versions: DevContractVersions,
        limits: DevRunLimits | None = None,
        recorder: RunRecorder | None = None,
        event_sink: EventSink | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._provider = provider
        self._provider_source = provider_source
        self._provider_family = provider_family
        self._registry = registry
        self._scope_resolver = scope_resolver
        self._versions = versions
        self._limits = limits or DevRunLimits()
        self._recorder = recorder or NullRunRecorder()
        self._event_sink = event_sink
        self._monotonic = monotonic
        self._composer = PromptComposer(registry)

    async def run(
        self,
        *,
        request: DevMessageRequest,
        org_id: str,
        user_id: str,
        permission_fingerprint: str,
        run_id: str,
        conversation_id: str,
        answer_id: str,
        cancellation: asyncio.Event,
        prior_turns: tuple[PromptConversationTurn, ...] = (),
    ) -> OrchestratorResult:
        started = self._monotonic()
        events: list[OrchestratorEvent] = []
        tool_results: list[DevToolResult] = []
        tool_bytes_total = 0
        duplicate_counts: Counter[str] = Counter()
        budget = ProviderBudget(self._limits)
        provider_fingerprint: str | None = None
        model_fingerprint: str | None = None
        prompt_checksum: str | None = None
        terminal_written = False
        repair_count = 0
        retry_count = 0

        async def transition(state: RunState, safe_code: str | None = None) -> None:
            event = OrchestratorEvent(state=state, safe_code=safe_code)
            events.append(event)
            await self._recorder.transition(state)
            if self._event_sink is not None:
                await self._event_sink(event)

        async def finish(
            state: RunState,
            *,
            answer: DevAnswer | None = None,
            error: DevError | None = None,
        ) -> OrchestratorResult:
            nonlocal terminal_written
            if terminal_written:
                raise RuntimeError("terminal state already written")
            if answer is not None:
                try:
                    await self._recorder.record_answer(answer)
                except Exception:
                    state = RunState.FAILED
                    answer = None
                    error = DevError(
                        schema_version="dev_error.v1",
                        request_id=request.request_id,
                        code="internal_error",
                        safe_message="The validated answer could not be stored.",
                        retryable=True,
                    )
            await self._recorder.terminal(
                state=state,
                answer=answer,
                error=error,
                usage=budget.usage,
                tool_call_count=len(tool_results),
                provider_fingerprint=provider_fingerprint,
                model_fingerprint=model_fingerprint,
                prompt_checksum=prompt_checksum,
            )
            terminal_written = True
            event = OrchestratorEvent(
                state=state, safe_code=error.code if error else None
            )
            events.append(event)
            if self._event_sink is not None:
                await self._event_sink(event)
            return OrchestratorResult(
                run_id=run_id,
                state=state,
                answer=answer,
                error=error,
                events=tuple(events),
                usage=budget.usage,
                tool_call_count=len(tool_results),
                provider_fingerprint=provider_fingerprint,
                model_fingerprint=model_fingerprint,
            )

        def error(code: str, message: str, *, retryable: bool = False) -> DevError:
            return DevError(
                schema_version="dev_error.v1",
                request_id=request.request_id,
                code=code,
                safe_message=message,
                retryable=retryable,
            )

        def remaining() -> float:
            return self._limits.wall_seconds - (self._monotonic() - started)

        await transition(RunState.ACCEPTED)
        try:
            if cancellation.is_set():
                return await finish(
                    RunState.CANCELLED,
                    error=error("cancelled", "The request was cancelled."),
                )
            await transition(RunState.RESOLVING_SCOPE)
            resolution = await self._scope_resolver(
                org_id=org_id,
                user_id=user_id,
                requested_scope=request.scope,
            )
            if resolution.outcome is ScopeResolutionOutcome.AMBIGUOUS:
                return await finish(
                    RunState.INSUFFICIENT_EVIDENCE,
                    error=error("scope_ambiguous", "The requested scope is ambiguous."),
                )
            if resolution.outcome in {
                ScopeResolutionOutcome.UNRESOLVED,
                ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
            }:
                return await finish(
                    RunState.INSUFFICIENT_EVIDENCE,
                    error=error(
                        "scope_not_found", "The requested scope was not found."
                    ),
                )
            authorized_scope = resolution.resolved_scope
            if authorized_scope is None or authorized_scope.organization_id != org_id:
                return await finish(
                    RunState.FAILED,
                    error=error(
                        "scope_forbidden", "The requested scope is not authorized."
                    ),
                )

            for round_index in range(self._limits.model_rounds):
                del round_index
                if cancellation.is_set():
                    return await finish(
                        RunState.CANCELLED,
                        error=error("cancelled", "The request was cancelled."),
                    )
                if remaining() <= 0:
                    return await finish(
                        RunState.FAILED,
                        error=error(
                            "tool_limit_reached", "The request time limit was reached."
                        ),
                    )

                await transition(RunState.MODEL_DECISION)
                composed = self._composer.compose(
                    question=request.question,
                    scope=resolution,
                    prior_turns=prior_turns,
                    tool_results=tuple(tool_results),
                )
                prompt_checksum = composed.checksum
                prompt_bytes = len(composed.system_text.encode()) + len(
                    composed.user_text.encode()
                )
                messages = (
                    AgentMessage(AgentMessageRole.SYSTEM, composed.system_text),
                    AgentMessage(AgentMessageRole.USER, composed.user_text),
                )
                tools = self._provider_tools()
                while True:
                    budget.require(prompt_bytes=prompt_bytes)
                    provider_timeout = min(self._limits.provider_seconds, remaining())
                    if provider_timeout <= 0:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The request time limit was reached.",
                            ),
                        )
                    try:
                        decision_result = await asyncio.wait_for(
                            self._provider.decide(
                                messages=messages,
                                tools=tools,
                                response_schema=DevAnswer.model_json_schema(
                                    mode="validation"
                                ),
                                timeout_seconds=provider_timeout,
                                max_output_tokens=self._limits.max_output_tokens_per_call,
                                signal=EventCancellationSignal(cancellation),
                            ),
                            timeout=provider_timeout,
                        )
                        break
                    except Exception as exc:
                        provider_error = safe_agent_provider_error(exc)
                        if (
                            provider_error.retryable
                            and retry_count < self._limits.provider_retries
                            and not cancellation.is_set()
                            and remaining() > 0
                        ):
                            retry_count += 1
                            continue
                        state = (
                            RunState.CANCELLED
                            if provider_error.code is AgentProviderErrorCode.CANCELLED
                            or cancellation.is_set()
                            else RunState.FAILED
                        )
                        return await finish(
                            state,
                            error=self._provider_error(
                                request.request_id, provider_error
                            ),
                        )

                budget.add(decision_result.usage)
                provider_fingerprint = decision_result.provider_fingerprint
                model_fingerprint = decision_result.model_fingerprint
                decision = decision_result.decision

                if isinstance(decision, AgentToolRequest):
                    if len(tool_results) >= self._limits.tool_calls:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached", "The tool-call limit was reached."
                            ),
                        )
                    await transition(RunState.TOOL_VALIDATION)
                    tool_request, canonical_hash = self._canonical_tool_request(
                        decision=decision,
                        run_id=run_id,
                        authorized_scope=authorized_scope,
                    )
                    duplicate_counts[canonical_hash] += 1
                    if (
                        duplicate_counts[canonical_hash]
                        > self._limits.identical_tool_calls
                    ):
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "A repeated tool-call loop was stopped.",
                            ),
                        )
                    await transition(RunState.TOOL_EXECUTION)
                    context = ToolExecutionContext(
                        org_id=org_id,
                        user_id=user_id,
                        permission_fingerprint=permission_fingerprint,
                        authorized_scope=authorized_scope,
                        cancellation=cancellation,
                        remaining_seconds=remaining(),
                    )
                    execution = await self._registry.execute(tool_request, context)
                    next_total = tool_bytes_total + execution.serialized_bytes
                    if next_total > self._limits.total_tool_bytes:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The tool-result budget was reached.",
                            ),
                        )
                    tool_results.append(execution.result)
                    tool_bytes_total = next_total
                    await self._recorder.record_tool(
                        ordinal=len(tool_results) - 1,
                        request=tool_request,
                        canonical_input_hash=canonical_hash,
                        execution=execution,
                    )
                    continue

                if isinstance(decision, AgentFinalAnswer):
                    await transition(RunState.ANSWER_VALIDATION)
                    model = DevModelMetadata(
                        provider_source=self._provider_source,
                        provider_family=self._provider_family,
                        model_fingerprint=decision_result.model_fingerprint,
                    )
                    validation_context = AnswerValidationContext(
                        conversation_id=conversation_id,
                        answer_id=answer_id,
                        scope_resolution=resolution,
                        versions=self._versions,
                        model=model,
                        tool_results=tuple(tool_results),
                    )
                    try:
                        answer = validate_answer_candidate(
                            dict(decision.value), validation_context
                        )
                    except AnswerValidationError as exc:
                        if (
                            exc.repairable
                            and repair_count < self._limits.schema_repairs
                        ):
                            repair_count += 1
                            prior_turns = prior_turns + (
                                PromptConversationTurn(
                                    role="assistant",
                                    content="The previous response failed schema validation. Return one corrected dev_answer.v1 object.",
                                ),
                            )
                            continue
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "answer_validation_failed",
                                "The answer failed grounding validation.",
                            ),
                        )
                    if (
                        len(answer.evidence) > self._limits.evidence_refs
                        or len(answer.metrics) > self._limits.metrics
                    ):
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "answer_validation_failed",
                                "The answer exceeds grounded-result limits.",
                            ),
                        )
                    return await finish(RunState.COMPLETED, answer=answer)

                if isinstance(decision, AgentDisambiguation):
                    return await finish(
                        RunState.INSUFFICIENT_EVIDENCE,
                        error=error(
                            "scope_ambiguous",
                            "The request requires scope clarification.",
                        ),
                    )
                if isinstance(decision, AgentRefusal):
                    return await finish(
                        RunState.REFUSED,
                        error=error(
                            "insufficient_evidence",
                            "The request is not supported by Ask Dev.",
                        ),
                    )
                return await finish(
                    RunState.FAILED,
                    error=error(
                        "internal_error",
                        "The provider returned an unsupported decision.",
                    ),
                )

            return await finish(
                RunState.FAILED,
                error=error(
                    "tool_limit_reached", "The model-decision limit was reached."
                ),
            )
        except ToolExecutionCancelled:
            return await finish(
                RunState.CANCELLED,
                error=error("cancelled", "The request was cancelled."),
            )
        except BudgetExceeded:
            return await finish(
                RunState.FAILED,
                error=error("cost_limit_reached", "The provider budget was reached."),
            )
        except ToolRegistryError:
            return await finish(
                RunState.FAILED,
                error=error(
                    "tool_unavailable", "The requested tool was not available."
                ),
            )
        except Exception:
            if terminal_written:
                raise
            return await finish(
                RunState.FAILED,
                error=error("internal_error", "The request could not be completed."),
            )

    def _provider_tools(self) -> tuple[AgentToolDefinition, ...]:
        manifest = self._registry.manifest()
        tools = manifest["tools"]
        assert isinstance(tools, list)
        return tuple(
            AgentToolDefinition(
                tool_id=str(item["tool_id"]),
                description=str(item["description"]),
                input_schema=DevToolRequest.model_json_schema(mode="validation"),
            )
            for item in tools
            if isinstance(item, Mapping)
        )

    @staticmethod
    def _canonical_tool_request(
        *, decision: AgentToolRequest, run_id: str, authorized_scope: DevScope
    ) -> tuple[DevToolRequest, str]:
        try:
            tool_id = ToolID(decision.tool_id)
        except ValueError as exc:
            raise ToolRegistryError("tool is not registered") from exc
        allowed = {
            "query",
            "metric_id",
            "evidence_ref_ids",
            "include_comparison",
            "limit",
        }
        server_owned = {"schema_version", "run_id", "tool_call_id", "tool_id", "scope"}
        unknown = set(decision.arguments) - allowed - server_owned
        if unknown:
            raise ToolRegistryError("tool request contains unsupported arguments")
        arguments = {
            key: value for key, value in decision.arguments.items() if key in allowed
        }
        payload: dict[str, Any] = {
            "schema_version": "dev_tool_request.v1",
            "run_id": run_id,
            "tool_call_id": decision.call_id,
            "tool_id": tool_id.value,
            "scope": authorized_scope.model_dump(mode="json"),
            **arguments,
        }
        request = DevToolRequest.model_validate(payload)
        canonical = json.dumps(
            {"tool_id": tool_id.value, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
        return request, digest

    @staticmethod
    def _provider_error(request_id: str, exc: AgentProviderError) -> DevError:
        code_map = {
            AgentProviderErrorCode.DISABLED: "feature_not_enabled",
            AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED: "provider_not_configured",
            AgentProviderErrorCode.MODEL_NOT_SUPPORTED: "model_not_supported",
            AgentProviderErrorCode.PROVIDER_UNAVAILABLE: "provider_unavailable",
            AgentProviderErrorCode.INVALID_RESPONSE: "internal_error",
            AgentProviderErrorCode.TIMEOUT: "provider_unavailable",
            AgentProviderErrorCode.CANCELLED: "cancelled",
        }
        return DevError(
            schema_version="dev_error.v1",
            request_id=request_id,
            code=code_map[exc.code],
            safe_message=str(exc),
            retryable=exc.retryable,
        )


__all__ = [
    "DevOrchestrator",
    "DevRunLimits",
    "EventCancellationSignal",
    "NullRunRecorder",
    "OrchestratorEvent",
    "OrchestratorResult",
    "ProviderBudget",
    "RunRecorder",
    "RunState",
    "ScopeResolver",
]
