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
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol

from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
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
    AnswerStatus,
    DevAnswer,
    DevContractVersions,
    DevCoverage,
    DevError,
    DevEvidenceRef,
    DevMessageRequest,
    DevMetricRef,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    ScopeResolutionOutcome,
    ToolID,
)
from .org_policy import ASK_DEV_RUN_COST_HARD_MAX_MICROUSD
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
    "tool_seconds": 15.0,
    "provider_seconds": 30.0,
    "provider_retries": 1,
    "schema_repairs": 1,
    "total_tool_bytes": 256 * 1024,
    "per_tool_bytes": 64 * 1024,
    "evidence_refs": 25,
    "metrics": 12,
    "max_output_tokens_per_call": 4_096,
    "max_total_input_tokens": 100_000,
    "max_total_output_tokens": 16_384,
    "estimated_cost_per_call_microusd": 1_000_000,
    "max_estimated_cost_microusd": ASK_DEV_RUN_COST_HARD_MAX_MICROUSD,
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
    tool_seconds: float = 15.0
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
    max_estimated_cost_microusd: int = ASK_DEV_RUN_COST_HARD_MAX_MICROUSD

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


class RunDeadlineExceeded(RuntimeError):
    pass


@dataclass(slots=True)
class ProviderBudget:
    limits: DevRunLimits
    usage: AgentUsage = field(default_factory=AgentUsage)
    pending_input_reservations: list[int] = field(default_factory=list)

    def require(self, *, prompt_bytes: int) -> None:
        estimated_input_tokens = max(1, (prompt_bytes + 3) // 4)
        next_input_tokens = self.usage.input_tokens + estimated_input_tokens
        if next_input_tokens > self.limits.max_total_input_tokens:
            raise BudgetExceeded("input token budget exhausted")
        if (
            self.usage.output_tokens + self.limits.max_output_tokens_per_call
            > self.limits.max_total_output_tokens
        ):
            raise BudgetExceeded("output token budget exhausted")
        reserved_cost = self.limits.estimated_cost_per_call_microusd
        next_cost = (self.usage.estimated_cost_microusd or 0) + reserved_cost
        if next_cost > self.limits.max_estimated_cost_microusd:
            raise BudgetExceeded("provider cost budget exhausted")
        # Reserve before dispatch. Provider failures and responses without a cost
        # estimate must never become free merely because exact billing is unknown.
        self.usage = AgentUsage(
            input_tokens=next_input_tokens,
            output_tokens=self.usage.output_tokens,
            estimated_cost_microusd=next_cost,
        )
        self.pending_input_reservations.append(estimated_input_tokens)

    def add(self, usage: AgentUsage) -> None:
        if usage.input_tokens < 0 or usage.output_tokens < 0:
            raise BudgetExceeded("provider returned invalid token usage")
        if (
            usage.estimated_cost_microusd is not None
            and usage.estimated_cost_microusd < 0
        ):
            raise BudgetExceeded("provider returned invalid cost usage")
        prior_cost = self.usage.estimated_cost_microusd or 0
        if usage.estimated_cost_microusd is None:
            reconciled_cost = prior_cost
        else:
            reconciled_cost = (
                max(
                    0,
                    prior_cost - self.limits.estimated_cost_per_call_microusd,
                )
                + usage.estimated_cost_microusd
            )
        reserved_input = (
            self.pending_input_reservations.pop()
            if self.pending_input_reservations
            else 0
        )
        self.usage = AgentUsage(
            input_tokens=max(0, self.usage.input_tokens - reserved_input)
            + usage.input_tokens,
            output_tokens=self.usage.output_tokens + usage.output_tokens,
            estimated_cost_microusd=reconciled_cost,
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
        event_sink: EventSink | None = None,
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
        resolution: DevScopeResolution | None = None
        terminal_written = False
        repair_count = 0
        retry_count = 0
        selected_event_sink = event_sink or self._event_sink

        async def transition(state: RunState, safe_code: str | None = None) -> None:
            event = OrchestratorEvent(state=state, safe_code=safe_code)
            events.append(event)
            await self._recorder.transition(state)
            if selected_event_sink is not None:
                await selected_event_sink(event)

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
            if selected_event_sink is not None:
                await selected_event_sink(event)
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
            resolution = await self._resolve_with_cancellation(
                org_id=org_id,
                user_id=user_id,
                requested_scope=request.scope,
                cancellation=cancellation,
                timeout_seconds=max(0, remaining()),
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
                        decision_result = await self._decide_with_cancellation(
                            messages=messages,
                            tools=tools,
                            timeout_seconds=provider_timeout,
                            cancellation=cancellation,
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
                    tool_remaining = min(remaining(), self._limits.tool_seconds)
                    if tool_remaining <= 0:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The request time limit was reached.",
                            ),
                        )
                    context = ToolExecutionContext(
                        org_id=org_id,
                        user_id=user_id,
                        permission_fingerprint=permission_fingerprint,
                        authorized_scope=authorized_scope,
                        cancellation=cancellation,
                        remaining_seconds=tool_remaining,
                    )
                    execution = await self._registry.execute(tool_request, context)
                    if execution.serialized_bytes > self._limits.per_tool_bytes:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The tool-result budget was reached.",
                            ),
                        )
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
                    candidate = dict(decision.value)
                    candidate.update(
                        {
                            "schema_version": "dev_answer.v1",
                            "answer_id": answer_id,
                            "conversation_id": conversation_id,
                            "resolved_scope": resolution.model_dump(mode="json"),
                            "versions": self._versions.model_dump(mode="json"),
                            "model": model.model_dump(mode="json"),
                        }
                    )
                    try:
                        answer = validate_answer_candidate(
                            candidate, validation_context
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
            if (
                resolution is not None
                and provider_fingerprint is not None
                and model_fingerprint is not None
            ):
                partial = self._budget_answer(
                    answer_id=answer_id,
                    conversation_id=conversation_id,
                    resolution=resolution,
                    tool_results=tuple(tool_results),
                    model_fingerprint=model_fingerprint,
                )
                if partial is not None:
                    return await finish(RunState.COMPLETED, answer=partial)
            return await finish(
                RunState.FAILED,
                error=error("cost_limit_reached", "The provider budget was reached."),
            )
        except RunDeadlineExceeded:
            return await finish(
                RunState.FAILED,
                error=error(
                    "tool_limit_reached", "The request time limit was reached."
                ),
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

    async def _resolve_with_cancellation(
        self,
        *,
        org_id: str,
        user_id: str,
        requested_scope: DevScope,
        cancellation: asyncio.Event,
        timeout_seconds: float,
    ) -> DevScopeResolution:
        if timeout_seconds <= 0:
            raise RunDeadlineExceeded("scope resolution exceeded the run deadline")
        resolver_task: asyncio.Future[DevScopeResolution] = asyncio.ensure_future(
            self._scope_resolver(
                org_id=org_id, user_id=user_id, requested_scope=requested_scope
            )
        )
        cancellation_task = asyncio.create_task(cancellation.wait())
        wait_set: set[asyncio.Future[Any]] = {resolver_task, cancellation_task}
        try:
            done, _ = await asyncio.wait(
                wait_set,
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_task in done:
                resolver_task.cancel()
                await asyncio.gather(resolver_task, return_exceptions=True)
                raise ToolExecutionCancelled("scope resolution cancelled")
            if resolver_task not in done:
                resolver_task.cancel()
                await asyncio.gather(resolver_task, return_exceptions=True)
                raise RunDeadlineExceeded("scope resolution exceeded the run deadline")
            return resolver_task.result()
        finally:
            cancellation_task.cancel()
            await asyncio.gather(cancellation_task, return_exceptions=True)

    async def _decide_with_cancellation(
        self,
        *,
        messages: tuple[AgentMessage, ...],
        tools: tuple[AgentToolDefinition, ...],
        timeout_seconds: float,
        cancellation: asyncio.Event,
    ) -> AgentDecisionResult:
        provider_task: asyncio.Future[AgentDecisionResult] = asyncio.ensure_future(
            self._provider.decide(
                messages=messages,
                tools=tools,
                response_schema=DevAnswer.model_json_schema(mode="validation"),
                timeout_seconds=timeout_seconds,
                max_output_tokens=self._limits.max_output_tokens_per_call,
                signal=EventCancellationSignal(cancellation),
            )
        )
        cancellation_task = asyncio.create_task(cancellation.wait())
        wait_set: set[asyncio.Future[Any]] = {provider_task, cancellation_task}
        try:
            done, _ = await asyncio.wait(
                wait_set,
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_task in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
            if provider_task not in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
            return provider_task.result()
        finally:
            cancellation_task.cancel()
            await asyncio.gather(cancellation_task, return_exceptions=True)

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

    def _budget_answer(
        self,
        *,
        answer_id: str,
        conversation_id: str,
        resolution: DevScopeResolution,
        tool_results: tuple[DevToolResult, ...],
        model_fingerprint: str,
    ) -> DevAnswer | None:
        """Return only canonical retrieved data when a later model call is blocked."""

        evidence: dict[str, DevEvidenceRef] = {}
        metrics: dict[str, DevMetricRef] = {}
        for result in tool_results:
            for evidence_item in result.evidence:
                current_evidence = evidence.setdefault(
                    evidence_item.evidence_ref_id, evidence_item
                )
                if current_evidence != evidence_item:
                    return None
            for metric_item in result.metrics:
                current_metric = metrics.setdefault(
                    metric_item.metric_ref_id, metric_item
                )
                if current_metric != metric_item:
                    return None
        if not evidence and not metrics:
            return None
        canonical_evidence = list(evidence.values())[: self._limits.evidence_refs]
        allowed_evidence_ids = {item.evidence_ref_id for item in canonical_evidence}
        canonical_metrics = [
            item
            for item in metrics.values()
            if set(item.evidence_ref_ids) <= allowed_evidence_ids
        ][: self._limits.metrics]
        now = datetime.now(UTC)
        degraded = any(
            result.status in {"unavailable", "error"} for result in tool_results
        )
        return DevAnswer(
            schema_version="dev_answer.v1",
            answer_id=answer_id,
            conversation_id=conversation_id,
            generated_at=now,
            resolved_scope=resolution,
            as_of=now,
            status=AnswerStatus.DEGRADED if degraded else AnswerStatus.PARTIAL,
            direct_summary=(
                "The provider budget was reached. This answer contains only the "
                "validated data retrieved before the limit."
            ),
            claims=[],
            metrics=canonical_metrics,
            evidence=canonical_evidence,
            conflicts=[],
            coverage=DevCoverage(
                required_source_count=1,
                available_source_count=0 if degraded else 1,
                unavailable_required_sources=["tool_results"] if degraded else [],
                stale_required_sources=[],
                as_of=now,
            ),
            warnings=[
                "The provider budget was reached; no additional model call was made."
            ],
            suggested_follow_up_questions=[],
            versions=self._versions,
            model=DevModelMetadata(
                provider_source=self._provider_source,
                provider_family=self._provider_family,
                model_fingerprint=model_fingerprint,
            ),
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
