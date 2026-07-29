"""Deterministic provider used by Ask Dev certification and acceptance tests."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .contracts import (
    AgentDecision,
    AgentDecisionResult,
    AgentMessage,
    AgentProviderCapabilities,
    AgentToolDefinition,
    AgentUsage,
    CancellationSignal,
    StreamingMode,
    StructuredOutputMode,
    ToolDecisionMode,
)
from .errors import AgentProviderError, AgentProviderErrorCode

SCRIPTED_READINESS_VERSION = "ask-dev-agent-v1"


@dataclass(frozen=True, slots=True)
class ScriptedStep:
    decision: AgentDecision | None = None
    usage: AgentUsage = AgentUsage()
    delay_seconds: float = 0
    error: AgentProviderError | None = None


class ScriptedAgentProvider:
    def __init__(self, steps: Sequence[ScriptedStep]):
        self._steps = deque(steps)
        self._fingerprint = hashlib.sha256(repr(tuple(steps)).encode()).hexdigest()[:24]
        self._capabilities = AgentProviderCapabilities(
            structured_output=StructuredOutputMode.JSON_SCHEMA,
            tool_decisions=ToolDecisionMode.NATIVE,
            streaming=StreamingMode.BUFFERED,
            supports_cancellation=True,
            context_window_tokens=100_000,
            max_output_tokens=10_000,
            readiness_version=SCRIPTED_READINESS_VERSION,
            disclosure_key="scripted",
        )

    @property
    def capabilities(self) -> AgentProviderCapabilities:
        return self._capabilities

    @property
    def provider_fingerprint(self) -> str:
        return self._fingerprint

    @property
    def model_fingerprint(self) -> str:
        return self._fingerprint

    async def decide(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[AgentToolDefinition],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: CancellationSignal | None = None,
    ) -> AgentDecisionResult:
        del messages, tools, response_schema, max_output_tokens
        if not self._steps:
            raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
        if signal is not None and signal.is_cancelled():
            raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
        step = self._steps.popleft()
        started = time.monotonic()
        delay = asyncio.create_task(asyncio.sleep(step.delay_seconds))
        cancel = asyncio.create_task(signal.wait()) if signal is not None else None
        try:
            waiters = {delay}
            if cancel is not None:
                waiters.add(cancel)
            done, _ = await asyncio.wait(
                waiters, timeout=timeout_seconds, return_when=asyncio.FIRST_COMPLETED
            )
            if cancel is not None and cancel in done:
                raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
            if delay not in done:
                raise AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
        finally:
            delay.cancel()
            await asyncio.gather(delay, return_exceptions=True)
            if cancel is not None:
                cancel.cancel()
                await asyncio.gather(cancel, return_exceptions=True)
        if step.error:
            raise step.error
        if step.decision is None:
            raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
        return AgentDecisionResult(
            decision=step.decision,
            usage=step.usage,
            latency_ms=max(0, round((time.monotonic() - started) * 1000)),
            provider_fingerprint=self.provider_fingerprint,
            model_fingerprint=self.model_fingerprint,
        )

    async def aclose(self) -> None:
        """Match the production provider lifecycle without owning resources."""
