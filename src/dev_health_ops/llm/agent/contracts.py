"""Provider-neutral contracts for multi-turn Ask Dev model decisions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, TypeAlias


class AgentMessageRole(str, Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


@dataclass(frozen=True, slots=True)
class AgentMessage:
    role: AgentMessageRole
    content: str
    tool_call_id: str | None = None
    tool_request: AgentToolRequest | None = None


@dataclass(frozen=True, slots=True)
class AgentToolDefinition:
    tool_id: str
    description: str
    input_schema: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class AgentToolRequest:
    tool_id: str
    arguments: Mapping[str, Any]
    call_id: str


@dataclass(frozen=True, slots=True)
class AgentFinalAnswer:
    value: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class AgentDisambiguation:
    prompt: str
    candidates: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AgentRefusal:
    code: str
    message: str


AgentDecision: TypeAlias = (
    AgentToolRequest | AgentFinalAnswer | AgentDisambiguation | AgentRefusal
)


@dataclass(frozen=True, slots=True)
class AgentUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_microusd: int | None = None
    cached_input_tokens: int | None = None
    # Hidden reasoning tokens billed as part of output_tokens on
    # reasoning-tier models (gpt-5*, o-series). None when the provider
    # response carries no completion_tokens_details.reasoning_tokens field --
    # distinct from 0, which means the provider reported the field as zero
    # (CHAOS-3285).
    reasoning_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class AgentDecisionResult:
    decision: AgentDecision
    usage: AgentUsage
    latency_ms: int
    provider_fingerprint: str
    model_fingerprint: str


class StructuredOutputMode(str, Enum):
    JSON_SCHEMA = "json_schema"


class ToolDecisionMode(str, Enum):
    NATIVE = "native"
    JSON = "json"


class StreamingMode(str, Enum):
    BUFFERED = "buffered"
    STREAMING = "streaming"


@dataclass(frozen=True, slots=True)
class AgentProviderCapabilities:
    structured_output: StructuredOutputMode
    tool_decisions: ToolDecisionMode
    streaming: StreamingMode
    supports_cancellation: bool
    context_window_tokens: int | None
    max_output_tokens: int | None
    readiness_version: str
    disclosure_key: str


class CancellationSignal(Protocol):
    def is_cancelled(self) -> bool: ...

    async def wait(self) -> None: ...


class AgentLLMProvider(Protocol):
    @property
    def capabilities(self) -> AgentProviderCapabilities: ...

    async def decide(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[AgentToolDefinition],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: CancellationSignal | None = None,
    ) -> AgentDecisionResult: ...

    async def aclose(self) -> None: ...
