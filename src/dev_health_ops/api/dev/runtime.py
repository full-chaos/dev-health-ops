"""Server-owned construction seam for one bounded Ask Dev run."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal

from dev_health_ops.llm.agent.contracts import AgentLLMProvider

from .contracts import DevContractVersions, DevMessageRequest
from .orchestrator import (
    DevOrchestrator,
    EventSink,
    OrchestratorResult,
    RunRecorder,
    ScopeResolver,
)
from .prompts import PromptConversationTurn
from .subject_preflight import SubjectPreflight
from .tool_registry import AskDevToolRegistry


class DevRuntimeUnavailable(RuntimeError):
    def __init__(self, code: str, safe_message: str) -> None:
        self.code = code
        self.safe_message = safe_message
        super().__init__(code)


@dataclass(slots=True)
class BoundedDevRuntime:
    provider: AgentLLMProvider
    provider_source: Literal["platform", "byo"]
    provider_family: str
    registry: AskDevToolRegistry
    scope_resolver: ScopeResolver
    versions: DevContractVersions
    #: ``None`` when ``ask_dev_wave_3_1`` is off for this organization, which
    #: is the pre-CHAOS-3292 run path.
    preflight: SubjectPreflight | None = None

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
        recorder: RunRecorder,
        event_sink: EventSink,
        prior_turns: tuple[PromptConversationTurn, ...] = (),
    ) -> OrchestratorResult:
        orchestrator = DevOrchestrator(
            provider=self.provider,
            provider_source=self.provider_source,
            provider_family=self.provider_family,
            registry=self.registry,
            scope_resolver=self.scope_resolver,
            versions=self.versions,
            recorder=recorder,
            preflight=self.preflight,
        )
        return await orchestrator.run(
            request=request,
            org_id=org_id,
            user_id=user_id,
            permission_fingerprint=permission_fingerprint,
            run_id=run_id,
            conversation_id=conversation_id,
            answer_id=answer_id,
            cancellation=cancellation,
            prior_turns=prior_turns,
            event_sink=event_sink,
        )

    async def aclose(self) -> None:
        await self.provider.aclose()


__all__ = ["BoundedDevRuntime", "DevRuntimeUnavailable"]
