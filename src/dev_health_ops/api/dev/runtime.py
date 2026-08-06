"""Server-owned construction seam for one bounded Ask Dev run."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from dev_health_ops.llm.agent.contracts import AgentLLMProvider

from .answer_frames import narrative_fallback
from .contracts import DevContractVersions, DevMessageRequest
from .contracts_v2.base import QuestionIntentID
from .contracts_v2.plan import DevInvestigationPlan
from .investigation_plans import PlanExecutor
from .orchestrator import (
    DevOrchestrator,
    EventSink,
    OrchestratorResult,
    RunRecorder,
    ScopeResolver,
)
from .prompts import PromptConversationTurn
from .qua_shadow import QuestionUnderstandingShadow
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
    #: CHAOS-3358. Carried from ProductionProviderResolution so the authorized
    #: run path can heal a stale platform certification. Never acted on during
    #: runtime construction itself -- construction happens on read paths that
    #: have not checked Ask Dev authorization yet.
    platform_certification_stale: bool = False
    #: ``None`` when ``ask_dev_wave_3_1`` is off for this organization, which
    #: is the pre-CHAOS-3292 run path.
    preflight: SubjectPreflight | None = None
    #: CHAOS-3295. Both ``None`` together is the flag-off path (identical to
    #: today whether or not ``preflight`` is set); production only sets
    #: these once ``preflight`` is also set.
    plan_registry: Mapping[QuestionIntentID, DevInvestigationPlan] | None = None
    plan_executor: PlanExecutor | None = None
    #: CHAOS-3297 stack #4. ``None`` is the only certified state in
    #: production today (no narrative provider is certified yet --
    #: CHAOS-3285's territory); tests inject a scripted one through this
    #: same seam to drive the live C3/C4 provider-failure-matrix controls
    #: through the real endpoint.
    narrative_provider: narrative_fallback.NarrativeProvider | None = None
    #: CHAOS-3389 shadow phase. ``None`` is the flag-off path -- identical to
    #: today whether or not ``preflight`` is set. Production only sets this
    #: once ``preflight`` is also set (the shadow seam runs alongside the
    #: deterministic resolver, so it is meaningless without it).
    qua_shadow: QuestionUnderstandingShadow | None = None
    #: CHAOS-3452. The SAME provider instance passed into ``qua_shadow``'s
    #: constructor when the isolated shadow quota is wired -- carried here
    #: separately so ``aclose()`` below can close it too. A genuinely
    #: separate provider instance/HTTP client from ``self.provider``;
    #: without this it would leak a connection on every request once the
    #: shadow flag is enabled.
    qua_shadow_provider: AgentLLMProvider | None = None

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
            plan_registry=self.plan_registry,
            plan_executor=self.plan_executor,
            narrative_provider=self.narrative_provider,
            qua_shadow=self.qua_shadow,
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
        # Codex round 1 (MEDIUM, confirmed): a ``finally`` -- not a second
        # statement -- so the shadow provider's own client/connection is
        # still closed even when ``self.provider.aclose()`` itself raises;
        # every caller of ``aclose()`` (router.py) wraps the WHOLE call in a
        # swallowing try/except, so a live-provider close failure used to
        # silently skip shadow cleanup too, leaking its connection.
        try:
            await self.provider.aclose()
        finally:
            if self.qua_shadow_provider is not None:
                await self.qua_shadow_provider.aclose()


__all__ = ["BoundedDevRuntime", "DevRuntimeUnavailable"]
