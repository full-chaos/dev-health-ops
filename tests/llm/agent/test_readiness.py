from __future__ import annotations

import pytest

from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessRecord,
    AgentReadinessService,
)
from dev_health_ops.llm.agent.scripted import ScriptedAgentProvider, ScriptedStep


class Store:
    record: AgentReadinessRecord | None = None

    async def load(self) -> AgentReadinessRecord | None:
        return self.record

    async def save(self, record: AgentReadinessRecord) -> None:
        self.record = record


@pytest.mark.asyncio
async def test_certification_proves_tool_continuation_and_final_answer() -> None:
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                AgentToolRequest("readiness_echo", {"nonce": "ready-v1"}, "call-1"),
                AgentUsage(5, 2),
            ),
            ScriptedStep(AgentFinalAnswer({"nonce": "ready-v1"}), AgentUsage(4, 3)),
        ]
    )
    store = Store()
    record = await AgentReadinessService(store).certify(
        provider,
        provider_name="scripted",
        model="scripted-v1",
        fingerprint=provider.provider_fingerprint,
    )
    assert record.outcome is AgentReadinessOutcome.READY
    assert store.record == record
    assert record.is_current(
        fingerprint=provider.provider_fingerprint,
        readiness_version=provider.capabilities.readiness_version,
    )


@pytest.mark.asyncio
async def test_failed_certification_persists_only_safe_error_code() -> None:
    provider = ScriptedAgentProvider([ScriptedStep(AgentFinalAnswer({"wrong": True}))])
    store = Store()
    record = await AgentReadinessService(store).certify(
        provider,
        provider_name="scripted",
        model="scripted-v1",
        fingerprint=provider.provider_fingerprint,
    )
    assert record.outcome is AgentReadinessOutcome.FAILED
    assert record.safe_error_code == "invalid_response"


@pytest.mark.asyncio
async def test_scripted_provider_supports_shared_lifecycle() -> None:
    provider = ScriptedAgentProvider([])
    await provider.aclose()
