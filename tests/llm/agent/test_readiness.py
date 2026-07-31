from __future__ import annotations

import pytest

from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.openai_compatible import (
    OpenAICompatibleAgentProvider,
    _fingerprint,
)
from dev_health_ops.llm.agent.readiness import (
    READINESS_MAX_OUTPUT_TOKENS,
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


class CapturingScriptedProvider(ScriptedAgentProvider):
    def __init__(self, steps: list[ScriptedStep]):
        super().__init__(steps)
        self.calls: list[dict[str, object]] = []

    async def decide(self, *args, **kwargs):
        self.calls.append(
            {
                "response_schema": args[2],
                "tool_count": len(args[1]),
                "last_message": args[0][-1].content,
                "timeout_seconds": args[3],
                "max_output_tokens": args[4],
            }
        )
        return await super().decide(*args, **kwargs)


@pytest.mark.asyncio
async def test_certification_proves_tool_continuation_and_final_answer() -> None:
    provider = CapturingScriptedProvider(
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
    assert [call["timeout_seconds"] for call in provider.calls] == [30, 30]
    assert [call["tool_count"] for call in provider.calls] == [1, 0]
    assert provider.calls[1]["last_message"] == (
        'Return a final_answer now with value exactly {"nonce":"ready-v1"}. '
        "Do not request another tool."
    )
    assert [call["max_output_tokens"] for call in provider.calls] == [
        READINESS_MAX_OUTPUT_TOKENS,
        READINESS_MAX_OUTPUT_TOKENS,
    ]
    assert provider.calls[0]["response_schema"] == {
        "type": "object",
        "additionalProperties": False,
        "required": ["nonce"],
        "properties": {
            "nonce": {
                "type": "string",
                "const": "ready-v1",
            }
        },
    }


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
async def test_repeated_tool_request_on_final_answer_turn_fails_closed() -> None:
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                AgentToolRequest("readiness_echo", {"nonce": "ready-v1"}, "call-1")
            ),
            ScriptedStep(
                AgentToolRequest("readiness_echo", {"nonce": "ready-v1"}, "call-2")
            ),
        ]
    )
    record = await AgentReadinessService(Store()).certify(
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


def test_wire_contract_change_invalidates_a_pre_change_readiness_record() -> None:
    """CHAOS-3254: bumping READINESS_VERSION must invalidate stale certifications.

    A provider certified under the prior wire contract (before native tool
    requests started explicitly sending ``parallel_tool_calls``) never
    demonstrated that its endpoint accepts the new parameter. Simulate
    exactly that pre-change persisted record and confirm the CURRENT
    provider no longer treats it as current -- it must be re-certified, not
    silently trusted to handle a request shape it was never tested against.
    """
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", base_url="http://127.0.0.1:1/v1"
    )
    stale_readiness_version = "ask-dev-agent-v2"
    stale_fingerprint = _fingerprint(
        "openai-compatible", provider.base_url, stale_readiness_version
    )
    pre_change_record = AgentReadinessRecord(
        fingerprint=stale_fingerprint,
        readiness_version=stale_readiness_version,
        checked_at="2026-01-01T00:00:00+00:00",
        outcome=AgentReadinessOutcome.READY,
    )

    # The wire contract genuinely changed -- confirm this isn't a vacuous
    # comparison against itself.
    assert stale_readiness_version != provider.capabilities.readiness_version
    assert stale_fingerprint != provider.provider_fingerprint

    assert not pre_change_record.is_current(
        fingerprint=provider.provider_fingerprint,
        readiness_version=provider.capabilities.readiness_version,
    )
