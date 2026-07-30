"""Opt-in smoke coverage for an operator-configured GPT-5 Ask Dev path.

The direct readiness smoke requires ``ASK_DEV_LIVE_OPENAI_API_KEY``. The API
turn smoke requires an already configured authenticated Dev Health deployment:
``ASK_DEV_LIVE_API_BASE_URL``, ``ASK_DEV_LIVE_API_TOKEN``, and
``ASK_DEV_LIVE_ORG_ID``. Neither test prints credentials, prompts, or provider
payloads. They are intentionally outside the deterministic release gate.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessRecord,
    AgentReadinessService,
)


class _ReadinessStore:
    record: AgentReadinessRecord | None = None

    async def load(self) -> AgentReadinessRecord | None:
        return self.record

    async def save(self, record: AgentReadinessRecord) -> None:
        self.record = record


def _required_environment(*names: str) -> dict[str, str]:
    values = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.skip("live OpenAI smoke is not configured")
    return values


@pytest.mark.asyncio
@pytest.mark.live_openai
async def test_live_openai_gpt5_readiness() -> None:
    values = _required_environment("ASK_DEV_LIVE_OPENAI_API_KEY")
    model = os.getenv("ASK_DEV_LIVE_OPENAI_MODEL", "").strip() or "gpt-5-nano"
    base_url = os.getenv("ASK_DEV_LIVE_OPENAI_BASE_URL", "").strip() or None
    provider = OpenAICompatibleAgentProvider(
        api_key=values["ASK_DEV_LIVE_OPENAI_API_KEY"],
        model=model,
        base_url=base_url,
    )
    try:
        record = await AgentReadinessService(_ReadinessStore()).certify(
            provider,
            provider_name="openai",
            model=model,
            fingerprint=provider.model_fingerprint,
        )
    finally:
        await provider.aclose()

    assert record.outcome is AgentReadinessOutcome.READY
    assert record.safe_error_code is None


@pytest.mark.asyncio
@pytest.mark.live_openai
async def test_live_ask_dev_api_turn_uses_ready_platform_gpt5() -> None:
    values = _required_environment(
        "ASK_DEV_LIVE_API_BASE_URL",
        "ASK_DEV_LIVE_API_TOKEN",
        "ASK_DEV_LIVE_ORG_ID",
    )
    model = os.getenv("ASK_DEV_LIVE_OPENAI_MODEL", "").strip() or "gpt-5-nano"
    now = datetime.now(UTC)
    scope = {
        "schema_version": "dev_scope.v1",
        "organization_id": values["ASK_DEV_LIVE_ORG_ID"],
        "direct_scope": "organization",
        "repositories": [],
        "entity_refs": [],
        "team_ids": [],
        "time_range": {
            "start": (now - timedelta(days=14)).isoformat(),
            "end": now.isoformat(),
            "timezone": "UTC",
        },
    }
    headers = {"Authorization": f"Bearer {values['ASK_DEV_LIVE_API_TOKEN']}"}
    base_url = values["ASK_DEV_LIVE_API_BASE_URL"].rstrip("/")
    timeout = httpx.Timeout(60.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        capabilities = await client.get(
            f"{base_url}/api/v1/dev/capabilities", headers=headers
        )
        assert capabilities.status_code == 200
        capability = capabilities.json()
        assert capability["readiness"] == "ready"
        assert capability["provider_source"] == "platform"
        assert capability["effective_model_label"] == model

        created = await client.post(
            f"{base_url}/api/v1/dev/conversations",
            headers=headers,
            json={"current_scope": scope, "retention_days": 0},
        )
        assert created.status_code == 201
        conversation_id = created.json()["conversation_id"]
        stream = await client.post(
            f"{base_url}/api/v1/dev/conversations/{conversation_id}/messages",
            headers=headers,
            json={
                "schema_version": "dev_message_request.v1",
                "request_id": f"live-openai-{uuid.uuid4()}",
                "client_message_id": f"live-openai-{uuid.uuid4()}",
                "conversation_id": conversation_id,
                "question": "What is the current status of this organization?",
                "question_class": "status",
                "scope": scope,
                "requested_metric_ids": [],
            },
        )

    assert stream.status_code == 200
    events = [
        json.loads(line.removeprefix("data: "))
        for line in stream.text.splitlines()
        if line.startswith("data: ")
    ]
    assert events[-1]["event"] == "done"
    assert events[-1]["terminal_kind"] == "answer"
