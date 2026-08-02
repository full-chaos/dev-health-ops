"""Opt-in smoke coverage for an operator-configured GPT-5 Ask Dev path.

The direct readiness smoke requires ``ASK_DEV_LIVE_OPENAI_API_KEY``. The API
turn smoke requires an already configured authenticated Dev Health deployment:
``ASK_DEV_LIVE_API_BASE_URL``, ``ASK_DEV_LIVE_API_TOKEN``, and
``ASK_DEV_LIVE_ORG_ID``. Neither test prints credentials, prompts, or provider
payloads. They are intentionally outside the deterministic release gate.

Missing-credential behavior (CHAOS-3285 acceptance: "missing credentials
clearly leave the live gate non-passing rather than silently skipped"):

* By default (``ASK_DEV_LIVE_GATE`` unset) -- an ordinary local run without
  live credentials configured -- missing env vars still ``pytest.skip``.
  This is the only mode where skipping is legitimate: nobody asked for the
  live gate to run here.
* When ``ASK_DEV_LIVE_GATE=1`` is set -- the explicit signal that this run
  IS the live convergence gate, not an incidental local pass -- missing env
  vars ``pytest.fail`` instead. A gate that silently skips its own
  measurement reads as passing coverage it never actually took (verification
  Rule 4); the whole point of the flag is to make that failure loud instead.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.probes.legacy_agent import certify_legacy_agent
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessRecord,
    AgentReadinessService,
)
from dev_health_ops.llm.agent.roles import RoleCertificationState

#: Set to "1" to mark this run as the Wave 3.1 live convergence gate: missing
#: credentials become a hard failure instead of a skip. Unset (or any other
#: value) preserves today's default local-run behavior.
LIVE_GATE_ENV_VAR = "ASK_DEV_LIVE_GATE"


class _ReadinessStore:
    record: AgentReadinessRecord | None = None

    async def load(self) -> AgentReadinessRecord | None:
        return self.record

    async def save(self, record: AgentReadinessRecord) -> None:
        self.record = record


def _live_gate_required() -> bool:
    return os.getenv(LIVE_GATE_ENV_VAR, "").strip() == "1"


def _required_environment(*names: str) -> dict[str, str]:
    values = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        reason = f"live OpenAI smoke is not configured (missing: {', '.join(missing)})"
        if _live_gate_required():
            pytest.fail(
                f"{reason}. {LIVE_GATE_ENV_VAR}=1 requires these credentials to be "
                "present -- the live gate must fail, not skip, when it cannot take "
                "its measurement."
            )
        pytest.skip(reason)
    return values


def test_missing_credentials_skip_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ordinary local-run case: no live gate requested, so a missing
    credential is a legitimate skip, unchanged from before CHAOS-3285."""

    monkeypatch.delenv(LIVE_GATE_ENV_VAR, raising=False)
    monkeypatch.delenv("_ASK_DEV_LIVE_SMOKE_TEST_VAR", raising=False)

    with pytest.raises(pytest.skip.Exception):
        _required_environment("_ASK_DEV_LIVE_SMOKE_TEST_VAR")


def test_missing_credentials_fail_loudly_when_gate_is_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 acceptance: with the live gate explicitly requested, a
    missing credential must FAIL (non-passing), never silently skip -- the
    exact defect class verification Rule 4 exists to catch (a measurement
    that did not happen must not read as coverage)."""

    monkeypatch.setenv(LIVE_GATE_ENV_VAR, "1")
    monkeypatch.delenv("_ASK_DEV_LIVE_SMOKE_TEST_VAR", raising=False)

    with pytest.raises(pytest.fail.Exception):
        _required_environment("_ASK_DEV_LIVE_SMOKE_TEST_VAR")


def test_present_credentials_are_returned_regardless_of_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate flag only changes missing-credential behavior; a fully
    configured run is unaffected either way."""

    monkeypatch.setenv(LIVE_GATE_ENV_VAR, "1")
    monkeypatch.setenv("_ASK_DEV_LIVE_SMOKE_TEST_VAR", "configured-value")

    assert _required_environment("_ASK_DEV_LIVE_SMOKE_TEST_VAR") == {
        "_ASK_DEV_LIVE_SMOKE_TEST_VAR": "configured-value"
    }


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
        assert record.outcome is AgentReadinessOutcome.READY
        assert record.safe_error_code is None

        # CHAOS-3285 round 2 (Codex MEDIUM): the live gate must also run the
        # REAL production-sized legacy_agent probe against the real API --
        # not just the old 512-token echo probe above, which cannot observe
        # output/reasoning exhaustion at all. This is exactly the empirical
        # measurement the CHAOS-3285 plan's live-verification phase calls
        # for: does the configured model actually pass the production
        # request shape, against the real provider, right now.
        #
        # legacy_agent is deliberately NOT asserted COMPATIBLE
        # unconditionally: per the ratified TRD Option B, this role may
        # legitimately exhaust the existing 4,096-token envelope -- THAT is
        # the measurement this run is taking, not a test failure. Only a
        # genuinely unclassified outcome (an exception certify_legacy_agent
        # itself does not already turn into a state) fails this test.
        probe_result = await certify_legacy_agent(provider, timeout_seconds=60)
        print(
            "[ASK_DEV_LIVE_GATE] legacy_agent role vs "
            f"{model}: state={probe_result.state.value} "
            f"safe_error_code={probe_result.safe_error_code} "
            f"reasoning_tokens={probe_result.usage.reasoning_tokens} "
            f"output_tokens={probe_result.usage.output_tokens}"
        )
        assert probe_result.state in (
            RoleCertificationState.COMPATIBLE,
            RoleCertificationState.INCOMPATIBLE,
            RoleCertificationState.FAILED,
        )
    finally:
        await provider.aclose()


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
