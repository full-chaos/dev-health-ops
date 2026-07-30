from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevScope, DevToolRequest, ToolID
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.dev.tool_registry import ToolExecutionContext
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.policy import AgentProviderCandidate, AgentProviderSource
from dev_health_ops.llm.credentials import LLMCredentials


class FakeProvider:
    def __init__(self) -> None:
        self.closed = False

    async def decide(self, **_values):
        raise AssertionError("provider calls are outside this construction test")

    async def aclose(self) -> None:
        self.closed = True


class FakeSettingsService:
    values: dict[str, str] = {}

    def __init__(self, _session, _org_id: str) -> None:
        pass

    async def get(self, key: str, category: str, default=None):
        del category
        return self.values.get(key, default)


def _fingerprint(
    base_url: str = "", model: str = "certified-model", provider: str = "openai"
) -> str:
    return hashlib.sha256(
        "\0".join(("platform", provider, model, base_url, READINESS_VERSION)).encode()
    ).hexdigest()[:24]


def test_readiness_fingerprint_changes_when_source_changes() -> None:
    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    platform = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )
    byo = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.BYO,
    )

    assert production_runtime._readiness_fingerprint(
        platform
    ) != production_runtime._readiness_fingerprint(byo)


@pytest.mark.asyncio
async def test_provider_resolution_requires_current_certification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime,
        "_provider",
        lambda _candidate: FakeProvider(),
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {
        "ask_dev_agent_readiness": json.dumps(
            {
                "fingerprint": _fingerprint(),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        )
    }

    session = cast(Any, object())
    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.family == "openai"
    assert resolved.model == "certified-model"

    monkeypatch.setenv("LLM_MODEL", "changed-model")
    with pytest.raises(DevRuntimeUnavailable) as model_change:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert model_change.value.code == "provider_not_configured"
    monkeypatch.setenv("LLM_MODEL", "certified-model")

    FakeSettingsService.values["ask_dev_agent_readiness"] = json.dumps(
        {
            "fingerprint": "stale-fingerprint",
            "readiness_version": READINESS_VERSION,
            "checked_at": "2026-07-29T12:00:00+00:00",
            "outcome": "ready",
            "safe_error_code": None,
        }
    )
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_platform_local_provider_uses_only_operator_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    candidates: list[Any] = []

    def provider(candidate):
        candidates.append(candidate)
        return FakeProvider()

    monkeypatch.setattr(production_runtime, "_provider", provider)
    attached: list[dict[str, Any]] = []

    def attach(value, **kwargs):
        attached.append(kwargs)
        return value

    monkeypatch.setattr(production_runtime, "attach_agent_budget_guard", attach)
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LLM_MODEL", "google/gemma-4-e4b")
    monkeypatch.setenv("LOCAL_LLM_MODEL", "google/gemma-4-e4b")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", "http://host.docker.internal:1234/v1")
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("LOCAL_LLM_API_KEY", raising=False)
    FakeSettingsService.values = {
        "ask_dev_agent_readiness": json.dumps(
            {
                "fingerprint": _fingerprint(
                    provider="local",
                    model="google/gemma-4-e4b",
                    base_url="http://host.docker.internal:1234/v1",
                ),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        # A complete organization BYO bundle remains database-owned and does
        # not overwrite the independently resolved platform candidate.
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
    }

    resolved = await production_runtime.resolve_production_provider(
        cast(Any, object()), org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.family == "local"
    assert resolved.model == "google/gemma-4-e4b"
    assert len(candidates) == 1
    assert candidates[0].credentials.api_key == ""
    assert candidates[0].credentials.base_url == "http://host.docker.internal:1234/v1"
    assert attached == []


@pytest.mark.asyncio
async def test_explicit_fail_closed_prevents_platform_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LOCAL_LLM_MODEL", "local-agent-model")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", "http://host.docker.internal:1234/v1")
    FakeSettingsService.values = {
        "ask_dev_agent_readiness": json.dumps(
            {
                "fingerprint": _fingerprint(
                    provider="local",
                    model="local-agent-model",
                    base_url="http://host.docker.internal:1234/v1",
                ),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
        "ask_dev_platform_fallback": "fail_closed",
    }

    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(
            cast(Any, object()), org_id="org_01"
        )

    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_byo_provider_resolution_attaches_shared_budget_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    provider = FakeProvider()
    monkeypatch.setattr(production_runtime, "_provider", lambda _candidate: provider)
    attached: list[dict[str, Any]] = []

    def attach(value, **kwargs):
        attached.append(kwargs)
        return value

    monkeypatch.setattr(production_runtime, "attach_agent_budget_guard", attach)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    FakeSettingsService.values = {
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_certification_provider(
        session, org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.BYO
    assert resolved.provider is provider
    assert attached == [
        {
            "session": session,
            "org_id": "org_01",
            "provider": "openai",
            "model": "gpt-5-mini",
            "base_url": "https://api.openai.com/v1",
        }
    ]


@pytest.mark.asyncio
async def test_production_runtime_wires_exactly_the_nine_registered_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def resolve_provider(_session, *, org_id: str):
        assert org_id == "org_01"
        return ProductionProviderResolution(
            provider=cast(Any, FakeProvider()),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret-32-bytes")

    runtime = await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id="org_01",
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )
    manifest = cast(Any, runtime.registry.manifest())
    assert {item["tool_id"] for item in manifest["tools"]} == {
        item.value for item in ToolID
    }
    assert len(manifest["tools"]) == 9

    scope = DevScope.model_validate(positive_fixtures()["dev_scope.v1"])
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.LIST_METRICS,
            scope=scope,
            limit=8,
        ),
        ToolExecutionContext(
            org_id=scope.organization_id,
            user_id="user_01",
            permission_fingerprint="permissions_01",
            authorized_scope=scope,
            cancellation=asyncio.Event(),
            remaining_seconds=5,
        ),
    )
    assert execution.result.warnings == []
    assert execution.result.metric_definitions
    assert execution.result.metric_definitions[0].description
    assert execution.result.metric_definitions[0].supported_time_grains
    await runtime.aclose()


@pytest.mark.asyncio
async def test_runtime_construction_failure_closes_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeProvider()

    async def resolve_provider(_session, *, org_id: str):
        assert org_id == "org_01"
        return ProductionProviderResolution(
            provider=cast(Any, provider),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.delenv("JWT_SECRET_KEY", raising=False)

    with pytest.raises(DevRuntimeUnavailable):
        await production_runtime.build_production_runtime(
            cast(Any, object()),
            org_id="org_01",
            permission_fingerprint="permissions_01",
            clickhouse=cast(Any, object()),
        )
    assert provider.closed is True
