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
from dev_health_ops.llm.agent.policy import AgentProviderSource


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


def _fingerprint(base_url: str = "") -> str:
    return hashlib.sha256(
        "\0".join(("openai-compatible", base_url, READINESS_VERSION)).encode()
    ).hexdigest()[:24]


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
