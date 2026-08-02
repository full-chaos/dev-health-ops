from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevScope, DevToolRequest, ToolID
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.prompts import PROMPT_VERSION
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.dev.tool_registry import (
    TOOL_CONTRACT_VERSION,
    ToolExecutionContext,
)
from dev_health_ops.llm.agent.budget_policy import BUDGET_POLICY_VERSION
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.policy import AgentProviderCandidate, AgentProviderSource
from dev_health_ops.llm.agent.readiness import PLATFORM_READINESS_SETTING_KEY
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
)
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
    base_url: str = "",
    model: str = "certified-model",
    provider: str = "openai",
    role: AgentRole = AgentRole.LEGACY_AGENT,
) -> str:
    # CHAOS-3285: mirrors production_runtime._readiness_fingerprint's
    # extended formula. Every fixture in this file that builds a "current"
    # stored AgentReadinessRecord by hand must fold the same inputs the
    # real function now folds, or it would exercise a fingerprint formula
    # that no longer matches production and every "certification is
    # current" assertion below would be testing nothing.
    return hashlib.sha256(
        "\0".join(
            (
                "platform",
                provider,
                model,
                base_url,
                READINESS_VERSION,
                PROMPT_VERSION,
                TOOL_CONTRACT_VERSION,
                BUDGET_POLICY_VERSION,
                role.value,
            )
        ).encode()
    ).hexdigest()[:24]


def _role_certification_setting(
    *,
    key_prefix: str = PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    role: AgentRole = AgentRole.LEGACY_AGENT,
    certification_key: str,
    state: str = "compatible",
) -> tuple[str, str]:
    """(settings_key, json_value) for one role's row under
    SettingsRoleCertificationStore's per-role key format (CHAOS-3285)."""

    return (
        f"{key_prefix}:{role.value}",
        json.dumps(
            {
                "version": "ask-dev-role-certification.v1",
                "record": {
                    "role": role.value,
                    "certification_key": certification_key,
                    "readiness_version": READINESS_VERSION,
                    "checked_at": "2026-07-29T12:00:00+00:00",
                    "state": state,
                    "safe_error_code": None,
                },
            }
        ),
    )


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


def test_readiness_fingerprint_changes_when_role_changes() -> None:
    """CHAOS-3285: certification is now per-role -- a fingerprint computed
    for one role must never collide with another role's fingerprint for the
    otherwise-identical candidate, or a legacy_agent certification could be
    misread as covering intent_classification/answer_frame_narrative too."""

    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    candidate = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )

    fingerprints = {
        role: production_runtime._readiness_fingerprint(candidate, role=role)
        for role in AgentRole
    }
    assert len(set(fingerprints.values())) == len(AgentRole)
    # The default (no explicit role) must be legacy_agent -- production's
    # existing single-role selection path calls _readiness_fingerprint
    # without a role argument, and it is exactly the legacy_agent shape
    # (full tool registry, full DevAnswer grammar) that path exercises.
    assert (
        production_runtime._readiness_fingerprint(candidate)
        == fingerprints[AgentRole.LEGACY_AGENT]
    )


def test_readiness_fingerprint_invalidates_pre_chaos_3285_stored_records() -> None:
    """CHAOS-3285 migration semantics: a fingerprint computed under the old
    (pre-PR3) formula -- which folded only source/provider/model/base_url/
    READINESS_VERSION -- must never equal the new formula's output. This is
    the mechanism that makes every previously stored AgentReadinessRecord
    read as stale rather than silently still-current after this change
    (see the docstring on _readiness_fingerprint)."""

    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    candidate = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )
    pre_change_fingerprint = hashlib.sha256(
        "\0".join(
            (
                candidate.source.value,
                candidate.provider,
                candidate.model,
                candidate.credentials.base_url,
                READINESS_VERSION,
            )
        ).encode()
    ).hexdigest()[:24]

    assert pre_change_fingerprint != production_runtime._readiness_fingerprint(
        candidate
    )


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
    role_key, role_value = _role_certification_setting(certification_key=_fingerprint())
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": _fingerprint(),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
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

    FakeSettingsService.values[PLATFORM_READINESS_SETTING_KEY] = json.dumps(
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
async def test_echo_only_certification_does_not_restore_live_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 (Codex HIGH): the old binary AgentReadinessRecord being
    "ready" must NOT be sufficient for live selection on its own. Before the
    role-gate fix, an operator could re-certify through a route that only
    ever runs the old 512-token echo probe (or the new role probe simply
    never having run at all), the binary store would read current, and this
    candidate would become selectable for real traffic having never
    demonstrated it can handle the production request shape. This is the RED
    half: binary readiness alone, with NO legacy_agent role certification on
    record, must fail closed."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": _fingerprint(),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        # Deliberately NO role-certification row at all.
    }

    session = cast(Any, object())
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_incompatible_legacy_agent_role_does_not_restore_live_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same gate, with a role record present but INCOMPATIBLE (e.g. the
    production-sized probe reproduced output exhaustion) rather than absent
    -- an INCOMPATIBLE verdict must never be silently treated as good
    enough for live selection either."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    role_key, role_value = _role_certification_setting(
        certification_key=_fingerprint(), state="incompatible"
    )
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": _fingerprint(),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }

    session = cast(Any, object())
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
    local_fingerprint = _fingerprint(
        provider="local",
        model="google/gemma-4-e4b",
        base_url="http://host.docker.internal:1234/v1",
    )
    role_key, role_value = _role_certification_setting(
        certification_key=local_fingerprint
    )
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": local_fingerprint,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
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
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
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


async def _build_runtime_for_resolve_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    async def resolve_provider(_session, *, org_id: str):
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
    # Constructed at runtime, never a literal secret-shaped string in source.
    monkeypatch.setenv("JWT_SECRET_KEY", secrets.token_hex(32))
    return await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id="org_fullchaos",
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )


@pytest.mark.asyncio
async def test_resolve_scope_with_a_query_searches_the_authorized_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3256: a named-entity query must not re-resolve the caller's scope."""

    async def fake_query_dicts(_client, sql, params):
        if "FROM projects FINAL" in sql:
            assert params["org_id"] == "org_fullchaos"
            assert params["query"] == "ask dev"
            return [
                {
                    "canonical_id": "project-ask-dev",
                    "label": "Ask Dev",
                    "repository_id": None,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    runtime = await _build_runtime_for_resolve_scope(monkeypatch)
    org_scope = DevScope.model_validate(
        {
            **positive_fixtures()["dev_scope.v1"],
            "organization_id": "org_fullchaos",
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "surface_context": None,
        }
    )

    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.RESOLVE_SCOPE,
            scope=org_scope,
            query="ask dev",
            limit=25,
        ),
        ToolExecutionContext(
            org_id="org_fullchaos",
            user_id="user_01",
            permission_fingerprint="permissions_01",
            authorized_scope=org_scope,
            cancellation=asyncio.Event(),
            remaining_seconds=5,
        ),
    )

    resolution = execution.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome.value == "exact"
    assert resolution.resolved_scope is not None
    assert resolution.resolved_scope.direct_scope.value == "project"
    assert resolution.resolved_scope.entity_refs[0].entity_id == "project-ask-dev"
    await runtime.aclose()


@pytest.mark.asyncio
async def test_resolve_scope_without_a_query_keeps_resolving_the_current_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty/omitted query keeps the pre-existing re-authorization behavior."""

    async def fake_query_dicts(_client, sql, params):
        del sql, params
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    runtime = await _build_runtime_for_resolve_scope(monkeypatch)
    org_scope = DevScope.model_validate(
        {
            **positive_fixtures()["dev_scope.v1"],
            "organization_id": "org_fullchaos",
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "surface_context": None,
        }
    )

    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.RESOLVE_SCOPE,
            scope=org_scope,
            limit=25,
        ),
        ToolExecutionContext(
            org_id="org_fullchaos",
            user_id="user_01",
            permission_fingerprint="permissions_01",
            authorized_scope=org_scope,
            cancellation=asyncio.Event(),
            remaining_seconds=5,
        ),
    )

    resolution = execution.result.scope_resolution
    assert resolution is not None
    # No connected repos in this fake catalog -> explicit insufficient
    # evidence, never a fabricated exact organization scope (CHAOS-3255).
    assert resolution.outcome.value == "unresolved"
    assert resolution.resolved_scope is None
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
