from __future__ import annotations

import importlib
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    _impersonation_ctx,
    set_impersonation_context,
)
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.llm import credentials as llm_credentials
from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentProviderCapabilities,
    AgentToolRequest,
    AgentUsage,
    StreamingMode,
    StructuredOutputMode,
    ToolDecisionMode,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.policy import AgentProviderSource
from dev_health_ops.llm.agent.readiness import READINESS_ECHO_TOOL_ID
from dev_health_ops.llm.credentials import (
    BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD,
    BYO_LLM_BASE_URL_FALLBACK_ALERT_WINDOW,
    BYO_LLM_BASE_URL_FALLBACK_DEDUPE_WINDOW,
    evaluate_org_llm_status,
    resolve_llm_org_settings_credentials,
)
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.dev_persistence import DevConversation, DevRun
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.llm_budget import BYOLLMBudgetReservation
from dev_health_ops.models.settings import Setting, SettingCategory
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
settings_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.settings"
)

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    FeatureFlag,
    OrgFeatureOverride,
    Setting,
    AuditLog,
    BYOLLMBudgetReservation,
    DevConversation,
    DevRun,
)


class FakeReadinessProvider:
    """Echoes the readiness nonce successfully -- for the READY path."""

    def __init__(self) -> None:
        self.calls = 0
        self.closed = False

    @property
    def capabilities(self) -> AgentProviderCapabilities:
        return AgentProviderCapabilities(
            structured_output=StructuredOutputMode.JSON_SCHEMA,
            tool_decisions=ToolDecisionMode.NATIVE,
            streaming=StreamingMode.BUFFERED,
            supports_cancellation=True,
            context_window_tokens=16_384,
            max_output_tokens=1_024,
            readiness_version=READINESS_VERSION,
            disclosure_key="openai-compatible",
        )

    async def decide(self, *_args: Any, **_kwargs: Any) -> AgentDecisionResult:
        # CHAOS-3285: BYO preflight now runs the OLD binary transport-echo
        # probe (calls 1-2) AND the NEW production-sized legacy_agent role
        # probe (calls 3-6: two independent 2-call chains, committed-subject
        # then uncommitted-subject -- CHAOS-3285 round 4, Codex HIGH)
        # against the same resolved provider, 6 calls total. Odd calls
        # request a tool; even calls answer. Call 1 must echo
        # READINESS_ECHO_TOOL_ID exactly (readiness.py's own strict match);
        # every later tool_request round names a real registered tool
        # instead, since certify_legacy_agent validates its synthetic tool
        # result against the real DevToolResult/ToolID contract. This fake
        # is round-aware by parity (odd/even), not call-count-specific, so
        # it transparently supports however many role-probe rounds the real
        # probe currently makes -- but see the pinned provider.calls == 6
        # assertion in the success test below, which fails loudly if a
        # chain silently vanishes.
        self.calls += 1
        if self.calls % 2 == 1:
            tool_id = READINESS_ECHO_TOOL_ID if self.calls == 1 else "query_metric.v1"
            arguments = (
                {"nonce": "ready-v1"} if tool_id == READINESS_ECHO_TOOL_ID else {}
            )
            return AgentDecisionResult(
                decision=AgentToolRequest(tool_id, arguments, f"call-{self.calls}"),
                usage=AgentUsage(input_tokens=3, output_tokens=2),
                latency_ms=1,
                provider_fingerprint="provider",
                model_fingerprint="model",
            )
        value = (
            {"nonce": "ready-v1"}
            if self.calls == 2
            else {"status": "complete", "direct_summary": "Stub role probe answer."}
        )
        return AgentDecisionResult(
            decision=AgentFinalAnswer(value=value),
            usage=AgentUsage(input_tokens=4, output_tokens=1),
            latency_ms=1,
            provider_fingerprint="provider",
            model_fingerprint="model",
        )

    async def aclose(self) -> None:
        self.closed = True


class FailingReadinessProvider(FakeReadinessProvider):
    """Always fails the readiness exchange -- for the FAILED path."""

    async def decide(self, *_args: Any, **_kwargs: Any) -> AgentDecisionResult:
        raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)


async def _real_byo_fingerprint(session_maker, org_id: str) -> str:
    """Compute the ACTUAL fingerprint production code would derive for this
    org's currently-saved BYO settings, so tests that fake the certifying
    provider still satisfy the currency check in
    settings_router_module._llm_settings_status_response (readiness=ready/
    failed requires the stored fingerprint to match the live BYO config)."""

    async with session_maker() as session:
        svc = SettingsService(session, org_id)
        candidate = await production_runtime._byo_candidate(
            svc, readiness=None, certification=True
        )
        assert candidate is not None
        return production_runtime._readiness_fingerprint(candidate)


async def _dev_run_count(session_maker, org_id: str) -> int:
    async with session_maker() as session:
        return int(
            (
                await session.execute(
                    select(func.count(DevRun.id)).where(
                        DevRun.org_id == uuid.UUID(org_id)
                    )
                )
            ).scalar_one()
        )


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "llm-settings.db"
    async_url = f"sqlite+aiosqlite:///{db_path}"
    sync_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("POSTGRES_URI", sync_url)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    engine = create_async_engine(async_url)

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(
    session_maker, tier: str = "team", *, flag_enabled: bool | None = None
) -> dict[str, str]:
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(
                    id=org_id,
                    slug=f"{tier}-{org_id.hex[:8]}",
                    name="Test Org",
                    tier=tier,
                ),
                OrgLicense(org_id=org_id, tier=tier),
                User(
                    id=user_id,
                    email=f"admin-{user_id.hex[:8]}@example.com",
                    is_active=True,
                ),
            ]
        )
        if flag_enabled is not None:
            flag_result = await session.execute(
                select(FeatureFlag).where(FeatureFlag.key == "byo_llm")
            )
            flag = flag_result.scalar_one_or_none()
            if flag is None:
                session.add(
                    FeatureFlag(
                        key="byo_llm",
                        name="BYO LLM",
                        category="analytics",
                        min_tier="team",
                        is_enabled=flag_enabled,
                    )
                )
            else:
                flag.is_enabled = flag_enabled
        await session.commit()
    return {"org_id": str(org_id), "user_id": str(user_id)}


def _make_app(
    session_maker,
    state: dict[str, str],
    *,
    role: str = "owner",
    impersonated_by: str | None = None,
) -> FastAPI:
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=state["user_id"],
        email="admin@example.com",
        org_id=state["org_id"],
        role=role,
        is_superuser=False,
        impersonated_by=impersonated_by,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    return app


async def _set_llm_settings(
    session_maker,
    org_id: str,
    *,
    provider: str | None = "openai",
    api_key: str | None = "sk-org",
    base_url: str | None = None,
) -> None:
    async with session_maker() as session:
        svc = SettingsService(session, org_id)
        if provider is not None:
            await svc.set("provider", provider, SettingCategory.LLM.value)
        if api_key is not None:
            await svc.set("api_key", api_key, SettingCategory.LLM.value, encrypt=True)
        if base_url is not None:
            await svc.set("base_url", base_url, SettingCategory.LLM.value)
        await session.commit()


async def _audit_rows(session_maker, org_id: str) -> list[AuditLog]:
    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog)
            .where(AuditLog.org_id == uuid.UUID(org_id))
            .order_by(AuditLog.created_at.asc())
        )
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_evaluate_org_llm_status_is_side_effect_free_for_invalid_base_url(
    session_maker,
):
    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://127.0.0.1/v1",
    )

    async with session_maker() as session:
        svc = SettingsService(session, state["org_id"])
        status = await evaluate_org_llm_status(state["org_id"], svc)

    assert status.configured is True
    assert status.active is False
    assert status.reason_code == "invalid_base_url"
    assert await _audit_rows(session_maker, state["org_id"]) == []


@pytest.mark.asyncio
async def test_base_url_fallback_audit_dedupes_within_window_and_records_each_metric(
    session_maker, monkeypatch: pytest.MonkeyPatch
):
    state = await _seed_org(session_maker, "team")
    fallbacks: list[dict[str, str]] = []
    alerts: list[dict[str, str]] = []

    monkeypatch.setattr(
        llm_credentials,
        "record_byo_llm_base_url_fallback",
        lambda **kwargs: fallbacks.append(kwargs),
    )
    monkeypatch.setattr(
        llm_credentials,
        "record_byo_llm_base_url_fallback_alert",
        lambda **kwargs: alerts.append(kwargs),
    )

    for _ in range(2):
        llm_credentials._audit_org_byo_base_url_fallback(
            org_id=state["org_id"],
            provider_name="openai",
            base_url="https://127.0.0.1/v1",
            reason="LLM base_url host resolves to a non-public address",
        )

    rows = await _audit_rows(session_maker, state["org_id"])
    assert len(rows) == 1
    changes = rows[0].changes
    assert changes is not None
    assert changes["reason_code"] == "invalid_base_url"
    assert changes["dedupe_window_seconds"] == int(
        BYO_LLM_BASE_URL_FALLBACK_DEDUPE_WINDOW.total_seconds()
    )
    assert [event["audit_inserted"] for event in fallbacks] == ["true", "false"]
    assert alerts == [
        {
            "provider": "openai",
            "reason_code": "invalid_base_url",
            "threshold": str(BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD),
            "window_seconds": str(
                int(BYO_LLM_BASE_URL_FALLBACK_ALERT_WINDOW.total_seconds())
            ),
        }
    ]


@pytest.mark.asyncio
async def test_base_url_fallback_alerts_when_org_exceeds_threshold(
    session_maker, monkeypatch: pytest.MonkeyPatch
):
    state = await _seed_org(session_maker, "team")
    alerts: list[dict[str, str]] = []
    monkeypatch.setattr(
        llm_credentials,
        "record_byo_llm_base_url_fallback",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        llm_credentials,
        "record_byo_llm_base_url_fallback_alert",
        lambda **kwargs: alerts.append(kwargs),
    )

    for index in range(BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD):
        llm_credentials._audit_org_byo_base_url_fallback(
            org_id=state["org_id"],
            provider_name="openai",
            base_url=f"https://127.0.0.{index + 1}/v1",
            reason="LLM base_url host resolves to a non-public address",
        )

    rows = await _audit_rows(session_maker, state["org_id"])
    assert len(rows) == BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD
    assert alerts[-1] == {
        "provider": "openai",
        "reason_code": "invalid_base_url",
        "threshold": str(BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD),
        "window_seconds": str(
            int(BYO_LLM_BASE_URL_FALLBACK_ALERT_WINDOW.total_seconds())
        ),
    }


@pytest.mark.asyncio
async def test_admin_llm_settings_status_reports_unconfigured_valid_and_invalid_states(
    session_maker,
):
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        unconfigured = await ac.get("/api/v1/admin/llm-settings/status")
        assert unconfigured.status_code == 200
        assert unconfigured.json() == {
            "configured": False,
            "active": False,
            "degraded": False,
            "reason_code": "not_configured",
            "last_fallback_at": None,
            "readiness": "never_checked",
            "binary_transport_readiness": "never_checked",
            "readiness_checked_at": None,
            "readiness_safe_failure_reason": None,
        }

    await _set_llm_settings(session_maker, state["org_id"], base_url=None)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        active = await ac.get("/api/v1/admin/llm-settings/status")
        assert active.status_code == 200
        assert active.json() == {
            "configured": True,
            "active": True,
            "degraded": False,
            "reason_code": "active",
            "last_fallback_at": None,
            "readiness": "never_checked",
            "binary_transport_readiness": "never_checked",
            "readiness_checked_at": None,
            "readiness_safe_failure_reason": None,
        }

    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://127.0.0.1/v1",
    )
    llm_credentials._audit_org_byo_base_url_fallback(
        org_id=state["org_id"],
        provider_name="openai",
        base_url="https://127.0.0.1/v1",
        reason="LLM base_url host resolves to a non-public address",
    )
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        degraded = await ac.get("/api/v1/admin/llm-settings/status")
        assert degraded.status_code == 200
        body = degraded.json()
        assert body["configured"] is True
        assert body["active"] is False
        assert body["degraded"] is True
        assert body["reason_code"] == "invalid_base_url"
        assert body["last_fallback_at"] is not None


@pytest.mark.asyncio
async def test_admin_llm_settings_status_gate_enforces_flag_and_tier(session_maker):
    disabled = await _seed_org(session_maker, "team", flag_enabled=False)
    app = _make_app(session_maker, disabled)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        disabled_resp = await ac.get("/api/v1/admin/llm-settings/status")
    assert disabled_resp.status_code == 403

    community = await _seed_org(session_maker, "community", flag_enabled=True)
    app = _make_app(session_maker, community)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        community_resp = await ac.get("/api/v1/admin/llm-settings/status")
    assert community_resp.status_code == 402


@pytest.mark.asyncio
async def test_admin_llm_settings_status_ignores_stale_or_cross_org_fallback_rows(
    session_maker,
):
    stale_state = await _seed_org(session_maker, "team")
    other_state = await _seed_org(session_maker, "team")
    stale_url = "https://127.0.0.1/v1"
    await _set_llm_settings(
        session_maker,
        stale_state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url=stale_url,
    )
    llm_credentials._audit_org_byo_base_url_fallback(
        org_id=stale_state["org_id"],
        provider_name="openai",
        base_url=stale_url,
        reason="LLM base_url host resolves to a non-public address",
    )
    await _set_llm_settings(
        session_maker,
        stale_state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="",
    )
    await _set_llm_settings(
        session_maker,
        other_state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url=stale_url,
    )

    stale_app = _make_app(session_maker, stale_state)
    other_app = _make_app(session_maker, other_state)
    async with AsyncClient(
        transport=ASGITransport(app=stale_app), base_url="http://test"
    ) as ac:
        fixed = await ac.get("/api/v1/admin/llm-settings/status")
    async with AsyncClient(
        transport=ASGITransport(app=other_app), base_url="http://test"
    ) as ac:
        cross_org = await ac.get("/api/v1/admin/llm-settings/status")

    assert fixed.status_code == 200
    assert fixed.json() == {
        "configured": True,
        "active": True,
        "degraded": False,
        "reason_code": "active",
        "last_fallback_at": None,
        "readiness": "never_checked",
        "binary_transport_readiness": "never_checked",
        "readiness_checked_at": None,
        "readiness_safe_failure_reason": None,
    }
    assert cross_org.status_code == 200
    assert cross_org.json() == {
        "configured": True,
        "active": False,
        "degraded": True,
        "reason_code": "invalid_base_url",
        "last_fallback_at": None,
        "readiness": "never_checked",
        "binary_transport_readiness": "never_checked",
        "readiness_checked_at": None,
        "readiness_safe_failure_reason": None,
    }


@pytest.mark.asyncio
async def test_binary_ready_role_absent_reports_unavailable_not_ready(session_maker):
    """CHAOS-3285 round 2 (Codex HIGH): the BYO status endpoint must agree
    with live selection -- a binary-ready record with no legacy_agent role
    certification at all must never report "ready" here either."""

    from dev_health_ops.llm.agent.readiness import (
        AgentReadinessOutcome,
        AgentReadinessRecord,
        SettingsAgentReadinessStore,
    )

    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    fingerprint = await _real_byo_fingerprint(session_maker, state["org_id"])

    async with session_maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, state["org_id"])
        ).save(
            AgentReadinessRecord(
                fingerprint=fingerprint,
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        )
        # Deliberately NO role-certification row at all.
        await session.commit()

    app = _make_app(session_maker, state)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/api/v1/admin/llm-settings/status")

    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] != "ready"
    assert body["binary_transport_readiness"] == "ready"


# Removed: test_byo_llm_status_reason_codes_match_documented_contract.
# It asserted the BYO-LLM reason codes (`not_configured`, `unknown_provider`,
# `missing_credentials`, `invalid_base_url`, `active`) were documented in
# the legacy llm/byo-llm-credentials.md page. That page was deleted and no page in
# the live docs/ tree documents these reason codes at all (grep for `reason_code` across
# docs/ returns nothing), so there is no page to repoint the guard at. The reason codes
# themselves remain asserted against the live API in the status tests above.


@pytest.mark.asyncio
async def test_admin_llm_settings_encrypts_and_masks_api_key(session_maker):
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.put(
            "/api/v1/admin/llm-settings",
            json={
                "provider": "openai",
                "model": "gpt-test",
                "api_key": "sk-secret-value",
                "base_url": "https://api.openai.com/v1",
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data == {
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "sk-s…alue",
            "base_url": "https://api.openai.com/v1",
        }

        get_resp = await ac.get("/api/v1/admin/llm-settings")
        assert get_resp.status_code == 200
        assert get_resp.json()["api_key"] == "sk-s…alue"

    async with session_maker() as session:
        result = await session.execute(
            select(Setting).where(
                Setting.org_id == state["org_id"],
                Setting.category == SettingCategory.LLM.value,
                Setting.key == "api_key",
            )
        )
        setting = result.scalar_one()
        assert setting.is_encrypted is True
        assert setting.value != "sk-secret-value"
        assert decrypt_value(setting.value or "") == "sk-secret-value"

    credentials = resolve_llm_org_settings_credentials("openai", org_id=state["org_id"])
    assert credentials.api_key == "sk-secret-value"
    assert credentials.base_url == "https://api.openai.com/v1"


@pytest.mark.asyncio
async def test_admin_llm_settings_rejects_excessive_concurrency(session_maker):
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.put(
            "/api/v1/admin/llm-settings",
            json={"provider": "openai", "concurrency": 33},
        )

    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_admin_llm_budget_persists_separately_and_exposes_contract(
    session_maker, monkeypatch
):
    monkeypatch.setenv("BYO_LLM_MAX_BUDGET_MICRO_USD", "5000000")
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        updated = await ac.put(
            "/api/v1/admin/llm-settings",
            json={
                "provider": "openai",
                "model": "gpt-5-mini",
                "api_key": "sk-secret-value",
                "budget_limit_micro_usd": 2000000,
            },
        )
        budget = await ac.get("/api/v1/admin/llm-settings/budget")

    assert updated.status_code == 200, updated.text
    assert budget.status_code == 200, budget.text
    body = budget.json()
    assert body == {
        "used_micro_usd": 0,
        "limit_micro_usd": 2000000,
        "remaining_micro_usd": 2000000,
        "window": "calendar_month_utc",
        "reset_at": body["reset_at"],
        "enforcement_available": True,
        "reason": "available",
        "maximum_limit_micro_usd": 5000000,
        "pricing_version": "openai-public-2025-08-07.v1",
    }

    async with session_maker() as session:
        svc = SettingsService(session, state["org_id"])
        credentials = await svc.list_by_category(SettingCategory.LLM.value)
        monetary = await svc.list_by_category("llm_budget")
    assert {row["key"] for row in credentials}.isdisjoint(
        {row["key"] for row in monetary}
    )
    assert monetary[0]["value"] == "2000000"


@pytest.mark.asyncio
async def test_admin_llm_budget_rejects_above_operator_maximum(
    session_maker, monkeypatch
):
    monkeypatch.setenv("BYO_LLM_MAX_BUDGET_MICRO_USD", "1000")
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.put(
            "/api/v1/admin/llm-settings",
            json={
                "provider": "openai",
                "model": "gpt-5-mini",
                "api_key": "sk-secret-value",
                "budget_limit_micro_usd": 1001,
            },
        )

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "budget_limit_exceeds_maximum"


@pytest.mark.asyncio
async def test_admin_llm_budget_rejects_above_licensed_maximum(
    session_maker, monkeypatch
):
    monkeypatch.setenv("BYO_LLM_MAX_BUDGET_MICRO_USD", "5000")
    state = await _seed_org(session_maker, "team")
    async with session_maker() as session:
        license_result = await session.execute(
            select(OrgLicense).where(OrgLicense.org_id == uuid.UUID(state["org_id"]))
        )
        license_result.scalar_one().limits_override = {"byo_llm_budget_micro_usd": 750}
        await session.commit()
    app = _make_app(session_maker, state)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.put(
            "/api/v1/admin/llm-settings",
            json={
                "provider": "openai",
                "model": "gpt-5-mini",
                "api_key": "sk-secret-value",
                "budget_limit_micro_usd": 751,
            },
        )
        budget = await ac.get("/api/v1/admin/llm-settings/budget")

    assert response.status_code == 400
    assert budget.status_code == 200
    assert budget.json()["maximum_limit_micro_usd"] == 750


@pytest.mark.asyncio
async def test_llm_budget_rejects_non_admin(session_maker):
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state, role="member")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/api/v1/admin/llm-settings/budget")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_llm_budget_blocks_impersonated_admin_write(session_maker):
    state = await _seed_org(session_maker, "team")
    app = _make_app(
        session_maker,
        state,
        impersonated_by=str(uuid.uuid4()),
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.put(
            "/api/v1/admin/llm-settings",
            json={"provider": "openai", "budget_limit_micro_usd": 1000},
        )

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "impersonated_write_forbidden"


@pytest.mark.asyncio
async def test_admin_llm_settings_requires_team_or_enterprise(session_maker):
    state = await _seed_org(session_maker, "community")
    app = _make_app(session_maker, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.put(
            "/api/v1/admin/llm-settings",
            json={"provider": "openai", "api_key": "sk-secret"},
        )

    assert resp.status_code == 402
    assert resp.json()["detail"]["required_tier"] == "team"


@pytest.mark.asyncio
@pytest.mark.parametrize("category", ["llm", "llm_budget"])
async def test_generic_settings_routes_reject_llm_categories(
    session_maker, category: str
):
    # Review finding: the generic settings routes must NOT be a back door for
    # category='llm' (would bypass the BYO-LLM tier gate + forced encryption).
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        put_resp = await ac.put(
            f"/api/v1/admin/settings/{category}/api_key",
            json={"value": "sk-leak", "encrypt": False},
        )
        assert put_resp.status_code == 403
        assert put_resp.json()["detail"]["error"] == "use_llm_settings_endpoint"
        post_resp = await ac.post(
            "/api/v1/admin/settings",
            json={"key": "api_key", "value": "sk-leak", "category": category},
        )
        assert post_resp.status_code == 403
        get_resp = await ac.get(f"/api/v1/admin/settings/{category}/api_key")
        assert get_resp.status_code == 403
        del_resp = await ac.delete(f"/api/v1/admin/settings/{category}/api_key")
        assert del_resp.status_code == 403
        list_resp = await ac.get(f"/api/v1/admin/settings/{category}")
        assert list_resp.status_code == 403


@pytest.mark.asyncio
async def test_generic_get_setting_masks_encrypted_value(session_maker):
    # Review finding: the generic single-setting GET must not return decrypted
    # secrets in plaintext.
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        post_resp = await ac.post(
            "/api/v1/admin/settings",
            json={
                "key": "token",
                "value": "ghp-secret-value",
                "category": "github",
                "encrypt": True,
            },
        )
        assert post_resp.status_code == 200, post_resp.text
        get_resp = await ac.get("/api/v1/admin/settings/github/token")
        assert get_resp.status_code == 200
        body = get_resp.json()
        assert body["value"] == "[ENCRYPTED]"
        assert "ghp-secret-value" not in body["value"]


def test_resolve_provider_name_uses_org_settings_in_auto(monkeypatch):
    # Review finding: default worker path uses llm_provider='auto'; an org that
    # only configured BYO settings must resolve its provider via org_id. Hermetic:
    # clear ALL env provider signals (env detection precedes org settings) and
    # mock the org-settings loader so this is order-independent in the full suite.
    # CHAOS-2550: org BYO must be COMPLETE (anthropic requires a key) to win;
    # an incomplete org config warns and falls back to the platform default.
    from dev_health_ops.llm import LLMAuthError
    from dev_health_ops.llm import credentials as creds
    from dev_health_ops.llm.providers import resolve_provider_name

    for var in (
        "LLM_PROVIDER",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GEMINI_API_KEY",
        "LLM_API_KEY",
        "DASHSCOPE_API_KEY",
        "QWEN_API_KEY",
        "LOCAL_LLM_BASE_URL",
        "OLLAMA_MODEL",
        "OLLAMA_BASE_URL",
        "LMSTUDIO_MODEL",
        "LMSTUDIO_BASE_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(
        creds,
        "_load_org_llm_settings",
        lambda org_id: (
            {"provider": "anthropic", "api_key": "sk-org-ant"}
            if org_id == "org-xyz"
            else {}
        ),
    )

    # auto + org_id resolves the org's configured provider
    assert resolve_provider_name("auto", org_id="org-xyz") == "anthropic"
    # auto without org context (and no env) fails loud rather than guessing
    with pytest.raises(LLMAuthError):
        resolve_provider_name("auto", org_id=None)


@pytest.mark.asyncio
async def test_llm_settings_readiness_requires_byo_configuration(session_maker):
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/api/v1/admin/llm-settings/readiness")

    assert response.status_code == 404
    assert response.json()["detail"] == (
        "No BYO LLM configuration is saved for this organization."
    )


@pytest.mark.asyncio
async def test_llm_settings_readiness_succeeds_independent_of_ask_dev_selection(
    session_maker, monkeypatch: pytest.MonkeyPatch
):
    """CHAOS-3265 acceptance criterion: BYO readiness is available based on
    BYO configuration being set up, not on BYO currently being selected or
    enabled for Ask Dev. Ask Dev is forced emergency-disabled here (so Ask
    Dev's own provider_source resolves to None) and the BYO check must still
    run -- and must not touch DevRun or change Ask Dev's selection."""

    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    async with session_maker() as session:
        await SettingsService(session, state["org_id"]).set(
            "ask_dev_emergency_disabled",
            "true",
            SettingCategory.ASK_DEV.value,
        )
        await session.commit()

    app = _make_app(session_maker, state)
    provider = FakeReadinessProvider()
    byo_fingerprint = await _real_byo_fingerprint(session_maker, state["org_id"])

    async def fake_resolve_byo(_session, *, org_id: str):
        assert org_id == state["org_id"]
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.BYO,
            family="openai",
            model="gpt-5-mini",
            provider_label="OpenAI compatible",
            model_label="gpt-5-mini",
            readiness_fingerprint=byo_fingerprint,
        )

    monkeypatch.setattr(
        settings_router_module, "resolve_byo_certification_provider", fake_resolve_byo
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        before = await ac.get("/api/v1/admin/ask-dev")
        assert before.status_code == 200
        before_source = before.json()["provider_source"]
        assert before_source is None  # forced emergency-disabled above

        before_dev_run_count = await _dev_run_count(session_maker, state["org_id"])

        response = await ac.post("/api/v1/admin/llm-settings/readiness")
        assert response.status_code == 200
        body = response.json()
        assert body["readiness"] == "ready"
        assert body["readiness_checked_at"] is not None
        assert body["readiness_safe_failure_reason"] is None
        assert provider.closed is True
        # CHAOS-3285 round 5 (Codex LOW): pin the full preflight call count
        # (2 binary transport-echo + 4 legacy_agent role-probe calls, two
        # independent 2-call chains) so a chain silently vanishing in a
        # future refactor fails loudly here rather than passing unnoticed.
        assert provider.calls == 6

        after_dev_run_count = await _dev_run_count(session_maker, state["org_id"])
        assert before_dev_run_count == 0
        assert after_dev_run_count == 0

        after = await ac.get("/api/v1/admin/ask-dev")
        assert after.status_code == 200
        assert after.json()["provider_source"] == before_source


@pytest.mark.asyncio
async def test_llm_settings_readiness_succeeds_and_logs_when_provider_close_fails(
    session_maker, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """A transport-close failure after a successful certification must never
    mask the readiness result the caller already committed (CHAOS-3265 /
    CodeQL empty-except finding) -- and must be logged, not silently
    swallowed. Deleting the try/except's log call (or reverting it to a bare
    `pass`) makes this test fail on the caplog assertion below."""

    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    app = _make_app(session_maker, state)
    byo_fingerprint = await _real_byo_fingerprint(session_maker, state["org_id"])

    class CloseFailsProvider(FakeReadinessProvider):
        async def aclose(self) -> None:
            self.closed = True
            raise RuntimeError("transport already shut down")

    provider = CloseFailsProvider()

    async def fake_resolve_byo(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.BYO,
            family="openai",
            model="gpt-5-mini",
            provider_label="OpenAI compatible",
            model_label="gpt-5-mini",
            readiness_fingerprint=byo_fingerprint,
        )

    monkeypatch.setattr(
        settings_router_module, "resolve_byo_certification_provider", fake_resolve_byo
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        with caplog.at_level(
            "WARNING", logger="dev_health_ops.api.admin.routers.settings"
        ):
            response = await ac.post("/api/v1/admin/llm-settings/readiness")

    assert response.status_code == 200
    assert response.json()["readiness"] == "ready"
    assert provider.closed is True
    assert any(
        "Failed to close BYO Ask Dev provider connection" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_llm_settings_readiness_persists_and_status_reflects_failure(
    session_maker, monkeypatch: pytest.MonkeyPatch
):
    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    app = _make_app(session_maker, state)
    provider = FailingReadinessProvider()
    byo_fingerprint = await _real_byo_fingerprint(session_maker, state["org_id"])

    async def fake_resolve_byo(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.BYO,
            family="openai",
            model="gpt-5-mini",
            provider_label="OpenAI compatible",
            model_label="gpt-5-mini",
            readiness_fingerprint=byo_fingerprint,
        )

    monkeypatch.setattr(
        settings_router_module, "resolve_byo_certification_provider", fake_resolve_byo
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/api/v1/admin/llm-settings/readiness")
        assert response.status_code == 200
        body = response.json()
        assert body["readiness"] == "failed"
        assert body["readiness_safe_failure_reason"] is not None

        status = await ac.get("/api/v1/admin/llm-settings/status")
        assert status.status_code == 200
        status_body = status.json()
        assert status_body["readiness"] == "failed"
        assert (
            status_body["readiness_safe_failure_reason"]
            == (body["readiness_safe_failure_reason"])
        )
        assert status_body["readiness_checked_at"] == body["readiness_checked_at"]


@pytest.mark.asyncio
async def test_llm_settings_readiness_blocks_impersonated_admin(session_maker):
    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    app = _make_app(session_maker, state, impersonated_by=str(uuid.uuid4()))

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/api/v1/admin/llm-settings/readiness")

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "impersonated_write_forbidden"


@pytest.mark.asyncio
async def test_llm_settings_readiness_blocks_live_impersonation_without_jwt_claim(
    session_maker, monkeypatch: pytest.MonkeyPatch
):
    """Codex regression test (CHAOS-3265): the guard must also catch the
    LIVE, per-request impersonation context set by ImpersonationMiddleware
    from the Valkey-cached session -- not just the static JWT
    ``impersonated_by`` claim. Proven with that claim left unset."""

    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    app = _make_app(session_maker, state)  # impersonated_by NOT set

    def _must_not_resolve(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(
            "must never resolve a certification provider while a live "
            "impersonation context is active"
        )

    monkeypatch.setattr(
        settings_router_module, "resolve_byo_certification_provider", _must_not_resolve
    )

    token = set_impersonation_context(
        target_user_id=str(uuid.uuid4()),
        target_org_id=state["org_id"],
        target_role="admin",
        real_user_id=state["user_id"],
    )
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            response = await ac.post("/api/v1/admin/llm-settings/readiness")
    finally:
        _impersonation_ctx.reset(token)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_llm_settings_readiness_can_flip_ask_dev_selection_to_byo(
    session_maker, monkeypatch: pytest.MonkeyPatch
):
    """Honest companion to the independence test above (codex review,
    CHAOS-3265): certifying BYO's OWN credentials here is what makes BYO
    usable/selectable for subsequent Ask Dev runs -- that is the intended
    purpose of a preflight check, not a bug. Prove it directly against the
    REAL (unmocked) production resolver, starting from a state where
    platform is genuinely winning, rather than asserting "no effect" only in
    a scenario (emergency-disabled) where nothing could move regardless."""

    from dev_health_ops.api.dev.production_runtime import (
        _byo_candidate,
        _readiness_fingerprint,
        resolve_production_provider,
    )
    from dev_health_ops.llm.agent.readiness import (
        PLATFORM_READINESS_SETTING_KEY,
        PLATFORM_SETTINGS_ORG_ID,
        AgentReadinessOutcome,
        AgentReadinessRecord,
        SettingsAgentReadinessStore,
    )
    from dev_health_ops.llm.agent.roles import (
        PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        AgentRole,
        RoleCertificationRecord,
        RoleCertificationState,
        SettingsRoleCertificationStore,
    )

    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "platform-key")
    monkeypatch.setenv("LLM_MODEL", "platform-model")
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    # Certify the PLATFORM candidate (as Platform Admin would) so platform
    # is genuinely usable and winning before BYO is ever certified.
    async with session_maker() as session:
        svc = SettingsService(session, state["org_id"])
        byo_candidate = await _byo_candidate(svc, readiness=None, certification=True)
        assert byo_candidate is not None
        byo_fingerprint = _readiness_fingerprint(byo_candidate)

        platform, _ = production_runtime._platform_candidate(
            readiness=None, certification=True
        )
        assert platform is not None
        platform_fingerprint = _readiness_fingerprint(platform)
        await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).save(
            AgentReadinessRecord(
                fingerprint=platform_fingerprint,
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        )
        # CHAOS-3285: live selection also requires a current, COMPATIBLE
        # legacy_agent role certification -- mirror what a real
        # POST /platform/ask-dev/readiness run now also writes.
        await SettingsRoleCertificationStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        ).save_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key=platform_fingerprint,
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.COMPATIBLE,
            )
        )
        await session.commit()

    async with session_maker() as session:
        before = await resolve_production_provider(session, org_id=state["org_id"])
        try:
            assert before.source is AgentProviderSource.PLATFORM
        finally:
            await before.provider.aclose()

    app = _make_app(session_maker, state)
    provider = FakeReadinessProvider()

    async def fake_resolve_byo(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.BYO,
            family="openai",
            model="gpt-5-mini",
            provider_label="OpenAI compatible",
            model_label="gpt-5-mini",
            readiness_fingerprint=byo_fingerprint,
        )

    monkeypatch.setattr(
        settings_router_module, "resolve_byo_certification_provider", fake_resolve_byo
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/api/v1/admin/llm-settings/readiness")
    assert response.status_code == 200
    assert response.json()["readiness"] == "ready"

    async with session_maker() as session:
        after = await resolve_production_provider(session, org_id=state["org_id"])
        try:
            assert after.source is AgentProviderSource.BYO
        finally:
            await after.provider.aclose()


@pytest.mark.asyncio
async def test_llm_settings_status_reports_stale_not_ready_on_version_bump(
    session_maker,
):
    """CHAOS-3254 (READINESS_VERSION v2->v3) exposed a gap: a stored record
    whose fingerprint matches the CURRENT BYO config but whose
    readiness_version is OUTDATED must never be reported as "ready" (nor
    "failed" -- it was never actually re-checked under the new
    requirements). It must report "stale" with a safe, accurate remediation
    that does not imply anything is broken."""

    from dev_health_ops.llm.agent.readiness import (
        AgentReadinessOutcome,
        AgentReadinessRecord,
        SettingsAgentReadinessStore,
    )

    state = await _seed_org(session_maker, "team")
    await _set_llm_settings(
        session_maker,
        state["org_id"],
        provider="openai",
        api_key="sk-org",
        base_url="https://api.openai.com/v1",
    )
    current_fingerprint = await _real_byo_fingerprint(session_maker, state["org_id"])

    async with session_maker() as session:
        svc = SettingsService(session, state["org_id"])
        await SettingsAgentReadinessStore(svc).save(
            AgentReadinessRecord(
                fingerprint=current_fingerprint,  # matches live BYO config
                readiness_version="stale-version-v2",  # does NOT match current
                checked_at="2026-01-01T00:00:00+00:00",
                outcome=AgentReadinessOutcome.READY,
            )
        )
        await session.commit()

    app = _make_app(session_maker, state)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/api/v1/admin/llm-settings/status")

    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] == "stale"
    assert body["readiness"] != "ready"
    assert body["readiness"] != "failed"
    assert body["readiness_safe_failure_reason"] is not None
    lowered = body["readiness_safe_failure_reason"].lower()
    assert "unavailable" not in lowered
    assert "endpoint" not in lowered
    assert "fail" not in lowered


@pytest.mark.asyncio
async def test_llm_settings_readiness_is_tier_and_flag_gated(session_maker):
    community = await _seed_org(session_maker, "community", flag_enabled=True)
    app = _make_app(session_maker, community)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/api/v1/admin/llm-settings/readiness")
    assert response.status_code == 402

    disabled = await _seed_org(session_maker, "team", flag_enabled=False)
    app = _make_app(session_maker, disabled)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/api/v1/admin/llm-settings/readiness")
    assert response.status_code == 403
