from __future__ import annotations

import importlib
import os
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.llm import credentials as llm_credentials
from dev_health_ops.llm.credentials import (
    BYO_LLM_BASE_URL_FALLBACK_ALERT_THRESHOLD,
    BYO_LLM_BASE_URL_FALLBACK_ALERT_WINDOW,
    BYO_LLM_BASE_URL_FALLBACK_DEDUPE_WINDOW,
    evaluate_org_llm_status,
    resolve_llm_org_settings_credentials,
)
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.settings import Setting, SettingCategory
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    FeatureFlag,
    OrgFeatureOverride,
    Setting,
    AuditLog,
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


def _make_app(session_maker, state: dict[str, str]) -> FastAPI:
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=state["user_id"],
        email="admin@example.com",
        org_id=state["org_id"],
        role="owner",
        is_superuser=False,
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
    }
    assert cross_org.status_code == 200
    assert cross_org.json() == {
        "configured": True,
        "active": False,
        "degraded": True,
        "reason_code": "invalid_base_url",
        "last_fallback_at": None,
    }


def test_byo_llm_status_reason_codes_match_documented_contract():
    doc = Path(__file__).parents[3] / "docs" / "llm" / "byo-llm-credentials.md"
    text = doc.read_text(encoding="utf-8")
    for reason_code in (
        "not_configured",
        "unknown_provider",
        "missing_credentials",
        "invalid_base_url",
        "active",
    ):
        assert f"`{reason_code}`" in text


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
async def test_generic_settings_routes_reject_llm_category(session_maker):
    # Review finding: the generic settings routes must NOT be a back door for
    # category='llm' (would bypass the BYO-LLM tier gate + forced encryption).
    state = await _seed_org(session_maker, "team")
    app = _make_app(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        put_resp = await ac.put(
            "/api/v1/admin/settings/llm/api_key",
            json={"value": "sk-leak", "encrypt": False},
        )
        assert put_resp.status_code == 403
        assert put_resp.json()["detail"]["error"] == "use_llm_settings_endpoint"
        post_resp = await ac.post(
            "/api/v1/admin/settings",
            json={"key": "api_key", "value": "sk-leak", "category": "llm"},
        )
        assert post_resp.status_code == 403
        get_resp = await ac.get("/api/v1/admin/settings/llm/api_key")
        assert get_resp.status_code == 403
        del_resp = await ac.delete("/api/v1/admin/settings/llm/api_key")
        assert del_resp.status_code == 403
        list_resp = await ac.get("/api/v1/admin/settings/llm")
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
