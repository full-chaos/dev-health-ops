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
from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.llm.credentials import resolve_llm_org_settings_credentials
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import Setting, SettingCategory
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(User, Organization, OrgLicense, Setting)


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


async def _seed_org(session_maker, tier: str = "team") -> dict[str, str]:
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug=f"{tier}-org", name="Test Org", tier=tier),
                OrgLicense(org_id=org_id, tier=tier),
                User(id=user_id, email="admin@example.com", is_active=True),
            ]
        )
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
                "base_url": "https://api.example.test/v1",
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data == {
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "sk-s…alue",
            "base_url": "https://api.example.test/v1",
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
    assert credentials.base_url == "https://api.example.test/v1"


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
