from __future__ import annotations

import importlib
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.metrics.schemas import (
    LLMTokenSpendLegacyRecord,
    LLMTokenSpendRunRecord,
    LLMTokenSpendSummaryRecord,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
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
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "llm-settings-spend.db"
    async_url = f"sqlite+aiosqlite:///{db_path}"
    sync_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("POSTGRES_URI", sync_url)
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
    session_maker,
    *,
    tier: str = "team",
    flag_enabled: bool | None = True,
    override_enabled: bool | None = None,
) -> dict[str, str]:
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug=f"{tier}-org", name="Org", tier=tier),
                OrgLicense(org_id=org_id, tier=tier),
                User(id=user_id, email="admin@example.com", is_active=True),
            ]
        )
        if flag_enabled is not None:
            flag = FeatureFlag(
                key="byo_llm",
                name="BYO LLM",
                category="analytics",
                min_tier="team",
                is_enabled=flag_enabled,
            )
            session.add(flag)
            await session.flush()
            if override_enabled is not None:
                session.add(
                    OrgFeatureOverride(
                        org_id=org_id,
                        feature_id=flag.id,
                        is_enabled=override_enabled,
                    )
                )
        await session.commit()
    return {"org_id": str(org_id), "user_id": str(user_id)}


class FakeSpendSink:
    def __init__(self, summary: LLMTokenSpendSummaryRecord) -> None:
        self.summary = summary
        self.org_ids: list[str] = []
        self.closed = False

    def read_llm_token_spend(
        self,
        *,
        org_id: str,
        limit: int = 20,
        since: datetime | None = None,
    ) -> LLMTokenSpendSummaryRecord:
        self.org_ids.append(org_id)
        return self.summary

    def close(self) -> None:
        self.closed = True


def _make_app(
    session_maker,
    state: dict[str, str],
    sink: FakeSpendSink,
) -> FastAPI:
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

    def _sink_override():
        yield sink

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    app.dependency_overrides[settings_router_module.get_metrics_sink] = _sink_override
    return app


def _summary() -> LLMTokenSpendSummaryRecord:
    now = datetime(2026, 1, 2, 3, tzinfo=timezone.utc)
    since = datetime(2025, 12, 3, 3, tzinfo=timezone.utc)
    return LLMTokenSpendSummaryRecord(
        since=since,
        limit=20,
        runs=[
            LLMTokenSpendRunRecord(
                run_id="run-org-a",
                provider="openai",
                model="gpt-5-mini",
                calls=3,
                input_tokens=30,
                output_tokens=15,
                computed_at=now,
                failures_by_class={"llm_task_failed": 1},
            )
        ],
        legacy=[
            LLMTokenSpendLegacyRecord(
                provider="openai",
                model="gpt-legacy",
                calls=1,
                input_tokens=4,
                output_tokens=2,
                computed_at=now,
            )
        ],
    )


@pytest.mark.asyncio
async def test_admin_llm_settings_spend_returns_org_scoped_summary(session_maker):
    state = await _seed_org(session_maker, tier="team", flag_enabled=True)
    sink = FakeSpendSink(_summary())
    app = _make_app(session_maker, state, sink)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/admin/llm-settings/spend")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert sink.org_ids == [state["org_id"]]
    assert body["runs"] == [
        {
            "run_id": "run-org-a",
            "provider": "openai",
            "model": "gpt-5-mini",
            "calls": 3,
            "input_tokens": 30,
            "output_tokens": 15,
            "computed_at": "2026-01-02T03:00:00Z",
            "failures_by_class": {"llm_task_failed": 1},
        }
    ]
    assert body["legacy"][0]["marker"] == "legacy_empty_run_id"
    assert body["legacy"][0]["run_id"] == ""


@pytest.mark.asyncio
async def test_admin_llm_settings_spend_returns_empty_summary(session_maker):
    state = await _seed_org(session_maker, tier="team", flag_enabled=True)
    empty = LLMTokenSpendSummaryRecord(
        since=datetime(2025, 12, 3, 3, tzinfo=timezone.utc),
        limit=20,
        runs=[],
        legacy=[],
    )
    sink = FakeSpendSink(empty)
    app = _make_app(session_maker, state, sink)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/admin/llm-settings/spend")

    assert resp.status_code == 200, resp.text
    assert resp.json()["runs"] == []
    assert resp.json()["legacy"] == []


@pytest.mark.asyncio
async def test_admin_llm_settings_spend_requires_byo_llm_gate(session_maker):
    state = await _seed_org(session_maker, tier="team", flag_enabled=False)
    sink = FakeSpendSink(_summary())
    app = _make_app(session_maker, state, sink)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/admin/llm-settings/spend")

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "feature_not_enabled"
    assert sink.org_ids == []


@pytest.mark.asyncio
async def test_admin_llm_settings_spend_requires_team_tier(session_maker):
    state = await _seed_org(session_maker, tier="community", flag_enabled=True)
    sink = FakeSpendSink(_summary())
    app = _make_app(session_maker, state, sink)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/admin/llm-settings/spend")

    assert resp.status_code == 402
    assert resp.json()["detail"]["error"] == "feature_not_licensed"
    assert sink.org_ids == []
