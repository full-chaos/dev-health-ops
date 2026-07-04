"""Sync run unit endpoint regressions."""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationSource,
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User, Organization, Integration, IntegrationSource, SyncRun, SyncRunUnit
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "sync-run-units.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_state(session_maker):
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="test-org", name="Test Org", tier="pro"),
                User(id=user_id, email="admin@example.com", is_active=True),
            ]
        )
        await session.commit()
    return {"org_id": str(org_id), "user_id": str(user_id)}


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)
    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state

    app.dependency_overrides.clear()


async def _seed_run_with_units(session_maker, org_id: str, unit_count: int) -> str:
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    run_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Integration(
                    id=integration_id,
                    org_id=org_id,
                    provider="github",
                    name="sync-test",
                    config={},
                    is_active=True,
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    provider="github",
                    source_type="repository",
                    external_id="owner/repo",
                    name="repo",
                    full_name="owner/repo",
                    metadata_={"owner": "owner"},
                    is_enabled=True,
                ),
                SyncRun(
                    id=run_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    triggered_by="test",
                    mode="incremental",
                    status="success",
                    total_units=unit_count,
                    completed_units=unit_count,
                    failed_units=0,
                ),
            ]
        )
        session.add_all(
            [
                SyncRunUnit(
                    org_id=org_id,
                    sync_run_id=run_id,
                    integration_id=integration_id,
                    source_id=source_id,
                    provider="github",
                    dataset_key="git",
                    cost_class="standard",
                    mode="incremental",
                    status="success",
                    attempts=1,
                )
                for _ in range(unit_count)
            ]
        )
        await session.commit()
    return str(run_id)


@pytest.mark.asyncio
async def test_get_sync_run_units_default_includes_every_unit(client, session_maker):
    ac, seeded_state = client
    run_id = await _seed_run_with_units(session_maker, seeded_state["org_id"], 201)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert data["unit_count"] == 201
    assert len(data["units"]) == 201


@pytest.mark.asyncio
async def test_get_sync_run_units_explicit_limit_still_slices_units(
    client, session_maker
):
    ac, seeded_state = client
    run_id = await _seed_run_with_units(session_maker, seeded_state["org_id"], 5)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units?limit=3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["unit_count"] == 5
    assert len(data["units"]) == 3
    assert data["by_status"]["success"] == 5
    assert data["by_dataset"]["git"]["success"] == 5
    assert data["by_cost_class"]["standard"] == 5
