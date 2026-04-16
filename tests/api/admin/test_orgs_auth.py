"""Auth-dependency tests for admin orgs router (CHAOS security sprint)."""

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
from dev_health_ops.models.users import Organization

orgs_router_module = importlib.import_module("dev_health_ops.api.admin.routers.orgs")
admin_common = importlib.import_module("dev_health_ops.api.admin.routers.common")
admin_middleware = importlib.import_module("dev_health_ops.api.admin.middleware")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "orgs-auth.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn, tables=[Organization.__table__]
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(session_maker) -> str:
    org_id = uuid.uuid4()
    org = Organization(id=org_id, slug=f"o-{org_id.hex[:8]}", name="Acme")
    async with session_maker() as session:
        session.add(org)
        await session.commit()
    return str(org_id)


def _app(session_maker, current_user: AuthenticatedUser | None):
    app = FastAPI()
    app.include_router(orgs_router_module.router, prefix="/admin")

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[admin_common.get_session] = _session_override
    if current_user is not None:
        from dev_health_ops.api.auth.router import get_current_user

        app.dependency_overrides[get_current_user] = lambda: current_user
    return app


@pytest.mark.asyncio
async def test_get_org_by_id_rejects_anonymous(session_maker):
    """GET /admin/orgs/{id} must 401 when no bearer token is supplied."""
    org_id = await _seed_org(session_maker)
    app = _app(session_maker, current_user=None)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get(f"/admin/orgs/{org_id}")
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_get_org_by_id_rejects_non_superuser(session_maker):
    """GET /admin/orgs/{id} must 403 when caller is not a superuser."""
    org_id = await _seed_org(session_maker)
    member = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="m@example.com",
        org_id=str(uuid.uuid4()),
        role="member",
        is_superuser=False,
    )
    app = _app(session_maker, current_user=member)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get(f"/admin/orgs/{org_id}")
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_get_org_by_id_accepts_superuser(session_maker):
    """GET /admin/orgs/{id} must 200 for superuser."""
    org_id = await _seed_org(session_maker)
    su = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="su@example.com",
        org_id=str(uuid.uuid4()),
        role="owner",
        is_superuser=True,
    )
    app = _app(session_maker, current_user=su)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get(f"/admin/orgs/{org_id}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == org_id
