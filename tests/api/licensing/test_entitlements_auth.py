"""Tests for CHAOS-1205: entitlements endpoint authentication.

Verifies:
- 401 when unauthenticated (no token)
- 403 when authenticated but not a member of the requested org
- 200 when authenticated org member
"""
from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization


# Use importlib to get the actual module (not the re-exported router object)
_licensing_router_module = importlib.import_module("dev_health_ops.api.licensing.router")
licensing_router = _licensing_router_module.router


_TABLES = [
    Organization.__table__,
    OrgLicense.__table__,
    FeatureFlag.__table__,
    OrgFeatureOverride.__table__,
]


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "entitlements-auth.db"
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
async def seeded_org(session_maker):
    org_id = uuid.uuid4()
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="team")

    async with session_maker() as session:
        session.add(org)
        await session.commit()

    return str(org_id)


def _make_postgres_patcher(session_maker):
    """Return a context-manager replacement for get_postgres_session."""

    @asynccontextmanager
    async def _fake_session():
        async with session_maker() as session:
            yield session

    return _fake_session


@pytest.mark.asyncio
async def test_entitlements_unauthenticated_returns_401(session_maker, seeded_org, monkeypatch):
    """No Authorization header → 401."""
    app = FastAPI()
    app.include_router(licensing_router)

    monkeypatch.setattr(
        _licensing_router_module,
        "get_postgres_session",
        _make_postgres_patcher(session_maker),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/api/v1/licensing/entitlements/{seeded_org}")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_entitlements_wrong_org_returns_403(session_maker, seeded_org, monkeypatch):
    """Authenticated user whose org_id != requested org_id → 403."""
    other_org_id = str(uuid.uuid4())
    user = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="other@example.com",
        org_id=other_org_id,  # different from seeded_org
        role="member",
        is_superuser=False,
    )

    app = FastAPI()
    app.include_router(licensing_router)
    app.dependency_overrides[get_current_user] = lambda: user

    monkeypatch.setattr(
        _licensing_router_module,
        "get_postgres_session",
        _make_postgres_patcher(session_maker),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/api/v1/licensing/entitlements/{seeded_org}")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_entitlements_org_member_returns_200(session_maker, seeded_org, monkeypatch):
    """Authenticated user whose org_id == requested org_id → 200."""
    user = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="member@example.com",
        org_id=seeded_org,  # matches the org we're querying
        role="member",
        is_superuser=False,
    )

    app = FastAPI()
    app.include_router(licensing_router)
    app.dependency_overrides[get_current_user] = lambda: user

    monkeypatch.setattr(
        _licensing_router_module,
        "get_postgres_session",
        _make_postgres_patcher(session_maker),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/api/v1/licensing/entitlements/{seeded_org}")

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == seeded_org
    assert data["tier"] == "team"
