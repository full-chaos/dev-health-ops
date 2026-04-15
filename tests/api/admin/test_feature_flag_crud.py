"""Tests for CHAOS-1208: admin PATCH endpoint for feature flag properties.

Verifies:
- 200 when superuser patches a flag (full and partial updates)
- 403 when non-superuser attempts to patch
- Individual field updates (only is_enabled, only is_beta, only is_deprecated)
"""
from __future__ import annotations

import importlib
import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag


_TABLES = [FeatureFlag.__table__]

# Import the actual module (not the re-exported router object)
_features_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.features"
)
_features_router = _features_router_module.router

_common_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.common"
)
_get_session = _common_module.get_session

_middleware_module = importlib.import_module("dev_health_ops.api.admin.middleware")


def _build_user(*, superuser: bool) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(uuid.uuid4()),
        role="owner",
        is_superuser=superuser,
    )


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "feature-flag-crud.db"
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
async def seeded_flag(session_maker):
    flag = FeatureFlag(
        key="test_feature",
        name="Test Feature",
        description="A test feature flag",
        is_enabled=True,
        is_beta=False,
        is_deprecated=False,
    )
    async with session_maker() as session:
        session.add(flag)
        await session.commit()
        flag_id = str(flag.id)
    return flag_id


def _make_app(session_maker, override_user):
    app = FastAPI()
    app.include_router(_features_router, prefix="/api/v1/admin")

    async def _session_override():
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_current_user] = lambda: override_user
    app.dependency_overrides[_get_session] = _session_override
    return app


@pytest.mark.asyncio
async def test_superuser_patch_flag_returns_200(session_maker, seeded_flag):
    """Superuser can patch all three fields at once."""
    app = _make_app(session_maker, _build_user(superuser=True))
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            f"/api/v1/admin/feature-flags/{seeded_flag}",
            json={"is_enabled": False, "is_beta": True, "is_deprecated": True},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["is_enabled"] is False
    assert data["is_beta"] is True
    assert data["is_deprecated"] is True
    assert data["id"] == seeded_flag
    assert data["key"] == "test_feature"


@pytest.mark.asyncio
async def test_non_superuser_patch_flag_returns_403(session_maker, seeded_flag):
    """Non-superuser (org admin) cannot patch feature flags."""
    app = _make_app(session_maker, _build_user(superuser=False))
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            f"/api/v1/admin/feature-flags/{seeded_flag}",
            json={"is_enabled": False},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_only_is_enabled(session_maker, seeded_flag):
    """Patching only is_enabled leaves is_beta and is_deprecated unchanged."""
    app = _make_app(session_maker, _build_user(superuser=True))
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            f"/api/v1/admin/feature-flags/{seeded_flag}",
            json={"is_enabled": False},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["is_enabled"] is False
    assert data["is_beta"] is False      # unchanged from seed
    assert data["is_deprecated"] is False  # unchanged from seed


@pytest.mark.asyncio
async def test_patch_only_is_beta(session_maker, seeded_flag):
    """Patching only is_beta leaves is_enabled and is_deprecated unchanged."""
    app = _make_app(session_maker, _build_user(superuser=True))
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            f"/api/v1/admin/feature-flags/{seeded_flag}",
            json={"is_beta": True},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["is_enabled"] is True    # unchanged from seed
    assert data["is_beta"] is True
    assert data["is_deprecated"] is False  # unchanged from seed


@pytest.mark.asyncio
async def test_patch_only_is_deprecated(session_maker, seeded_flag):
    """Patching only is_deprecated leaves is_enabled and is_beta unchanged."""
    app = _make_app(session_maker, _build_user(superuser=True))
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            f"/api/v1/admin/feature-flags/{seeded_flag}",
            json={"is_deprecated": True},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["is_enabled"] is True    # unchanged from seed
    assert data["is_beta"] is False      # unchanged from seed
    assert data["is_deprecated"] is True


@pytest.mark.asyncio
async def test_patch_nonexistent_flag_returns_404(session_maker):
    """Patching a flag that does not exist returns 404."""
    app = _make_app(session_maker, _build_user(superuser=True))
    transport = ASGITransport(app=app)
    nonexistent_id = str(uuid.uuid4())

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.patch(
            f"/api/v1/admin/feature-flags/{nonexistent_id}",
            json={"is_enabled": False},
        )

    assert response.status_code == 404
