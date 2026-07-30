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
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag
from tests._helpers import tables_of

_TABLES = tables_of(FeatureFlag)

# Import the actual module (not the re-exported router object)
_features_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.features"
)
_features_router = _features_router_module.router

_common_module = importlib.import_module("dev_health_ops.api.admin.routers.common")
_get_session = _common_module.get_session

_unused_middleware_module = importlib.import_module(
    "dev_health_ops.api.admin.middleware"
)


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


def _apply_ask_dev_catalog_migrations(connection: Connection) -> None:
    migration_context = MigrationContext.configure(connection)
    migration_modules = (
        "dev_health_ops.alembic.versions.0067_seed_ask_dev_feature_flag",
        "dev_health_ops.alembic.versions.0070_seed_ask_dev_contextual_entrypoints_feature_flag",
    )
    with Operations.context(migration_context):
        for module_name in migration_modules:
            importlib.import_module(module_name).upgrade()


@pytest_asyncio.fixture
async def migrated_catalog_session_maker(tmp_path):
    """Build the catalog as the current Ask Dev seed migrations do."""
    db_path = tmp_path / "migrated-feature-catalog.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
        await conn.run_sync(_apply_ask_dev_catalog_migrations)

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
    assert data["is_beta"] is False  # unchanged from seed
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
    assert data["is_enabled"] is True  # unchanged from seed
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
    assert data["is_enabled"] is True  # unchanged from seed
    assert data["is_beta"] is False  # unchanged from seed
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


@pytest.mark.asyncio
async def test_superuser_list_includes_canonical_ask_dev_migration_rows(
    migrated_catalog_session_maker,
):
    app = _make_app(
        migrated_catalog_session_maker,
        _build_user(superuser=True),
    )
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/admin/feature-flags")

    assert response.status_code == 200
    rows = response.json()
    assert [row["key"] for row in rows] == [
        "ask_dev",
        "ask_dev_contextual_entrypoints",
    ]
    assert [
        {
            "key": row["key"],
            "name": row["name"],
            "category": row["category"],
            "min_tier": row["min_tier"],
            "is_enabled": row["is_enabled"],
            "is_beta": row["is_beta"],
            "is_deprecated": row["is_deprecated"],
        }
        for row in rows
    ] == [
        {
            "key": "ask_dev",
            "name": "Ask Dev",
            "category": "analytics",
            "min_tier": "community",
            "is_enabled": True,
            "is_beta": False,
            "is_deprecated": False,
        },
        {
            "key": "ask_dev_contextual_entrypoints",
            "name": "Ask Dev Contextual Entrypoints",
            "category": "analytics",
            "min_tier": "community",
            "is_enabled": True,
            "is_beta": False,
            "is_deprecated": False,
        },
    ]
    assert len({row["id"] for row in rows}) == 2
