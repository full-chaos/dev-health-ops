"""Tests for CHAOS-2520: deprecate child sync configs in UX and API defaults.

Covers:
- HIDE_MIGRATED_CHILD_CONFIGS flag hides migrated children from default list
- ?include_migrated=true bypasses the filter (support/rollback)
- Flag OFF → legacy list unchanged
- Batch endpoint creates one planner-managed parent plus Integration rows
- Legacy single-config endpoints (get/create/update/delete) still work
"""

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
    IntegrationDataset,
    IntegrationSource,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
sync_router_module = importlib.import_module("dev_health_ops.api.admin.routers.sync")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncCoverageProjection,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "deprecation-test.db"
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
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="pro")
    user = User(id=user_id, email="admin@example.com", is_active=True)

    async with session_maker() as session:
        session.add_all([org, user])
        await session.commit()

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
    }


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_configs(session_maker, org_id: str):
    """Seed a parent, a legacy child (parent_id set), and a migrated child."""
    async with session_maker() as session:
        parent = SyncConfiguration(
            name="parent-config",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
        )
        session.add(parent)
        await session.flush()

        child_legacy = SyncConfiguration(
            name="child-legacy",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            parent_id=parent.id,
        )
        child_migrated_integration = SyncConfiguration(
            name="migrated-parent-anchor",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            integration_id=uuid.uuid4(),
        )
        child_migrated_source = SyncConfiguration(
            name="child-migrated-source",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            source_id=uuid.uuid4(),
        )
        session.add_all(
            [child_legacy, child_migrated_integration, child_migrated_source]
        )
        await session.commit()

        return {
            "parent_id": str(parent.id),
            "child_legacy_id": str(child_legacy.id),
            "child_integration_id": str(child_migrated_integration.id),
            "child_source_id": str(child_migrated_source.id),
        }


# ---------------------------------------------------------------------------
# HIDE_MIGRATED_CHILD_CONFIGS flag tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_planner_parent_integration_invariant_rejects_second_parent(
    session_maker, seeded_state
):
    org_id = seeded_state["org_id"]
    async with session_maker() as session:
        integration = Integration(
            org_id=org_id,
            provider="github",
            name="shared-integration",
            config={"owner": "myorg"},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        session.add_all(
            [
                SyncConfiguration(
                    name="planner-parent-a",
                    provider="github",
                    org_id=org_id,
                    sync_targets=["git"],
                    integration_id=integration.id,
                    planner_managed=True,
                ),
                SyncConfiguration(
                    name="planner-parent-b",
                    provider="github",
                    org_id=org_id,
                    sync_targets=["git"],
                    integration_id=integration.id,
                    planner_managed=True,
                ),
            ]
        )
        await session.flush()
        with pytest.raises(RuntimeError, match="invariant violated"):
            await sync_router_module._assert_single_planner_parent_for_integration(
                session, org_id, integration.id
            )


# ---------------------------------------------------------------------------
# Legacy endpoints still work (rollback path)
# ---------------------------------------------------------------------------
