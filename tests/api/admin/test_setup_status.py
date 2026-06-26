"""CHAOS-2677: GET /api/v1/admin/setup/status (contract C2).

Seeds each of the four first-run states the dashboard must distinguish
(not-connected, connected-no-config, config-failed, sync-running) plus a
non-admin 403, and asserts the C2 projection over credentials + sync config +
planner job runs.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timezone
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
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    Membership,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncRun,
    SyncRunUnit,
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "setup-status.db"
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
    org = Organization(id=org_id, slug="test-org", name="Test Org")
    user = User(id=user_id, email="admin@example.com", is_active=True)
    async with session_maker() as session:
        session.add_all([org, user])
        session.add(Membership(org_id=org_id, user_id=user_id, role="owner"))
        await session.commit()
    return {"org_id": str(org_id), "user_id": str(user_id)}


def _build_app(session_maker, current_user: AuthenticatedUser) -> FastAPI:
    app = FastAPI()
    app.include_router(admin_router_module.router)

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: current_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    return app


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )
    app = _build_app(session_maker, admin_user)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


async def _add_credential(session_maker, org_id: str, provider: str = "github") -> None:
    async with session_maker() as session:
        session.add(
            IntegrationCredential(
                provider=provider,
                name="github-app",
                org_id=org_id,
                credentials_encrypted="enc",
                is_active=True,
            )
        )
        await session.commit()


async def _add_config(
    session_maker,
    org_id: str,
    *,
    provider: str = "github",
    name: str = "primary",
    with_repo_source: bool = True,
    last_sync_success: bool | None = None,
    last_sync_error: str | None = None,
) -> str:
    async with session_maker() as session:
        integration = Integration(
            org_id=org_id,
            provider=provider,
            name=f"{name}-integration",
            config={},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        config = SyncConfiguration(
            name=name,
            provider=provider,
            org_id=org_id,
            sync_targets=["git"],
            sync_options={},
            is_active=True,
            integration_id=integration.id,
        )
        config.planner_managed = True
        config.last_sync_success = last_sync_success
        config.last_sync_error = last_sync_error
        session.add(config)
        if with_repo_source:
            session.add(
                IntegrationSource(
                    org_id=org_id,
                    integration_id=integration.id,
                    provider=provider,
                    source_type="repository",
                    external_id="acme/repo",
                    name="repo",
                    full_name="acme/repo",
                    metadata_={},
                    is_enabled=True,
                )
            )
        await session.flush()
        config_id = str(config.id)
        await session.commit()
    return config_id


async def _add_job_run(
    session_maker,
    org_id: str,
    config_id: str,
    *,
    status: int,
    error: str | None = None,
) -> None:
    async with session_maker() as session:
        job = ScheduledJob(
            name=f"sync-config-{config_id}",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id=org_id,
            provider="github",
            sync_config_id=uuid.UUID(config_id),
        )
        session.add(job)
        await session.flush()
        run = JobRun(job_id=job.id, triggered_by="manual", status=status)
        run.error = error
        run.created_at = datetime.now(timezone.utc)
        session.add(run)
        await session.commit()


# ---------------------------------------------------------------------------
# State coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_setup_status_not_connected(client):
    ac, _ = client

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["has_integration"] is False
    assert data["providers"] == []
    assert data["has_sync_config"] is False
    assert data["sync_status"] == "none"
    assert data["next_action"] == "connect_integration"
    assert data["blocker"] == "No integration connected"


@pytest.mark.asyncio
async def test_setup_status_connected_no_config_github(client, session_maker):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"], provider="github")

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["has_integration"] is True
    assert data["providers"] == ["github"]
    assert data["has_sync_config"] is False
    assert data["sync_config_id"] is None
    assert data["next_action"] == "select_repositories"
    assert data["blocker"] == "No sync configuration"


@pytest.mark.asyncio
async def test_setup_status_connected_no_config_non_repo_provider(
    client, session_maker
):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"], provider="jira")

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["providers"] == ["jira"]
    assert data["next_action"] == "create_sync_config"


@pytest.mark.asyncio
async def test_setup_status_config_ready_to_start(client, session_maker):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"])
    config_id = await _add_config(session_maker, seeded_state["org_id"])

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["has_sync_config"] is True
    assert data["sync_config_id"] == config_id
    assert data["selected_repositories_count"] == 1
    assert data["first_sync_started"] is False
    assert data["sync_status"] == "none"
    assert data["can_start_sync"] is True
    assert data["next_action"] == "start_sync"


@pytest.mark.asyncio
async def test_setup_status_config_without_repo_selection_blocks(client, session_maker):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"])
    await _add_config(session_maker, seeded_state["org_id"], with_repo_source=False)

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["selected_repositories_count"] == 0
    assert data["can_start_sync"] is False
    assert data["next_action"] == "select_repositories"
    assert data["blocker"] == "No repositories selected"


@pytest.mark.asyncio
async def test_setup_status_config_failed_via_job_run(client, session_maker):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"])
    config_id = await _add_config(session_maker, seeded_state["org_id"])
    await _add_job_run(
        session_maker,
        seeded_state["org_id"],
        config_id,
        status=JobRunStatus.FAILED.value,
        error="boom: credentials expired",
    )

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["first_sync_started"] is True
    assert data["sync_status"] == "failed"
    assert data["last_sync_error"] == "boom: credentials expired"
    assert data["next_action"] == "start_sync"
    assert data["blocker"] == "boom: credentials expired"


@pytest.mark.asyncio
async def test_setup_status_config_failed_via_last_sync_fields(client, session_maker):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"])
    await _add_config(
        session_maker,
        seeded_state["org_id"],
        last_sync_success=False,
        last_sync_error="prior failure",
    )

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["sync_status"] == "failed"
    assert data["last_sync_error"] == "prior failure"
    assert data["next_action"] == "start_sync"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (JobRunStatus.PENDING.value, "pending"),
        (JobRunStatus.RUNNING.value, "running"),
    ],
)
async def test_setup_status_sync_running(client, session_maker, status, expected):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"])
    config_id = await _add_config(session_maker, seeded_state["org_id"])
    await _add_job_run(session_maker, seeded_state["org_id"], config_id, status=status)

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["first_sync_started"] is True
    assert data["sync_status"] == expected
    assert data["can_start_sync"] is False
    assert data["next_action"] == "complete"
    assert data["blocker"] is None


@pytest.mark.asyncio
async def test_setup_status_sync_complete(client, session_maker):
    ac, seeded_state = client
    await _add_credential(session_maker, seeded_state["org_id"])
    config_id = await _add_config(session_maker, seeded_state["org_id"])
    await _add_job_run(
        session_maker,
        seeded_state["org_id"],
        config_id,
        status=JobRunStatus.SUCCESS.value,
    )

    resp = await ac.get("/api/v1/admin/setup/status")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["sync_status"] == "complete"
    assert data["first_sync_started"] is True
    assert data["next_action"] == "complete"


@pytest.mark.asyncio
async def test_setup_status_requires_admin(session_maker, seeded_state):
    non_admin = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="member@example.com",
        org_id=seeded_state["org_id"],
        role="member",
        is_superuser=False,
    )
    app = _build_app(session_maker, non_admin)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/admin/setup/status")
    app.dependency_overrides.clear()

    assert resp.status_code == 403, resp.text
