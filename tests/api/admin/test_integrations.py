"""Tests for the integration admin API (CHAOS-2515)."""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
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
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
sync_router_module = importlib.import_module("dev_health_ops.api.admin.routers.sync")
integrations_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.integrations"
)

_TABLES = tables_of(
    User,
    Organization,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncRun,
    SyncRunUnit,
    SyncConfiguration,
    IntegrationCredential,
    ScheduledJob,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "integrations.db"
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


async def _create_integration(
    ac,
    name: str = "my-integration",
    provider: str = "github",
) -> dict:
    resp = await ac.post(
        "/api/v1/admin/integrations",
        json={"name": name, "provider": provider},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _seed_source(session_maker, org_id: str, integration_id: str) -> str:
    """Directly insert an IntegrationSource row and return its id."""
    source_id = uuid.uuid4()
    async with session_maker() as session:
        source = IntegrationSource(
            id=source_id,
            org_id=org_id,
            integration_id=uuid.UUID(integration_id),
            provider="github",
            source_type="repository",
            external_id="owner/repo",
            name="repo",
            full_name="owner/repo",
            metadata_={"owner": "owner"},
            is_enabled=True,
        )
        session.add(source)
        await session.commit()
    return str(source_id)


async def _seed_dataset(session_maker, org_id: str, integration_id: str) -> str:
    """Directly insert an IntegrationDataset row and return its id."""
    dataset_id = uuid.uuid4()
    async with session_maker() as session:
        dataset = IntegrationDataset(
            id=dataset_id,
            org_id=org_id,
            integration_id=uuid.UUID(integration_id),
            dataset_key="git",
            is_enabled=True,
            options={},
        )
        session.add(dataset)
        await session.commit()
    return str(dataset_id)


async def _seed_sync_run(
    session_maker, org_id: str, integration_id: str, status: str = "planned"
) -> str:
    """Directly insert a SyncRun row and return its id."""
    run_id = uuid.uuid4()
    async with session_maker() as session:
        run = SyncRun(
            id=run_id,
            org_id=org_id,
            integration_id=uuid.UUID(integration_id),
            triggered_by="test",
            mode="incremental",
            status=status,
            total_units=0,
            completed_units=0,
            failed_units=0,
        )
        session.add(run)
        await session.commit()
    return str(run_id)


# ---------------------------------------------------------------------------
# Integration CRUD tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_integrations_empty(client):
    ac, _ = client
    resp = await ac.get("/api/v1/admin/integrations")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_integration_returns_201(client):
    ac, _ = client
    resp = await ac.post(
        "/api/v1/admin/integrations",
        json={"name": "my-gh", "provider": "github"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "my-gh"
    assert data["provider"] == "github"
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data


@pytest.mark.asyncio
async def test_create_integration_org_scoped(client, session_maker):
    """org_id must come from auth context, not client payload."""
    ac, seeded_state = client
    data = await _create_integration(ac, name="scoped-test")
    integration_id = data["id"]

    async with session_maker() as session:
        result = await session.execute(
            select(Integration).where(Integration.id == uuid.UUID(integration_id))
        )
        row = result.scalar_one_or_none()

    assert row is not None
    assert row.org_id == seeded_state["org_id"]


@pytest.mark.asyncio
async def test_list_integrations_returns_created(client):
    ac, _ = client
    await _create_integration(ac, name="int-a")
    await _create_integration(ac, name="int-b")

    resp = await ac.get("/api/v1/admin/integrations")
    assert resp.status_code == 200
    names = {i["name"] for i in resp.json()}
    assert {"int-a", "int-b"} <= names


@pytest.mark.asyncio
async def test_get_integration(client):
    ac, _ = client
    created = await _create_integration(ac, name="get-test")
    integration_id = created["id"]

    resp = await ac.get(f"/api/v1/admin/integrations/{integration_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == integration_id


@pytest.mark.asyncio
async def test_get_integration_not_found(client):
    ac, _ = client
    resp = await ac.get(f"/api/v1/admin/integrations/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_patch_integration(client):
    ac, _ = client
    created = await _create_integration(ac, name="patch-test")
    integration_id = created["id"]

    resp = await ac.patch(
        f"/api/v1/admin/integrations/{integration_id}",
        json={"name": "patched-name", "is_active": False},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "patched-name"
    assert data["is_active"] is False


@pytest.mark.asyncio
async def test_patch_integration_not_found(client):
    ac, _ = client
    resp = await ac.patch(
        f"/api/v1/admin/integrations/{uuid.uuid4()}",
        json={"name": "x"},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Discovery tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_integration_sources(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac, name="discover-test")
    integration_id = created["id"]

    mock_source = MagicMock()
    mock_source.id = uuid.uuid4()
    mock_source.org_id = seeded_state["org_id"]
    mock_source.integration_id = uuid.UUID(integration_id)
    mock_source.provider = "github"
    mock_source.source_type = "repository"
    mock_source.external_id = "owner/repo"
    mock_source.name = "repo"
    mock_source.full_name = "owner/repo"
    mock_source.metadata_ = {"owner": "owner"}
    mock_source.is_enabled = True
    mock_source.discovered_at = "2024-01-01T00:00:00+00:00"
    mock_source.last_seen_at = "2024-01-01T00:00:00+00:00"
    mock_source.last_sync_at = None
    mock_source.last_sync_success = None
    mock_source.last_sync_error = None

    with patch(
        "dev_health_ops.api.admin.routers.integrations.discover_sources_for_integration",
        return_value=[mock_source],
    ):
        resp = await ac.post(f"/api/v1/admin/integrations/{integration_id}/discover")

    assert resp.status_code == 202
    data = resp.json()
    assert data["integration_id"] == integration_id
    assert data["discovered"] == 1
    assert len(data["sources"]) == 1


@pytest.mark.asyncio
async def test_discover_integration_not_found(client):
    ac, _ = client
    resp = await ac.post(f"/api/v1/admin/integrations/{uuid.uuid4()}/discover")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Source enable/disable tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_sources_empty(client):
    ac, _ = client
    created = await _create_integration(ac)
    resp = await ac.get(f"/api/v1/admin/integrations/{created['id']}/sources")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_sources_returns_seeded(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    await _seed_source(session_maker, seeded_state["org_id"], integration_id)

    resp = await ac.get(f"/api/v1/admin/integrations/{integration_id}/sources")
    assert resp.status_code == 200
    assert len(resp.json()) == 1
    assert resp.json()[0]["full_name"] == "owner/repo"


@pytest.mark.asyncio
async def test_patch_source_disable(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )

    resp = await ac.patch(
        f"/api/v1/admin/integrations/{integration_id}/sources/{source_id}",
        json={"is_enabled": False},
    )
    assert resp.status_code == 200
    assert resp.json()["is_enabled"] is False


@pytest.mark.asyncio
async def test_patch_source_enable(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )

    # Disable first
    await ac.patch(
        f"/api/v1/admin/integrations/{integration_id}/sources/{source_id}",
        json={"is_enabled": False},
    )
    # Re-enable
    resp = await ac.patch(
        f"/api/v1/admin/integrations/{integration_id}/sources/{source_id}",
        json={"is_enabled": True},
    )
    assert resp.status_code == 200
    assert resp.json()["is_enabled"] is True


@pytest.mark.asyncio
async def test_patch_source_not_found(client):
    ac, _ = client
    created = await _create_integration(ac)
    resp = await ac.patch(
        f"/api/v1/admin/integrations/{created['id']}/sources/{uuid.uuid4()}",
        json={"is_enabled": False},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Dataset enable/disable tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_datasets_empty(client):
    ac, _ = client
    created = await _create_integration(ac)
    resp = await ac.get(f"/api/v1/admin/integrations/{created['id']}/datasets")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_datasets_returns_seeded(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    await _seed_dataset(session_maker, seeded_state["org_id"], integration_id)

    resp = await ac.get(f"/api/v1/admin/integrations/{integration_id}/datasets")
    assert resp.status_code == 200
    assert len(resp.json()) == 1
    assert resp.json()[0]["dataset_key"] == "git"


@pytest.mark.asyncio
async def test_patch_datasets_disable(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    await _seed_dataset(session_maker, seeded_state["org_id"], integration_id)

    resp = await ac.patch(
        f"/api/v1/admin/integrations/{integration_id}/datasets",
        json={"datasets": [{"dataset_key": "git", "is_enabled": False}]},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["is_enabled"] is False


@pytest.mark.asyncio
async def test_patch_datasets_not_found(client):
    ac, _ = client
    created = await _create_integration(ac)
    resp = await ac.patch(
        f"/api/v1/admin/integrations/{created['id']}/datasets",
        json={"datasets": [{"dataset_key": "nonexistent", "is_enabled": False}]},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Sync trigger tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_sync_org_scoped(client, seeded_state):
    """Sync trigger must use org_id from auth, not client payload."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]

    mock_plan = MagicMock()
    mock_plan.sync_run_id = str(uuid.uuid4())
    mock_plan.total_units = 0
    mock_plan.unit_ids = ()

    captured_request = {}

    def _fake_plan(session, request):
        captured_request["org_id"] = request.org_id
        captured_request["mode"] = request.mode
        return mock_plan

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async = MagicMock()

    with (
        patch(
            "dev_health_ops.api.admin.routers.integrations.plan_sync_run",
            side_effect=_fake_plan,
        ),
        patch(
            "dev_health_ops.api.admin.routers.integrations.dispatch_sync_run",
            mock_dispatch,
        ),
    ):
        resp = await ac.post(
            f"/api/v1/admin/integrations/{integration_id}/sync",
            json={},
        )

    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["integration_id"] == integration_id
    # org_id must come from auth, not client
    assert captured_request["org_id"] == seeded_state["org_id"]
    assert captured_request["mode"] == "incremental"


@pytest.mark.asyncio
async def test_trigger_sync_marks_run_failed_when_enqueue_fails(
    client,
    session_maker,
):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async = MagicMock(side_effect=RuntimeError("broker down"))

    with patch(
        "dev_health_ops.api.admin.routers.integrations.dispatch_sync_run",
        mock_dispatch,
    ):
        resp = await ac.post(
            f"/api/v1/admin/integrations/{integration_id}/sync",
            json={},
        )

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Task queue unavailable: broker down"

    async with session_maker() as session:
        result = await session.execute(
            select(SyncRun).where(SyncRun.integration_id == uuid.UUID(integration_id))
        )
        run = result.scalar_one()

    assert run.status == "failed"
    assert run.error == "Task queue unavailable: broker down"
    assert run.completed_at is not None


@pytest.mark.asyncio
async def test_trigger_sync_not_found(client):
    ac, _ = client
    resp = await ac.post(
        f"/api/v1/admin/integrations/{uuid.uuid4()}/sync",
        json={},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_backfill_org_scoped(client, seeded_state):
    """Backfill trigger must use org_id from auth, not client payload."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]

    mock_plan = MagicMock()
    mock_plan.sync_run_id = str(uuid.uuid4())
    mock_plan.total_units = 0
    mock_plan.unit_ids = ()

    captured_request = {}

    def _fake_plan(session, request):
        captured_request["org_id"] = request.org_id
        captured_request["mode"] = request.mode
        return mock_plan

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async = MagicMock()

    with (
        patch(
            "dev_health_ops.api.admin.routers.integrations.plan_sync_run",
            side_effect=_fake_plan,
        ),
        patch(
            "dev_health_ops.api.admin.routers.integrations.dispatch_sync_run",
            mock_dispatch,
        ),
    ):
        resp = await ac.post(
            f"/api/v1/admin/integrations/{integration_id}/backfill",
            json={
                "since": "2024-01-01T00:00:00Z",
                "before": "2024-02-01T00:00:00Z",
            },
        )

    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "accepted"
    assert captured_request["org_id"] == seeded_state["org_id"]
    assert captured_request["mode"] == "backfill"


@pytest.mark.asyncio
async def test_trigger_backfill_marks_run_failed_when_enqueue_fails(
    client,
    session_maker,
):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async = MagicMock(side_effect=RuntimeError("broker down"))

    with patch(
        "dev_health_ops.api.admin.routers.integrations.dispatch_sync_run",
        mock_dispatch,
    ):
        resp = await ac.post(
            f"/api/v1/admin/integrations/{integration_id}/backfill",
            json={
                "since": "2024-01-01T00:00:00Z",
                "before": "2024-02-01T00:00:00Z",
            },
        )

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Task queue unavailable: broker down"

    async with session_maker() as session:
        result = await session.execute(
            select(SyncRun).where(SyncRun.integration_id == uuid.UUID(integration_id))
        )
        run = result.scalar_one()

    assert run.status == "failed"
    assert run.mode == "backfill"
    assert run.error == "Task queue unavailable: broker down"
    assert run.completed_at is not None


@pytest.mark.asyncio
async def test_trigger_backfill_not_found(client):
    ac, _ = client
    resp = await ac.post(
        f"/api/v1/admin/integrations/{uuid.uuid4()}/backfill",
        json={
            "since": "2024-01-01T00:00:00Z",
            "before": "2024-02-01T00:00:00Z",
        },
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Sync run status tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sync_run(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == run_id
    assert data["integration_id"] == integration_id
    assert data["status"] == "planned"


@pytest.mark.asyncio
async def test_get_sync_run_not_found(client):
    ac, _ = client
    resp = await ac.get(f"/api/v1/admin/sync-runs/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_sync_run_units_empty(client, session_maker, seeded_state):
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert data["units"] == []
    assert data["by_status"] == {}
    assert data["by_source"] == {}
    assert data["by_dataset"] == {}
    assert data["by_cost_class"] == {}


@pytest.mark.asyncio
async def test_get_sync_run_units_rollups(client, session_maker, seeded_state):
    """Rollup dicts are correctly computed from unit rows."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    # Seed two SyncRunUnit rows
    async with session_maker() as session:
        for status in ("success", "failed"):
            unit = SyncRunUnit(
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="git",
                cost_class="standard",
                mode="incremental",
                status=status,
                attempts=1,
            )
            session.add(unit)
        await session.commit()

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["units"]) == 2
    assert data["by_status"]["success"] == 1
    assert data["by_status"]["failed"] == 1
    assert data["by_cost_class"]["standard"] == 2
    assert "git" in data["by_dataset"]


# ---------------------------------------------------------------------------
# Existing sync-config endpoints still importable / unbroken
# ---------------------------------------------------------------------------


def test_sync_config_router_still_importable():
    """Existing sync-config router must not be broken by the new router."""
    router = sync_router_module.router
    paths = {r.path for r in router.routes}
    assert any("sync-configs" in p for p in paths)


@pytest.mark.asyncio
async def test_sync_configs_list_still_works(client):
    """Existing /sync-configs endpoint must still return 200."""
    ac, _ = client
    resp = await ac.get("/api/v1/admin/sync-configs")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_trigger_sync_empty_selection_is_zero_units_not_all(client):
    # Regression (Codex Wave 3): an explicit empty source/dataset list must
    # mean ZERO units, not "sync everything" (None). Truthiness collapsed [] to
    # None, turning an empty UI selection into a full integration sync.
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]

    mock_plan = MagicMock()
    mock_plan.sync_run_id = str(uuid.uuid4())
    mock_plan.total_units = 0
    mock_plan.unit_ids = ()
    captured = {}

    def _fake_plan(session, request):
        captured["source_ids"] = request.source_ids
        captured["dataset_keys"] = request.dataset_keys
        return mock_plan

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async = MagicMock()
    with (
        patch(
            "dev_health_ops.api.admin.routers.integrations.plan_sync_run",
            side_effect=_fake_plan,
        ),
        patch(
            "dev_health_ops.api.admin.routers.integrations.dispatch_sync_run",
            mock_dispatch,
        ),
    ):
        resp = await ac.post(
            f"/api/v1/admin/integrations/{integration_id}/sync",
            json={"source_ids": [], "dataset_keys": []},
        )

    assert resp.status_code == 202
    # explicit empty list -> empty tuple (zero units), NOT None (all enabled)
    assert captured["source_ids"] == ()
    assert captured["dataset_keys"] == ()


@pytest.mark.asyncio
async def test_create_integration_rejects_foreign_credential(client):
    # Regression (Codex Wave 3): credential_id must reference a credential that
    # belongs to the authenticated org + provider; a random/foreign UUID is 400.
    ac, _ = client
    resp = await ac.post(
        "/api/v1/admin/integrations",
        json={
            "name": "bad-cred",
            "provider": "github",
            "credential_id": str(uuid.uuid4()),
        },
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# CHAOS-2519: new rollup fields (slowest_unit_ids, failed_unit_ids,
# partial_failure_summary)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sync_run_units_new_rollup_fields_present_empty(
    client, session_maker, seeded_state
):
    """New rollup fields are present even when there are no units."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert "slowest_unit_ids" in data
    assert "failed_unit_ids" in data
    assert "partial_failure_summary" in data
    assert data["slowest_unit_ids"] == []
    assert data["failed_unit_ids"] == []
    assert data["partial_failure_summary"] is None


@pytest.mark.asyncio
async def test_get_sync_run_units_failed_unit_ids(client, session_maker, seeded_state):
    """failed_unit_ids lists IDs of all failed units."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    failed_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            SyncRunUnit(
                id=failed_id,
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="git",
                cost_class="standard",
                mode="incremental",
                status="failed",
                attempts=1,
                result={"error_category": "timeout"},
            )
        )
        await session.commit()

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert str(failed_id) in data["failed_unit_ids"]


@pytest.mark.asyncio
async def test_get_sync_run_units_slowest_unit_ids(client, session_maker, seeded_state):
    """slowest_unit_ids lists up to 5 unit IDs sorted by duration desc."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    slow_id = uuid.uuid4()
    fast_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            SyncRunUnit(
                id=slow_id,
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="git",
                cost_class="standard",
                mode="incremental",
                status="success",
                attempts=1,
                duration_seconds=120,
            )
        )
        session.add(
            SyncRunUnit(
                id=fast_id,
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="prs",
                cost_class="standard",
                mode="incremental",
                status="success",
                attempts=1,
                duration_seconds=5,
            )
        )
        await session.commit()

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["slowest_unit_ids"]) == 2
    # Slowest first
    assert data["slowest_unit_ids"][0] == str(slow_id)
    assert data["slowest_unit_ids"][1] == str(fast_id)


@pytest.mark.asyncio
async def test_get_sync_run_units_partial_failure_summary(
    client, session_maker, seeded_state
):
    """partial_failure_summary is populated when run is partial (some success, some failed)."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    async with session_maker() as session:
        session.add(
            SyncRunUnit(
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="git",
                cost_class="standard",
                mode="incremental",
                status="success",
                attempts=1,
            )
        )
        session.add(
            SyncRunUnit(
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="prs",
                cost_class="standard",
                mode="incremental",
                status="failed",
                attempts=1,
                result={"error_category": "rate_limit"},
            )
        )
        await session.commit()

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    summary = data["partial_failure_summary"]
    assert summary is not None
    assert "failed_sources" in summary
    assert "failed_datasets" in summary
    assert "error_categories" in summary
    assert "prs" in summary["failed_datasets"]
    assert summary["error_categories"].get("rate_limit") == 1


@pytest.mark.asyncio
async def test_get_sync_run_units_all_success_no_partial_summary(
    client, session_maker, seeded_state
):
    """partial_failure_summary is None when all units succeed."""
    ac, _ = client
    created = await _create_integration(ac)
    integration_id = created["id"]
    source_id = await _seed_source(
        session_maker, seeded_state["org_id"], integration_id
    )
    run_id = await _seed_sync_run(session_maker, seeded_state["org_id"], integration_id)

    async with session_maker() as session:
        session.add(
            SyncRunUnit(
                org_id=seeded_state["org_id"],
                sync_run_id=uuid.UUID(run_id),
                integration_id=uuid.UUID(integration_id),
                source_id=uuid.UUID(source_id),
                provider="github",
                dataset_key="git",
                cost_class="standard",
                mode="incremental",
                status="success",
                attempts=1,
            )
        )
        await session.commit()

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert data["partial_failure_summary"] is None
    assert data["failed_unit_ids"] == []
