from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.retention import OrgRetentionPolicy
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "retention.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    User, Organization, OrgLicense, OrgRetentionPolicy, AuditLog
                ),
            )
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
    org = Organization(id=org_id, slug="acme", name="Acme Corp", tier="enterprise")
    user = User(id=user_id, email="admin@example.com", is_active=True)

    async with session_maker() as session:
        session.add_all([org, user])
        await session.commit()

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
    }


@pytest_asyncio.fixture
async def client(monkeypatch, session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    app.dependency_overrides[admin_router_module.get_user_id] = lambda: seeded_state[
        "user_id"
    ]

    monkeypatch.setattr(
        "dev_health_ops.licensing.gating.has_feature", lambda *args, **kwargs: True
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, seeded_state

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_list_retention_policies_empty(client):
    async_client, _ = client

    response = await async_client.get("/api/v1/admin/retention-policies")

    assert response.status_code == 200
    data = response.json()
    assert data["items"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_create_retention_policy(client):
    async_client, seeded_state = client

    response = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 90},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["resource_type"] == "audit_logs"
    assert data["retention_days"] == 90
    assert data["is_active"] is True
    assert data["org_id"] == seeded_state["org_id"]
    assert "id" in data


@pytest.mark.asyncio
async def test_create_retention_policy_persists_to_db(client, session_maker):
    async_client, seeded_state = client

    response = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 60},
    )

    assert response.status_code == 201
    policy_id = response.json()["id"]

    async with session_maker() as session:
        result = await session.execute(
            select(OrgRetentionPolicy).where(
                OrgRetentionPolicy.id == uuid.UUID(policy_id)
            )
        )
        policy = result.scalar_one_or_none()

    assert policy is not None
    assert str(policy.org_id) == seeded_state["org_id"]
    assert policy.resource_type == "audit_logs"
    assert policy.retention_days == 60


@pytest.mark.asyncio
async def test_create_duplicate_retention_policy_returns_error(client):
    async_client, _ = client

    first = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 90},
    )
    second = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 30},
    )

    assert first.status_code == 201
    assert second.status_code == 400
    assert "already exists" in second.json()["detail"]


@pytest.mark.asyncio
async def test_update_retention_policy(client):
    async_client, _ = client

    create_response = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 90},
    )
    assert create_response.status_code == 201
    policy_id = create_response.json()["id"]

    update_response = await async_client.patch(
        f"/api/v1/admin/retention-policies/{policy_id}",
        json={"retention_days": 180},
    )

    assert update_response.status_code == 200
    data = update_response.json()
    assert data["retention_days"] == 180
    assert data["id"] == policy_id


@pytest.mark.asyncio
async def test_delete_retention_policy(client):
    async_client, _ = client

    create_response = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 90},
    )
    assert create_response.status_code == 201
    policy_id = create_response.json()["id"]

    delete_response = await async_client.delete(
        f"/api/v1/admin/retention-policies/{policy_id}"
    )

    assert delete_response.status_code == 200
    assert delete_response.json() == {"deleted": True}


@pytest.mark.asyncio
async def test_list_retention_resource_types(client):
    async_client, _ = client

    response = await async_client.get("/api/v1/admin/retention-policies/resource-types")

    assert response.status_code == 200
    resource_types = response.json()
    assert isinstance(resource_types, list)
    assert len(resource_types) > 0
    assert "audit_logs" in resource_types


@pytest_asyncio.fixture
async def client_no_feature(monkeypatch, session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    app.dependency_overrides[admin_router_module.get_user_id] = lambda: seeded_state[
        "user_id"
    ]

    monkeypatch.setattr(
        "dev_health_ops.licensing.gating.has_feature", lambda *args, **kwargs: False
    )

    async def _no_feature(*args, **kwargs):
        return False

    monkeypatch.setattr(
        "dev_health_ops.licensing.gating._check_org_feature_async", _no_feature
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, seeded_state

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_retention_endpoints_require_custom_retention_feature_gate_returns_402(
    client_no_feature,
):
    """Org without custom_retention gets HTTP 402 on the list endpoint (feature gate check)."""
    async_client, seeded_state = client_no_feature

    response = await async_client.get("/api/v1/admin/retention-policies")
    assert response.status_code == 402
    detail = response.json()["detail"]
    assert detail["error"] == "feature_not_licensed"
    assert detail["feature"] == "custom_retention"


@pytest.mark.asyncio
async def test_retention_endpoints_with_custom_retention_feature_returns_200(client):
    """Orgs with custom_retention feature get HTTP 200 on retention list endpoint."""
    async_client, _ = client

    response = await async_client.get("/api/v1/admin/retention-policies")
    assert response.status_code == 200


def test_all_retention_route_handlers_gated_by_custom_retention():
    """All 7 retention route handlers must have _require_feature == 'custom_retention'.

    The @require_feature decorator sets func._require_feature on the wrapped handler.
    This test introspects each handler directly to catch any future key regressions.
    """
    import dev_health_ops.api.admin.routers.retention as retention_router

    handlers = [
        retention_router.list_retention_policies,
        retention_router.list_retention_resource_types,
        retention_router.create_retention_policy,
        retention_router.get_retention_policy,
        retention_router.update_retention_policy,
        retention_router.delete_retention_policy,
        retention_router.execute_retention_policy,
    ]
    for handler in handlers:
        assert getattr(handler, "_require_feature", None) == "custom_retention", (
            f"{handler.__name__} has _require_feature={getattr(handler, '_require_feature', None)!r},"
            " expected 'custom_retention'"
        )


@pytest.mark.asyncio
async def test_execute_retention_policy_dry_run_counts_without_deleting(
    client, session_maker
):
    """dry_run=True returns the count of matching rows but does NOT delete them
    and does NOT mutate last_run_at / last_run_deleted_count / next_run_at.
    """
    async_client, seeded_state = client
    org_id = uuid.UUID(seeded_state["org_id"])

    # Create a retention policy
    create_resp = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 30},
    )
    assert create_resp.status_code == 201
    policy_id = create_resp.json()["id"]

    # Seed two old audit log rows (older than 30 days) and one recent row
    old_ts = datetime.now(timezone.utc) - timedelta(days=60)
    recent_ts = datetime.now(timezone.utc) - timedelta(days=1)
    async with session_maker() as session:
        log1 = AuditLog(
            org_id=org_id,
            action="create",
            resource_type="test",
            resource_id="r1",
            status="success",
        )
        log1.created_at = old_ts
        log2 = AuditLog(
            org_id=org_id,
            action="create",
            resource_type="test",
            resource_id="r2",
            status="success",
        )
        log2.created_at = old_ts
        log3 = AuditLog(
            org_id=org_id,
            action="create",
            resource_type="test",
            resource_id="r3",
            status="success",
        )
        log3.created_at = recent_ts
        session.add_all([log1, log2, log3])
        await session.commit()

    # Execute with dry_run=True
    exec_resp = await async_client.post(
        f"/api/v1/admin/retention-policies/{policy_id}/execute",
        json={"dry_run": True},
    )
    assert exec_resp.status_code == 200
    data = exec_resp.json()
    assert data["deleted_count"] == 2, "dry_run should count 2 old rows"
    assert data["error"] is None

    # Rows must still exist
    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog).where(AuditLog.org_id == org_id)
        )
        rows = result.scalars().all()
    assert len(rows) == 3, "dry_run must not delete any rows"

    # last_run_at must remain None (policy never mutated)
    async with session_maker() as session:
        result = await session.execute(
            select(OrgRetentionPolicy).where(
                OrgRetentionPolicy.id == uuid.UUID(policy_id)
            )
        )
        policy = result.scalar_one()
    assert policy.last_run_at is None, "dry_run must not update last_run_at"
    assert policy.last_run_deleted_count is None, (
        "dry_run must not update last_run_deleted_count"
    )


@pytest.mark.asyncio
async def test_execute_retention_policy_real_run_deletes_and_updates_metadata(
    client, session_maker
):
    """dry_run=False deletes matching rows and updates last_run_at / last_run_deleted_count."""
    async_client, seeded_state = client
    org_id = uuid.UUID(seeded_state["org_id"])

    # Create a retention policy
    create_resp = await async_client.post(
        "/api/v1/admin/retention-policies",
        json={"resource_type": "audit_logs", "retention_days": 30},
    )
    assert create_resp.status_code == 201
    policy_id = create_resp.json()["id"]

    # Seed two old audit log rows and one recent row
    old_ts = datetime.now(timezone.utc) - timedelta(days=60)
    recent_ts = datetime.now(timezone.utc) - timedelta(days=1)
    async with session_maker() as session:
        log1 = AuditLog(
            org_id=org_id,
            action="create",
            resource_type="test",
            resource_id="r1",
            status="success",
        )
        log1.created_at = old_ts
        log2 = AuditLog(
            org_id=org_id,
            action="create",
            resource_type="test",
            resource_id="r2",
            status="success",
        )
        log2.created_at = old_ts
        log3 = AuditLog(
            org_id=org_id,
            action="create",
            resource_type="test",
            resource_id="r3",
            status="success",
        )
        log3.created_at = recent_ts
        session.add_all([log1, log2, log3])
        await session.commit()

    # Execute with dry_run=False
    exec_resp = await async_client.post(
        f"/api/v1/admin/retention-policies/{policy_id}/execute",
        json={"dry_run": False},
    )
    assert exec_resp.status_code == 200
    data = exec_resp.json()
    assert data["deleted_count"] == 2, "real run should delete 2 old rows"
    assert data["error"] is None

    # Only the recent row should remain
    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog).where(AuditLog.org_id == org_id)
        )
        rows = result.scalars().all()
    assert len(rows) == 1, "real run must delete the 2 old rows"

    # last_run_at and last_run_deleted_count must be updated
    async with session_maker() as session:
        result = await session.execute(
            select(OrgRetentionPolicy).where(
                OrgRetentionPolicy.id == uuid.UUID(policy_id)
            )
        )
        policy = result.scalar_one()
    assert policy.last_run_at is not None, "real run must set last_run_at"
    assert policy.last_run_deleted_count == 2, (
        "real run must set last_run_deleted_count"
    )
