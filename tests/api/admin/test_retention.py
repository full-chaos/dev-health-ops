from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.retention import OrgRetentionPolicy
from dev_health_ops.models.users import Organization, User

admin_router_module = importlib.import_module("dev_health_ops.api.admin.router")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "retention.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    OrgLicense.__table__,
                    OrgRetentionPolicy.__table__,
                ],
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
