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
from dev_health_ops.models.settings import IdentityMapping
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
admin_router_module = importlib.import_module("dev_health_ops.api.admin")

ORG_ID = str(uuid.uuid4())
USER_ID = str(uuid.uuid4())
ADMIN_EMAIL = "admin@example.com"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "identities.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(User, Organization, Membership, IdentityMapping),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(session_maker):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: (
        AuthenticatedUser(
            user_id=USER_ID,
            email=ADMIN_EMAIL,
            org_id=ORG_ID,
            role="owner",
            is_superuser=False,
        )
    )
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, session_maker

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_list_identities_empty(client):
    async_client, _ = client
    response = await async_client.get("/api/v1/admin/identities")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_create_identity_returns_response_shape(client):
    async_client, _ = client
    payload = {
        "canonical_id": "alice@example.com",
        "display_name": "Alice Smith",
        "email": "alice@example.com",
        "provider_identities": {"github": ["alice-gh"]},
        "team_ids": ["team-1"],
    }
    response = await async_client.post("/api/v1/admin/identities", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["canonical_id"] == "alice@example.com"
    assert data["display_name"] == "Alice Smith"
    assert data["email"] == "alice@example.com"
    assert data["provider_identities"] == {"github": ["alice-gh"]}
    assert data["team_ids"] == ["team-1"]
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data
    assert "updated_at" in data


@pytest.mark.asyncio
async def test_create_identity_persists_to_db(client):
    async_client, session_maker = client
    payload = {
        "canonical_id": "bob@example.com",
        "display_name": "Bob Jones",
        "email": "bob@example.com",
        "provider_identities": {"jira": ["bob-jira-id"]},
        "team_ids": [],
    }
    response = await async_client.post("/api/v1/admin/identities", json=payload)
    assert response.status_code == 200

    async with session_maker() as session:
        result = await session.execute(
            select(IdentityMapping).where(
                IdentityMapping.canonical_id == "bob@example.com",
                IdentityMapping.org_id == ORG_ID,
            )
        )
        mapping = result.scalar_one_or_none()

    assert mapping is not None
    assert mapping.display_name == "Bob Jones"
    assert mapping.email == "bob@example.com"
    assert mapping.provider_identities == {"jira": ["bob-jira-id"]}
    assert mapping.is_active is True


@pytest.mark.asyncio
async def test_update_identity_by_canonical_id(client):
    async_client, session_maker = client
    canonical_id = "carol@example.com"

    # Create initial record
    create_payload = {
        "canonical_id": canonical_id,
        "display_name": "Carol",
        "email": canonical_id,
        "provider_identities": {"github": ["carol-gh"]},
        "team_ids": ["team-a"],
    }
    r1 = await async_client.post("/api/v1/admin/identities", json=create_payload)
    assert r1.status_code == 200
    original_id = r1.json()["id"]

    # Upsert with updated fields using same canonical_id
    update_payload = {
        "canonical_id": canonical_id,
        "display_name": "Carol Updated",
        "email": canonical_id,
        "provider_identities": {"github": ["carol-gh"], "jira": ["carol-jira"]},
        "team_ids": ["team-a", "team-b"],
    }
    r2 = await async_client.post("/api/v1/admin/identities", json=update_payload)
    assert r2.status_code == 200
    data = r2.json()

    # Should be same record (same id), updated fields
    assert data["id"] == original_id
    assert data["display_name"] == "Carol Updated"
    assert data["provider_identities"] == {
        "github": ["carol-gh"],
        "jira": ["carol-jira"],
    }
    assert data["team_ids"] == ["team-a", "team-b"]

    # Verify only one record in db
    async with session_maker() as session:
        result = await session.execute(
            select(IdentityMapping).where(
                IdentityMapping.canonical_id == canonical_id,
                IdentityMapping.org_id == ORG_ID,
            )
        )
        rows = result.scalars().all()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_list_identities_active_only_filter(client):
    async_client, session_maker = client

    # Create an active and an inactive mapping directly in db
    active = IdentityMapping(
        org_id=ORG_ID,
        canonical_id="active@example.com",
        email="active@example.com",
        provider_identities={},
        team_ids=[],
        is_active=True,
    )
    inactive = IdentityMapping(
        org_id=ORG_ID,
        canonical_id="inactive@example.com",
        email="inactive@example.com",
        provider_identities={},
        team_ids=[],
        is_active=False,
    )
    async with session_maker() as session:
        session.add_all([active, inactive])
        await session.commit()

    # Default active_only=True
    response = await async_client.get("/api/v1/admin/identities")
    assert response.status_code == 200
    canonical_ids = [m["canonical_id"] for m in response.json()]
    assert "active@example.com" in canonical_ids
    assert "inactive@example.com" not in canonical_ids


@pytest.mark.asyncio
async def test_list_identities_includes_inactive_when_false(client):
    async_client, session_maker = client

    active = IdentityMapping(
        org_id=ORG_ID,
        canonical_id="active2@example.com",
        email="active2@example.com",
        provider_identities={},
        team_ids=[],
        is_active=True,
    )
    inactive = IdentityMapping(
        org_id=ORG_ID,
        canonical_id="inactive2@example.com",
        email="inactive2@example.com",
        provider_identities={},
        team_ids=[],
        is_active=False,
    )
    async with session_maker() as session:
        session.add_all([active, inactive])
        await session.commit()

    # active_only=False should include both
    response = await async_client.get("/api/v1/admin/identities?active_only=false")
    assert response.status_code == 200
    canonical_ids = [m["canonical_id"] for m in response.json()]
    assert "active2@example.com" in canonical_ids
    assert "inactive2@example.com" in canonical_ids
