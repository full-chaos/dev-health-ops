from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import TeamMapping
from dev_health_ops.models.users import Membership, Organization, User

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "teams.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
                    TeamMapping.__table__,
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
    mock_sync = MagicMock()
    monkeypatch.setattr(
        "dev_health_ops.workers.tasks.sync_teams_to_analytics", mock_sync
    )

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

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, seeded_state, mock_sync

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_list_teams_empty(client):
    async_client, _, _ = client
    response = await async_client.get("/api/v1/admin/teams")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_create_team_returns_response_shape(client):
    async_client, _, _ = client
    payload = {
        "team_id": "backend-team",
        "name": "Backend Team",
        "description": "Handles backend services",
        "repo_patterns": ["backend/*"],
        "project_keys": ["BACK"],
    }
    response = await async_client.post("/api/v1/admin/teams", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["team_id"] == "backend-team"
    assert data["name"] == "Backend Team"
    assert data["description"] == "Handles backend services"
    assert data["repo_patterns"] == ["backend/*"]
    assert data["project_keys"] == ["BACK"]
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data
    assert "updated_at" in data


@pytest.mark.asyncio
async def test_create_team_persists_to_db(client, session_maker):
    async_client, seeded_state, _ = client
    payload = {
        "team_id": "frontend-team",
        "name": "Frontend Team",
        "description": "Handles UI work",
        "repo_patterns": ["frontend/*"],
        "project_keys": ["FRONT"],
    }
    response = await async_client.post("/api/v1/admin/teams", json=payload)
    assert response.status_code == 200

    async with session_maker() as session:
        result = await session.execute(
            select(TeamMapping).where(
                TeamMapping.team_id == "frontend-team",
                TeamMapping.org_id == seeded_state["org_id"],
            )
        )
        team = result.scalar_one_or_none()

    assert team is not None
    assert team.name == "Frontend Team"
    assert team.description == "Handles UI work"
    assert team.is_active is True


@pytest.mark.asyncio
async def test_create_team_triggers_celery_sync(client):
    async_client, _, mock_sync = client
    payload = {
        "team_id": "ops-team",
        "name": "Ops Team",
        "repo_patterns": [],
        "project_keys": [],
    }
    response = await async_client.post("/api/v1/admin/teams", json=payload)
    assert response.status_code == 200
    mock_sync.apply_async.assert_called_once()


@pytest.mark.asyncio
async def test_get_team_by_id(client):
    async_client, _, _ = client
    create_payload = {
        "team_id": "data-team",
        "name": "Data Team",
        "description": "Data engineering",
        "repo_patterns": ["data/*"],
        "project_keys": [],
    }
    create_response = await async_client.post(
        "/api/v1/admin/teams", json=create_payload
    )
    assert create_response.status_code == 200

    response = await async_client.get("/api/v1/admin/teams/data-team")
    assert response.status_code == 200
    data = response.json()
    assert data["team_id"] == "data-team"
    assert data["name"] == "Data Team"
    assert data["description"] == "Data engineering"


@pytest.mark.asyncio
async def test_update_team_description(client):
    async_client, _, _ = client
    create_payload = {
        "team_id": "infra-team",
        "name": "Infra Team",
        "description": "Old description",
        "repo_patterns": [],
        "project_keys": [],
    }
    create_response = await async_client.post(
        "/api/v1/admin/teams", json=create_payload
    )
    assert create_response.status_code == 200

    update_response = await async_client.patch(
        "/api/v1/admin/teams/infra-team",
        json={"description": "New description"},
    )
    assert update_response.status_code == 200
    data = update_response.json()
    assert data["description"] == "New description"
    assert data["team_id"] == "infra-team"


@pytest.mark.asyncio
async def test_delete_team(client):
    async_client, _, _ = client
    create_payload = {
        "team_id": "temp-team",
        "name": "Temp Team",
        "repo_patterns": [],
        "project_keys": [],
    }
    create_response = await async_client.post(
        "/api/v1/admin/teams", json=create_payload
    )
    assert create_response.status_code == 200

    delete_response = await async_client.delete("/api/v1/admin/teams/temp-team")
    assert delete_response.status_code == 200
    assert delete_response.json() == {"deleted": True}


@pytest.mark.asyncio
async def test_delete_nonexistent_team_returns_404(client):
    async_client, _, _ = client
    response = await async_client.delete("/api/v1/admin/teams/nonexistent-team")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_teams_returns_multiple_teams(client):
    async_client, _, _ = client
    teams_to_create = [
        {
            "team_id": "team-alpha",
            "name": "Team Alpha",
            "repo_patterns": [],
            "project_keys": [],
        },
        {
            "team_id": "team-beta",
            "name": "Team Beta",
            "repo_patterns": [],
            "project_keys": [],
        },
    ]
    for t in teams_to_create:
        r = await async_client.post("/api/v1/admin/teams", json=t)
        assert r.status_code == 200

    response = await async_client.get("/api/v1/admin/teams")
    assert response.status_code == 200
    data = response.json()
    team_ids = [t["team_id"] for t in data]
    assert "team-alpha" in team_ids
    assert "team-beta" in team_ids
    assert len(data) >= 2


@pytest.mark.asyncio
async def test_import_teams_creates_team_mappings(client, session_maker):
    async_client, seeded_state, mock_sync = client
    payload = {
        "teams": [
            {
                "provider_type": "github",
                "provider_team_id": "backend",
                "name": "Backend Squad",
                "description": "Backend services team",
            },
            {
                "provider_type": "github",
                "provider_team_id": "frontend",
                "name": "Frontend Squad",
                "description": "Frontend services team",
            },
        ],
        "on_conflict": "skip",
    }
    response = await async_client.post("/api/v1/admin/teams/import", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["imported"] == 2
    assert data["skipped"] == 0
    assert "details" in data

    async with session_maker() as session:
        result = await session.execute(
            select(TeamMapping).where(TeamMapping.org_id == seeded_state["org_id"])
        )
        teams = result.scalars().all()

    assert len(teams) == 2
    team_names = {t.name for t in teams}
    assert "Backend Squad" in team_names
    assert "Frontend Squad" in team_names
    mock_sync.apply_async.assert_called()
