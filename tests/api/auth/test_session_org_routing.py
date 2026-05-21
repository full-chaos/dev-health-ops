from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bcrypt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.routers.common import OrganizationActivity
from dev_health_ops.api.services.auth import AuthenticatedUser, AuthService
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import LoginAttempt, Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
login_router_module = importlib.import_module("dev_health_ops.api.auth.routers.login")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "session-org-routing.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    User,
                    Organization,
                    Membership,
                    AuditLog,
                    LoginAttempt,
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
    password_hash = bcrypt.hashpw(b"SecurePass123!", bcrypt.gensalt()).decode()
    user = User(
        id=uuid.uuid4(),
        email="multi@example.com",
        username="multi",
        password_hash=password_hash,
        is_active=True,
        is_verified=True,
    )
    empty_org = Organization(
        id=uuid.uuid4(), slug="empty", name="Empty Org", tier="community"
    )
    data_org = Organization(
        id=uuid.uuid4(), slug="data", name="Data Org", tier="team"
    )
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        session.add_all([user, empty_org, data_org])
        session.add_all(
            [
                Membership(
                    user_id=user.id,
                    org_id=empty_org.id,
                    role="member",
                    joined_at=now - timedelta(days=30),
                ),
                Membership(
                    user_id=user.id,
                    org_id=data_org.id,
                    role="admin",
                    joined_at=now - timedelta(days=1),
                ),
            ]
        )
        await session.commit()

    return {
        "user_id": str(user.id),
        "email": str(user.email),
        "empty_org_id": str(empty_org.id),
        "data_org_id": str(data_org.id),
    }


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker, seeded_state):
    app = FastAPI()
    app.include_router(auth_router_module.router)

    current_user = {
        "value": AuthenticatedUser(
            user_id=seeded_state["user_id"],
            email=seeded_state["email"],
            org_id=seeded_state["empty_org_id"],
            role="member",
            is_superuser=False,
        )
    }

    @asynccontextmanager
    async def _session_override():
        async with session_maker() as session:
            yield session

    async def _noop_refresh(*args, **kwargs):
        return None

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: current_user[
        "value"
    ]
    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(
        auth_router_module,
        "get_auth_service",
        lambda: AuthService(secret_key="session-org-routing-test-secret"),
    )
    monkeypatch.setattr(auth_router_module, "create_refresh_token_record", _noop_refresh)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, current_user

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_login_defaults_to_membership_with_metrics_activity(
    client, seeded_state, monkeypatch: pytest.MonkeyPatch
):
    async_client, _current_user = client

    def _activity(org_ids):
        return {
            org_id: OrganizationActivity(
                has_data=str(org_id) == seeded_state["data_org_id"],
                last_metrics_at=datetime(2026, 5, 1, tzinfo=timezone.utc)
                if str(org_id) == seeded_state["data_org_id"]
                else None,
            )
            for org_id in org_ids
        }

    monkeypatch.setattr(login_router_module, "_load_org_activity", _activity)

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "multi@example.com", "password": "SecurePass123!"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["user"]["org_id"] == seeded_state["data_org_id"]
    assert data["user"]["role"] == "admin"
    assert data["access_token"]
    assert data["refresh_token"]


@pytest.mark.asyncio
async def test_me_organizations_marks_active_org_and_data_state(
    client, seeded_state, monkeypatch: pytest.MonkeyPatch
):
    async_client, _current_user = client

    def _activity(org_ids):
        return {
            org_id: OrganizationActivity(
                has_data=str(org_id) == seeded_state["data_org_id"],
                last_metrics_at=datetime(2026, 5, 2, tzinfo=timezone.utc)
                if str(org_id) == seeded_state["data_org_id"]
                else None,
            )
            for org_id in org_ids
        }

    monkeypatch.setattr(
        "dev_health_ops.api.auth.routers.session._load_org_activity", _activity
    )

    response = await async_client.get("/api/v1/auth/me/organizations")

    assert response.status_code == 200
    data = response.json()
    assert data["active_org_id"] == seeded_state["empty_org_id"]
    organizations = {org["id"]: org for org in data["organizations"]}
    assert organizations[seeded_state["empty_org_id"]]["has_data"] is False
    assert organizations[seeded_state["data_org_id"]]["has_data"] is True
    assert organizations[seeded_state["data_org_id"]]["last_metrics_at"] is not None


@pytest.mark.asyncio
async def test_switch_org_issues_tokens_for_selected_membership(client, seeded_state):
    async_client, _current_user = client

    response = await async_client.post(
        "/api/v1/auth/switch-org",
        json={"org_id": seeded_state["data_org_id"]},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["user"]["org_id"] == seeded_state["data_org_id"]
    assert data["user"]["role"] == "admin"
    assert data["access_token"]
    assert data["refresh_token"]


@pytest.mark.asyncio
async def test_switch_org_rejects_non_membership(client):
    async_client, _current_user = client

    response = await async_client.post(
        "/api/v1/auth/switch-org",
        json={"org_id": str(uuid.uuid4())},
    )

    assert response.status_code == 403
