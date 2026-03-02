from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.api.services.auth import AuthenticatedUser, AuthService
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.email_verification_token import EmailVerificationToken
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    IdentityMapping,
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
    TeamMapping,
)
from dev_health_ops.models.users import LoginAttempt, Membership, Organization, User

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
admin_router_module = importlib.import_module("dev_health_ops.api.admin.router")

VALID_PASSWORD = "SecurePass123!"

_TABLES = [
    User.__table__,
    Organization.__table__,
    Membership.__table__,
    AuditLog.__table__,
    LoginAttempt.__table__,
    EmailVerificationToken.__table__,
    IntegrationCredential.__table__,
    SyncConfiguration.__table__,
    ScheduledJob.__table__,
    JobRun.__table__,
    IdentityMapping.__table__,
    TeamMapping.__table__,
]


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "journey.db"
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
async def journey_app(monkeypatch: pytest.MonkeyPatch, session_maker):
    app = FastAPI()
    app.include_router(auth_router_module.router)
    app.include_router(admin_router_module.router)

    current_user: dict = {"value": None}

    async def _admin_session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    @asynccontextmanager
    async def _auth_session_override():
        async with session_maker() as session:
            yield session

    async def _noop_refresh(*args, **kwargs):
        return None

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: current_user["value"]
    app.dependency_overrides[admin_router_module.get_session] = _admin_session_override

    monkeypatch.setattr(auth_router_module, "get_postgres_session", _auth_session_override)
    monkeypatch.setattr(
        auth_router_module,
        "get_auth_service",
        lambda: AuthService(secret_key="journey-test-secret"),
    )
    monkeypatch.setattr(auth_router_module, "create_refresh_token_record", _noop_refresh)
    monkeypatch.setattr(rate_limiter, "enabled", False)
    monkeypatch.setattr(
        "dev_health_ops.api.services.email_verification.send_verification_email",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.tasks.sync_teams_to_analytics",
        MagicMock(),
    )
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "journey-test-encryption-key")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, current_user, session_maker

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_creates_user_and_org(journey_app):
    ac, _, session_maker = journey_app

    response = await ac.post(
        "/api/v1/auth/register",
        json={"email": "journey1@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 201
    data = response.json()
    user_id = uuid.UUID(data["user_id"])
    org_id = uuid.UUID(data["org_id"])

    async with session_maker() as session:
        user_result = await session.execute(select(User).where(User.id == user_id))
        user = user_result.scalar_one_or_none()

        org_result = await session.execute(select(Organization).where(Organization.id == org_id))
        org = org_result.scalar_one_or_none()

        membership_result = await session.execute(
            select(Membership).where(
                Membership.user_id == user_id,
                Membership.org_id == org_id,
            )
        )
        membership = membership_result.scalar_one_or_none()

    assert user is not None
    assert user.email == "journey1@example.com"
    assert org is not None
    assert membership is not None
    assert membership.role == "owner"


@pytest.mark.asyncio
async def test_register_then_login_returns_tokens(journey_app):
    ac, _, session_maker = journey_app

    email = "journey2@example.com"
    reg = await ac.post(
        "/api/v1/auth/register",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert reg.status_code == 201

    async with session_maker() as session:
        result = await session.execute(select(User).where(User.email == email))
        user = result.scalar_one()
        user.is_verified = True
        await session.commit()

    login = await ac.post(
        "/api/v1/auth/login",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert login.status_code == 200
    data = login.json()
    assert "access_token" in data
    assert "needs_onboarding" in data
    assert data["needs_onboarding"] is False


@pytest.mark.asyncio
async def test_full_journey_register_login_create_credential_create_sync_config(journey_app):
    ac, current_user, session_maker = journey_app

    email = "journey3@example.com"
    reg = await ac.post(
        "/api/v1/auth/register",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert reg.status_code == 201
    reg_data = reg.json()

    async with session_maker() as session:
        result = await session.execute(select(User).where(User.email == email))
        user = result.scalar_one()
        user.is_verified = True
        await session.commit()

    login = await ac.post(
        "/api/v1/auth/login",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert login.status_code == 200

    current_user["value"] = AuthenticatedUser(
        user_id=reg_data["user_id"],
        email=email,
        org_id=reg_data["org_id"],
        role="owner",
        is_superuser=False,
    )

    cred_resp = await ac.post(
        "/api/v1/admin/credentials",
        json={
            "provider": "github",
            "name": "default",
            "credentials": {"token": "ghp_test_token"},
        },
    )
    assert cred_resp.status_code == 200
    assert cred_resp.json()["provider"] == "github"

    sync_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={"name": "journey-sync", "provider": "github", "sync_targets": []},
    )
    assert sync_resp.status_code == 200
    config_id = sync_resp.json()["id"]

    mock_task = MagicMock(id="fake-task-id")
    mock_run = MagicMock()
    mock_run.delay.return_value = mock_task

    with patch("dev_health_ops.workers.tasks.run_sync_config", mock_run):
        trigger_resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert trigger_resp.status_code == 202
    assert trigger_resp.json()["status"] == "triggered"
    mock_run.delay.assert_called_once()


@pytest.mark.asyncio
async def test_full_journey_register_create_identity_and_team(journey_app):
    ac, current_user, session_maker = journey_app

    email = "journey4@example.com"
    reg = await ac.post(
        "/api/v1/auth/register",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert reg.status_code == 201
    reg_data = reg.json()

    async with session_maker() as session:
        result = await session.execute(select(User).where(User.email == email))
        user = result.scalar_one()
        user.is_verified = True
        await session.commit()

    current_user["value"] = AuthenticatedUser(
        user_id=reg_data["user_id"],
        email=email,
        org_id=reg_data["org_id"],
        role="owner",
        is_superuser=False,
    )

    identity_resp = await ac.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": "alice@example.com",
            "display_name": "Alice Smith",
            "email": "alice@example.com",
            "provider_identities": {"github": ["alice-gh"]},
            "team_ids": [],
        },
    )
    assert identity_resp.status_code == 200

    team_resp = await ac.post(
        "/api/v1/admin/teams",
        json={
            "team_id": "backend-team",
            "name": "Backend Team",
            "repo_patterns": [],
            "project_keys": [],
        },
    )
    assert team_resp.status_code == 200

    async with session_maker() as session:
        identity_result = await session.execute(
            select(IdentityMapping).where(
                IdentityMapping.canonical_id == "alice@example.com",
                IdentityMapping.org_id == reg_data["org_id"],
            )
        )
        identity = identity_result.scalar_one_or_none()

        team_result = await session.execute(
            select(TeamMapping).where(
                TeamMapping.team_id == "backend-team",
                TeamMapping.org_id == reg_data["org_id"],
            )
        )
        team = team_result.scalar_one_or_none()

    assert identity is not None
    assert identity.display_name == "Alice Smith"
    assert team is not None
    assert team.name == "Backend Team"


@pytest.mark.asyncio
async def test_register_duplicate_email_rejected(journey_app):
    ac, _, session_maker = journey_app

    email = "journey5@example.com"
    first = await ac.post(
        "/api/v1/auth/register",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert first.status_code == 201

    second = await ac.post(
        "/api/v1/auth/register",
        json={"email": email, "password": VALID_PASSWORD},
    )
    assert second.status_code == 400

    async with session_maker() as session:
        result = await session.execute(select(User).where(User.email == email))
        users = result.scalars().all()

    assert len(users) == 1
