from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthService
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

SECRET = "onboarding-state-test-secret-32-chars"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "onboarding-state.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    User,
                    Organization,
                    Membership,
                    IntegrationCredential,
                ),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker):
    app = FastAPI()
    app.include_router(auth_router_module.router)

    @asynccontextmanager
    async def _session_override():
        async with session_maker() as session:
            yield session

    auth_service = AuthService(secret_key=SECRET)
    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(auth_router_module, "get_auth_service", lambda: auth_service)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, auth_service


def _access_token(
    auth_service: AuthService,
    user: User,
    org_id: str = "",
    role: str = "member",
) -> str:
    return auth_service.create_access_token(
        user_id=str(user.id),
        email=str(user.email),
        org_id=org_id,
        role=role,
        token_version=int(user.token_version or 0),
    )


async def _seed_user(
    session_maker,
    *,
    email: str = "user@example.com",
    verified: bool = True,
    superuser: bool = False,
) -> User:
    user = User(
        id=uuid.uuid4(),
        email=email,
        is_active=True,
        is_verified=verified,
        is_superuser=superuser,
    )
    async with session_maker() as session:
        session.add(user)
        await session.commit()
    return user


async def _seed_org_membership(
    session_maker,
    user: User,
    *,
    role: str = "owner",
    skipped: bool = False,
) -> Organization:
    org = Organization(
        id=uuid.uuid4(),
        slug=f"org-{uuid.uuid4().hex[:8]}",
        name="First Workspace",
    )
    if skipped:
        org.onboarding_integration_skipped_at = datetime.now(timezone.utc)
    membership = Membership(org_id=org.id, user_id=user.id, role=role)
    async with session_maker() as session:
        session.add_all([org, membership])
        await session.commit()
    return org


@pytest.mark.asyncio
async def test_state_orgless_verified_user_routes_to_workspace(client, session_maker):
    async_client, auth_service = client
    user = await _seed_user(session_maker)
    token = _access_token(auth_service, user)

    response = await async_client.get(
        "/api/v1/auth/onboarding/state",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "needs_onboarding": True,
        "org_created": False,
        "org_id": None,
        "org_name": None,
        "first_integration_connected": False,
        "integration_skipped": False,
        "recommended_provider": "github",
        "next_step": "workspace",
        "blocker": None,
    }


@pytest.mark.asyncio
async def test_state_membership_without_integration_routes_to_integration(
    client, session_maker
):
    async_client, auth_service = client
    user = await _seed_user(session_maker)
    org = await _seed_org_membership(session_maker, user)
    token = _access_token(auth_service, user, org_id=str(org.id), role="owner")

    response = await async_client.get(
        "/api/v1/auth/onboarding/state",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["needs_onboarding"] is True
    assert data["org_created"] is True
    assert data["org_id"] == str(org.id)
    assert data["org_name"] == "First Workspace"
    assert data["first_integration_connected"] is False
    assert data["integration_skipped"] is False
    assert data["next_step"] == "integration"


@pytest.mark.asyncio
async def test_state_connected_integration_routes_to_complete(client, session_maker):
    async_client, auth_service = client
    user = await _seed_user(session_maker)
    org = await _seed_org_membership(session_maker, user)
    async with session_maker() as session:
        session.add(
            IntegrationCredential(
                org_id=str(org.id),
                provider="github",
                name="default",
                is_active=True,
            )
        )
        await session.commit()
    token = _access_token(auth_service, user, org_id=str(org.id), role="owner")

    response = await async_client.get(
        "/api/v1/auth/onboarding/state",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["needs_onboarding"] is False
    assert data["first_integration_connected"] is True
    assert data["integration_skipped"] is False
    assert data["next_step"] == "complete"


@pytest.mark.asyncio
async def test_state_skipped_integration_routes_to_complete(client, session_maker):
    async_client, auth_service = client
    user = await _seed_user(session_maker)
    org = Organization(
        id=uuid.uuid4(),
        slug="skipped",
        name="Skipped Workspace",
        onboarding_integration_skipped_at=datetime.now(timezone.utc),
    )
    async with session_maker() as session:
        session.add_all([org, Membership(org_id=org.id, user_id=user.id, role="owner")])
        await session.commit()
    token = _access_token(auth_service, user, org_id=str(org.id), role="owner")

    response = await async_client.get(
        "/api/v1/auth/onboarding/state",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["needs_onboarding"] is False
    assert data["integration_skipped"] is True
    assert data["next_step"] == "complete"


@pytest.mark.asyncio
async def test_state_admin_routes_to_dashboard(client, session_maker):
    async_client, auth_service = client
    user = await _seed_user(session_maker)
    org = await _seed_org_membership(session_maker, user, role="admin")
    token = _access_token(auth_service, user, org_id=str(org.id), role="admin")

    response = await async_client.get(
        "/api/v1/auth/onboarding/state",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["needs_onboarding"] is False
    assert data["next_step"] == "dashboard"


@pytest.mark.asyncio
async def test_state_unverified_user_rejected(client, session_maker):
    async_client, auth_service = client
    user = await _seed_user(session_maker, verified=False)
    token = _access_token(auth_service, user)

    response = await async_client.get(
        "/api/v1/auth/onboarding/state",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "email_unverified"}
