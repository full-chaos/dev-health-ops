from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthService
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

SECRET = "onboarding-skip-test-secret-32-chars"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "onboarding-skip.db"
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


@pytest_asyncio.fixture
async def seeded_org_user(session_maker):
    user = User(
        id=uuid.uuid4(),
        email="owner@example.com",
        is_active=True,
        is_verified=True,
    )
    org = Organization(id=uuid.uuid4(), slug="first", name="First Workspace")
    membership = Membership(org_id=org.id, user_id=user.id, role="owner")
    async with session_maker() as session:
        session.add_all([user, org, membership])
        await session.commit()
    return user, org


def _token(auth_service: AuthService, user: User, org: Organization) -> str:
    return auth_service.create_access_token(
        user_id=str(user.id),
        email=str(user.email),
        org_id=str(org.id),
        role="owner",
        token_version=int(user.token_version or 0),
    )


@pytest.mark.asyncio
async def test_skip_integration_persists_flag_and_returns_complete_state(
    client, session_maker, seeded_org_user
):
    async_client, auth_service = client
    user, org = seeded_org_user
    token = _token(auth_service, user, org)

    response = await async_client.post(
        "/api/v1/auth/onboarding/skip-integration",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == str(org.id)
    assert data["org_name"] == "First Workspace"
    assert data["first_integration_connected"] is False
    assert data["integration_skipped"] is True
    assert data["needs_onboarding"] is False
    assert data["next_step"] == "complete"

    async with session_maker() as session:
        db_org = await session.scalar(
            select(Organization).where(Organization.id == org.id)
        )

    assert db_org is not None
    assert db_org.onboarding_integration_skipped_at is not None


@pytest.mark.asyncio
async def test_skip_integration_is_idempotent(client, session_maker, seeded_org_user):
    async_client, auth_service = client
    user, org = seeded_org_user
    token = _token(auth_service, user, org)

    first = await async_client.post(
        "/api/v1/auth/onboarding/skip-integration",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    async with session_maker() as session:
        first_timestamp = await session.scalar(
            select(Organization.onboarding_integration_skipped_at).where(
                Organization.id == org.id
            )
        )

    second = await async_client.post(
        "/api/v1/auth/onboarding/skip-integration",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 200
    async with session_maker() as session:
        second_timestamp = await session.scalar(
            select(Organization.onboarding_integration_skipped_at).where(
                Organization.id == org.id
            )
        )

    assert first_timestamp is not None
    assert second_timestamp == first_timestamp
    assert second.json()["integration_skipped"] is True


@pytest.mark.asyncio
async def test_skip_integration_requires_membership(client, session_maker):
    async_client, auth_service = client
    user = User(
        id=uuid.uuid4(),
        email="outsider@example.com",
        is_active=True,
        is_verified=True,
    )
    org = Organization(id=uuid.uuid4(), slug="other", name="Other Workspace")
    async with session_maker() as session:
        session.add_all([user, org])
        await session.commit()
    token = auth_service.create_access_token(
        user_id=str(user.id),
        email=str(user.email),
        org_id=str(org.id),
        role="owner",
        token_version=int(user.token_version or 0),
    )

    response = await async_client.post(
        "/api/v1/auth/onboarding/skip-integration",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "organization_membership_required"}
    async with session_maker() as session:
        skipped_at = await session.scalar(
            select(Organization.onboarding_integration_skipped_at).where(
                Organization.id == org.id
            )
        )
    assert skipped_at is None
