from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser, AuthService
from dev_health_ops.api.services.invites import create_invite
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.org_invite import OrgInvite
from dev_health_ops.models.users import Membership, Organization, User

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
admin_router_module = importlib.import_module("dev_health_ops.api.admin.routers.orgs")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "invite-flow.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
                    OrgInvite.__table__,
                    AuditLog.__table__,
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
    org = Organization(id=uuid.uuid4(), slug="acme", name="Acme")
    owner = User(id=uuid.uuid4(), email="owner@example.com", is_active=True)
    member = User(id=uuid.uuid4(), email="member@example.com", is_active=True)
    invitee = User(id=uuid.uuid4(), email="invitee@example.com", is_active=True)

    async with session_maker() as session:
        session.add_all([org, owner, member, invitee])
        session.add_all(
            [
                Membership(org_id=org.id, user_id=owner.id, role="owner"),
                Membership(org_id=org.id, user_id=member.id, role="member"),
            ]
        )
        await session.commit()

    return {
        "org_id": str(org.id),
        "owner_id": str(owner.id),
        "member_id": str(member.id),
        "invitee_id": str(invitee.id),
        "owner_email": str(owner.email),
        "member_email": str(member.email),
        "invitee_email": str(invitee.email),
    }


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)
    app.include_router(auth_router_module.router)

    current_user = {
        "value": AuthenticatedUser(
            user_id=seeded_state["owner_id"],
            email=seeded_state["owner_email"],
            org_id=seeded_state["org_id"],
            role="owner",
            is_superuser=False,
        )
    }

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

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: (
        current_user["value"]
    )
    app.dependency_overrides[admin_router_module.get_session] = _admin_session_override

    monkeypatch.setattr(
        auth_router_module, "get_postgres_session", _auth_session_override
    )
    monkeypatch.setattr(
        auth_router_module,
        "get_auth_service",
        lambda: AuthService(secret_key="invite-flow-test-secret"),
    )
    monkeypatch.setattr(
        auth_router_module, "create_refresh_token_record", _noop_refresh
    )
    monkeypatch.setattr(admin_router_module, "send_invite_email", AsyncMock())

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, current_user

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_create_invite_admin_can_member_cannot(client, seeded_state):
    async_client, current_user = client

    current_user["value"] = AuthenticatedUser(
        user_id=seeded_state["owner_id"],
        email=seeded_state["owner_email"],
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )
    ok_response = await async_client.post(
        f"/api/v1/admin/orgs/{seeded_state['org_id']}/invites",
        json={"email": "new-user@example.com", "role": "member"},
    )

    assert ok_response.status_code == 201
    assert ok_response.json()["status"] == "pending"

    current_user["value"] = AuthenticatedUser(
        user_id=seeded_state["member_id"],
        email=seeded_state["member_email"],
        org_id=seeded_state["org_id"],
        role="member",
        is_superuser=False,
    )
    denied_response = await async_client.post(
        f"/api/v1/admin/orgs/{seeded_state['org_id']}/invites",
        json={"email": "member-denied@example.com", "role": "member"},
    )

    assert denied_response.status_code == 403


@pytest.mark.asyncio
async def test_accept_invite_creates_membership(client, session_maker, seeded_state):
    async_client, current_user = client

    async with session_maker() as session:
        _, token = await create_invite(
            db=session,
            org_id=uuid.UUID(seeded_state["org_id"]),
            email=seeded_state["invitee_email"],
            role="member",
            invited_by_id=uuid.UUID(seeded_state["owner_id"]),
        )
        await session.commit()

    current_user["value"] = AuthenticatedUser(
        user_id=seeded_state["invitee_id"],
        email=seeded_state["invitee_email"],
        org_id="",
        role="member",
        is_superuser=False,
    )
    response = await async_client.post(
        "/api/v1/auth/accept-invite",
        json={"token": token},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == seeded_state["org_id"]
    assert data["role"] == "member"
    assert data["access_token"]
    assert data["refresh_token"]

    async with session_maker() as session:
        membership_result = await session.execute(
            select(Membership).where(
                Membership.org_id == uuid.UUID(seeded_state["org_id"]),
                Membership.user_id == uuid.UUID(seeded_state["invitee_id"]),
            )
        )
        membership = membership_result.scalar_one_or_none()
        assert membership is not None


@pytest.mark.asyncio
async def test_expired_invite_rejected(client, session_maker, seeded_state):
    async_client, current_user = client

    async with session_maker() as session:
        _, token = await create_invite(
            db=session,
            org_id=uuid.UUID(seeded_state["org_id"]),
            email=seeded_state["invitee_email"],
            role="member",
            invited_by_id=uuid.UUID(seeded_state["owner_id"]),
            ttl_hours=-1,
        )
        await session.commit()

    current_user["value"] = AuthenticatedUser(
        user_id=seeded_state["invitee_id"],
        email=seeded_state["invitee_email"],
        org_id="",
        role="member",
        is_superuser=False,
    )
    response = await async_client.post(
        "/api/v1/auth/accept-invite",
        json={"token": token},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["message"] == "Invalid or expired invite"


@pytest.mark.asyncio
async def test_duplicate_invite_for_same_email_org_rejected(client, seeded_state):
    async_client, current_user = client

    current_user["value"] = AuthenticatedUser(
        user_id=seeded_state["owner_id"],
        email=seeded_state["owner_email"],
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )
    first = await async_client.post(
        f"/api/v1/admin/orgs/{seeded_state['org_id']}/invites",
        json={"email": "dup@example.com", "role": "member"},
    )
    second = await async_client.post(
        f"/api/v1/admin/orgs/{seeded_state['org_id']}/invites",
        json={"email": "dup@example.com", "role": "member"},
    )

    assert first.status_code == 201
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_accepting_as_already_member_returns_error(
    client, session_maker, seeded_state
):
    async_client, current_user = client

    async with session_maker() as session:
        _, token = await create_invite(
            db=session,
            org_id=uuid.UUID(seeded_state["org_id"]),
            email=seeded_state["owner_email"],
            role="member",
            invited_by_id=uuid.UUID(seeded_state["owner_id"]),
        )
        await session.commit()

    current_user["value"] = AuthenticatedUser(
        user_id=seeded_state["owner_id"],
        email=seeded_state["owner_email"],
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )
    response = await async_client.post(
        "/api/v1/auth/accept-invite",
        json={"token": token},
    )

    assert response.status_code == 400
    assert (
        response.json()["detail"]["message"]
        == "User is already a member of this organization"
    )
