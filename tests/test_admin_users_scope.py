from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.admin.router import get_session
from dev_health_ops.api.main import app
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Membership, Organization, User


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "admin-users-scope.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
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
    org_a = Organization(id=uuid.uuid4(), slug="org-a", name="Org A")
    org_b = Organization(id=uuid.uuid4(), slug="org-b", name="Org B")

    user_a = User(
        id=uuid.uuid4(),
        email="alice@example.com",
        username="alice",
        full_name="Alice Alpha",
        is_active=True,
    )
    user_b = User(
        id=uuid.uuid4(),
        email="bob@example.com",
        username="bob",
        full_name="Bob Beta",
        is_active=True,
    )
    user_c = User(
        id=uuid.uuid4(),
        email="charlie@example.com",
        username="charlie",
        full_name="Charlie Gamma",
        is_active=False,
    )

    async with session_maker() as session:
        session.add_all([org_a, org_b, user_a, user_b, user_c])
        session.add_all(
            [
                Membership(org_id=org_a.id, user_id=user_a.id, role="owner"),
                Membership(org_id=org_b.id, user_id=user_b.id, role="member"),
                Membership(org_id=org_a.id, user_id=user_c.id, role="member"),
            ]
        )
        await session.commit()

    return {
        "org_a": str(org_a.id),
        "org_b": str(org_b.id),
        "user_a": str(user_a.id),
        "user_b": str(user_b.id),
        "user_c": str(user_c.id),
    }


@pytest_asyncio.fixture
async def client(session_maker):
    current_user = {
        "value": AuthenticatedUser(
            user_id="admin-user",
            email="admin@example.com",
            org_id="",
            role="owner",
            is_superuser=False,
        )
    }

    async def _override_get_session():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_session] = _override_get_session
    app.dependency_overrides[require_admin] = lambda: current_user["value"]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, current_user

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_superadmin_users_list_is_global(client, seeded_state):
    async_client, current_user = client
    current_user["value"] = AuthenticatedUser(
        user_id="super-user",
        email="super@example.com",
        org_id=seeded_state["org_a"],
        role="owner",
        is_superuser=True,
    )

    response = await async_client.get("/api/v1/admin/users")

    assert response.status_code == 200
    body = response.json()
    emails = {row["email"] for row in body}
    assert emails == {"alice@example.com", "bob@example.com"}


@pytest.mark.asyncio
async def test_org_admin_users_list_stays_org_scoped(client, seeded_state):
    async_client, current_user = client
    current_user["value"] = AuthenticatedUser(
        user_id="org-admin",
        email="org-admin@example.com",
        org_id=seeded_state["org_a"],
        role="owner",
        is_superuser=False,
    )

    response = await async_client.get("/api/v1/admin/users")

    assert response.status_code == 200
    body = response.json()
    emails = {row["email"] for row in body}
    assert emails == {"alice@example.com"}


@pytest.mark.asyncio
async def test_users_search_honors_scope_for_superadmin_and_org_admin(
    client, seeded_state
):
    async_client, current_user = client

    current_user["value"] = AuthenticatedUser(
        user_id="super-user",
        email="super@example.com",
        org_id=seeded_state["org_a"],
        role="owner",
        is_superuser=True,
    )
    super_response = await async_client.get("/api/v1/admin/users?q=bob")
    assert super_response.status_code == 200
    assert [row["email"] for row in super_response.json()] == ["bob@example.com"]

    current_user["value"] = AuthenticatedUser(
        user_id="org-admin",
        email="org-admin@example.com",
        org_id=seeded_state["org_a"],
        role="owner",
        is_superuser=False,
    )
    org_response = await async_client.get("/api/v1/admin/users?q=bob")
    assert org_response.status_code == 200
    assert org_response.json() == []
