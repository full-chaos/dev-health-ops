from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.routers.dependencies import get_current_user
from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.api.services.auth import AuthenticatedUser, AuthService
from dev_health_ops.api.services.users import UserService
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "token-version-auth.db"
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
                    RefreshToken,
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
    user = User(
        id=uuid.uuid4(),
        email="tv@example.com",
        password_hash="old-hash",
        is_active=True,
        is_verified=True,
        token_version=3,
    )
    org = Organization(id=uuid.uuid4(), slug="tv-org", name="Token Version Org")
    membership = Membership(user_id=user.id, org_id=org.id, role="admin")
    async with session_maker() as session:
        session.add_all([user, org, membership])
        await session.commit()
    return {"user_id": user.id, "org_id": org.id, "email": str(user.email)}


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker):
    app = FastAPI()
    app.include_router(auth_router_module.router)

    @app.get("/protected")
    async def protected(
        user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    ) -> dict[str, str]:
        return {"user_id": user.user_id}

    @asynccontextmanager
    async def _session_override():
        async with session_maker() as session:
            yield session

    auth_service = AuthService(secret_key="token-version-test-secret-key-123456")
    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(auth_router_module, "get_auth_service", lambda: auth_service)
    monkeypatch.setattr(rate_limiter, "enabled", False)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, auth_service

    app.dependency_overrides.clear()


def _issue_access_token(
    auth_service: AuthService,
    seeded_state: dict[str, object],
    token_version: int,
) -> str:
    return auth_service.create_access_token(
        user_id=str(seeded_state["user_id"]),
        email=str(seeded_state["email"]),
        org_id=str(seeded_state["org_id"]),
        role="admin",
        token_version=token_version,
    )


@pytest.mark.asyncio
async def test_token_with_current_token_version_validates(client, seeded_state):
    async_client, auth_service = client
    token = _issue_access_token(auth_service, seeded_state, token_version=3)

    payload = auth_service.validate_token(token)
    assert payload is not None
    assert payload["tv"] == 3

    response = await async_client.post("/api/v1/auth/validate", json={"token": token})

    assert response.status_code == 200
    assert response.json()["valid"] is True


@pytest.mark.asyncio
async def test_stale_token_version_invalidates_validate_and_protected_route(
    client,
    session_maker,
    seeded_state,
):
    async_client, auth_service = client
    token = _issue_access_token(auth_service, seeded_state, token_version=3)
    async with session_maker() as session:
        user = await session.get(User, seeded_state["user_id"])
        assert user is not None
        user.token_version = 4
        await session.commit()

    validate_response = await async_client.post(
        "/api/v1/auth/validate", json={"token": token}
    )
    protected_response = await async_client.get(
        "/protected", headers={"Authorization": f"Bearer {token}"}
    )

    assert validate_response.status_code == 200
    assert validate_response.json() == {
        "valid": False,
        "user_id": None,
        "email": None,
        "org_id": None,
        "role": None,
        "expires_at": None,
    }
    assert protected_response.status_code == 401


@pytest.mark.asyncio
async def test_set_password_increments_token_version(session_maker, seeded_state):
    async with session_maker() as session:
        svc = UserService(session)
        success = await svc.set_password(
            str(seeded_state["user_id"]), "NewPassword@123"
        )
        await session.commit()

    async with session_maker() as session:
        result = await session.execute(
            select(User).where(User.id == seeded_state["user_id"])
        )
        user = result.scalar_one()

    assert success is True
    assert user.token_version == 4
