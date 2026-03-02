from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import bcrypt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.password_reset import create_password_reset_token
from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.password_reset_token import PasswordResetToken
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import Membership, Organization, User

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
password_reset_module = importlib.import_module(
    "dev_health_ops.api.services.password_reset"
)

KNOWN_PASSWORD = "OldPassword@123"
KNOWN_PASSWORD_HASH = bcrypt.hashpw(
    KNOWN_PASSWORD.encode("utf-8"), bcrypt.gensalt()
).decode("utf-8")
GENERIC_MSG = "If the account exists, a password reset email has been sent"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "password-reset.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
                    AuditLog.__table__,
                    RefreshToken.__table__,
                    PasswordResetToken.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_user(session_maker):
    user = User(
        id=uuid.uuid4(),
        email="resetuser@example.com",
        password_hash=KNOWN_PASSWORD_HASH,
        is_active=True,
        is_verified=True,
    )
    async with session_maker() as session:
        session.add(user)
        await session.commit()
    return user


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker, seeded_user):
    app = FastAPI()
    app.include_router(auth_router_module.router)

    @asynccontextmanager
    async def _session_override():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(rate_limiter, "enabled", False)

    mock_send_email = AsyncMock()
    monkeypatch.setattr(
        password_reset_module, "send_password_reset_email", mock_send_email
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, mock_send_email

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_forgot_password_known_email_returns_generic_message(client, seeded_user):
    async_client, _ = client
    response = await async_client.post(
        "/api/v1/auth/forgot-password",
        json={"email": seeded_user.email},
    )
    assert response.status_code == 200
    assert response.json()["message"] == GENERIC_MSG


@pytest.mark.asyncio
async def test_forgot_password_unknown_email_returns_same_message(client):
    async_client, _ = client
    response = await async_client.post(
        "/api/v1/auth/forgot-password",
        json={"email": "nobody@example.com"},
    )
    assert response.status_code == 200
    assert response.json()["message"] == GENERIC_MSG


@pytest.mark.asyncio
async def test_forgot_password_creates_token_in_db(client, session_maker, seeded_user):
    async_client, _ = client
    await async_client.post(
        "/api/v1/auth/forgot-password",
        json={"email": seeded_user.email},
    )
    async with session_maker() as session:
        result = await session.execute(
            select(PasswordResetToken).where(
                PasswordResetToken.user_id == seeded_user.id
            )
        )
        token_record = result.scalar_one_or_none()
    assert token_record is not None


@pytest.mark.asyncio
async def test_forgot_password_email_send_failure_does_not_expose_error(
    client, seeded_user
):
    async_client, mock_send_email = client
    mock_send_email.side_effect = Exception("SMTP connection refused")
    response = await async_client.post(
        "/api/v1/auth/forgot-password",
        json={"email": seeded_user.email},
    )
    assert response.status_code == 200
    assert response.json()["message"] == GENERIC_MSG


@pytest.mark.asyncio
async def test_reset_password_valid_token_succeeds(client, session_maker, seeded_user):
    async_client, _ = client
    async with session_maker() as session:
        token = await create_password_reset_token(session, seeded_user.id)
        await session.commit()
    response = await async_client.post(
        "/api/v1/auth/reset-password",
        json={"token": token, "new_password": "NewPassword@456"},
    )
    assert response.status_code == 200
    assert response.json()["message"] == "Password reset successful"


@pytest.mark.asyncio
async def test_reset_password_valid_token_updates_password_hash(
    client, session_maker, seeded_user
):
    async_client, _ = client
    new_password = "NewPassword@456"
    async with session_maker() as session:
        token = await create_password_reset_token(session, seeded_user.id)
        await session.commit()
    await async_client.post(
        "/api/v1/auth/reset-password",
        json={"token": token, "new_password": new_password},
    )
    async with session_maker() as session:
        result = await session.execute(select(User).where(User.id == seeded_user.id))
        updated_user = result.scalar_one()
    assert bcrypt.checkpw(
        new_password.encode("utf-8"),
        updated_user.password_hash.encode("utf-8"),
    )


@pytest.mark.asyncio
async def test_reset_password_invalid_token_returns_400(client):
    async_client, _ = client
    response = await async_client.post(
        "/api/v1/auth/reset-password",
        json={"token": "not-a-valid-token", "new_password": "NewPass@123"},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid or expired token"


@pytest.mark.asyncio
async def test_reset_password_expired_token_returns_400(
    client, session_maker, seeded_user
):
    async_client, _ = client
    async with session_maker() as session:
        token = await create_password_reset_token(session, seeded_user.id)
        await session.execute(
            update(PasswordResetToken)
            .where(PasswordResetToken.user_id == seeded_user.id)
            .values(expires_at=datetime.now(timezone.utc) - timedelta(hours=1))
        )
        await session.commit()
    response = await async_client.post(
        "/api/v1/auth/reset-password",
        json={"token": token, "new_password": "NewPass@123"},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid or expired token"
