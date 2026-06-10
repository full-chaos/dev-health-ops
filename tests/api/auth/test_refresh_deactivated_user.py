"""Deactivated-user refresh rejection tests (CHAOS-2238).

Root cause: /api/v1/auth/refresh loaded the User row but never checked
``User.is_active``, so a deactivated user could keep rotating refresh tokens
and minting fresh access tokens indefinitely.  get_current_user blocks API
use downstream, but token issuance itself must also be cut off.

Tests in this module:
- deactivated_user_refresh_rejected: deactivated user presents a valid
  refresh token → 401 "Account is disabled", the token family is revoked,
  and an audit log entry is recorded.
- deactivated_user_token_unusable_after_rejection: after the rejection the
  same token (and family) stays dead — replaying it returns 401.
- deactivated_user_grace_window_replay_rejected: a token rotated while the
  user was active cannot be replayed through the concurrent-rotation grace
  window once the user is deactivated.
- active_user_refresh_unaffected: regression guard — normal rotation still
  returns 200 with a usable successor.
"""

from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import bcrypt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.api.services.auth import AuthService
from dev_health_ops.api.services.refresh_tokens import (
    create_refresh_token as db_create_refresh_token,
)
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import LoginAttempt, Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

JWT_SECRET = "test-secret-key-that-is-long-enough-32+"
AUTH_SERVICE = AuthService(secret_key=JWT_SECRET)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "refresh-deactivated.db"
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
async def seed(session_maker):
    """Create one active user + org + membership in the DB."""
    password_hash = bcrypt.hashpw(b"Pass123!", bcrypt.gensalt()).decode()
    user = User(
        id=uuid.uuid4(),
        email="deactivated@example.com",
        username="deactivated",
        password_hash=password_hash,
        is_active=True,
        is_verified=True,
    )
    org = Organization(
        id=uuid.uuid4(), slug="deact-org", name="Deact Org", tier="community"
    )
    async with session_maker() as session:
        session.add_all([user, org])
        await session.flush()
        session.add(
            Membership(
                user_id=user.id,
                org_id=org.id,
                role="owner",
                joined_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    return {
        "user_id": str(user.id),
        "email": str(user.email),
        "org_id": str(org.id),
    }


async def _mint_and_store_token(
    session_maker, seed: dict, *, family_id: str | None = None
) -> tuple[str, str]:
    """Mint a fresh refresh JWT and persist it to the DB.

    Returns (jwt_string, jti_string).
    """
    fid = family_id or str(uuid.uuid4())
    refresh_jwt = AUTH_SERVICE.create_refresh_token(
        user_id=seed["user_id"],
        org_id=seed["org_id"],
        family_id=fid,
    )
    payload = AUTH_SERVICE.validate_token(refresh_jwt, token_type="refresh")
    assert payload is not None
    jti = str(payload["jti"])
    exp = datetime.fromtimestamp(float(payload["exp"]), tz=timezone.utc)

    async with session_maker() as session:
        await db_create_refresh_token(
            db=session,
            user_id=seed["user_id"],
            org_id=seed["org_id"],
            token_hash=jti,
            family_id=fid,
            expires_at=exp,
        )
        await session.commit()

    return refresh_jwt, jti


async def _deactivate_user(session_maker, seed: dict) -> None:
    async with session_maker() as session:
        await session.execute(
            update(User)
            .where(User.id == uuid.UUID(seed["user_id"]))
            .values(is_active=False)
        )
        await session.commit()


async def _active_token_count(session_maker, family_id: str) -> int:
    async with session_maker() as session:
        result = await session.execute(
            select(func.count())
            .select_from(RefreshToken)
            .where(
                RefreshToken.family_id == uuid.UUID(family_id),
                RefreshToken.revoked_at.is_(None),
            )
        )
        return int(result.scalar_one())


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker):
    """FastAPI test client wired to the in-memory SQLite DB."""
    app = FastAPI()
    app.include_router(auth_router_module.router)

    app.add_exception_handler(
        RequestValidationError,
        lambda req, exc: JSONResponse(
            status_code=422,
            content={"detail": {"message": "Validation failed", "errors": []}},
        ),
    )

    @asynccontextmanager
    async def _session_override():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(
        auth_router_module,
        "get_auth_service",
        lambda: AUTH_SERVICE,
    )
    monkeypatch.setattr(rate_limiter, "enabled", False)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_deactivated_user_refresh_rejected(client, session_maker, seed):
    """A deactivated user's refresh must 401, revoke the family, and audit."""
    family_id = str(uuid.uuid4())
    refresh_jwt, _ = await _mint_and_store_token(
        session_maker, seed, family_id=family_id
    )
    await _deactivate_user(session_maker, seed)

    res = await client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_jwt})
    assert res.status_code == 401, f"expected 401, got {res.status_code} {res.text}"
    assert "Account is disabled" in res.text

    assert await _active_token_count(session_maker, family_id) == 0, (
        "Token family must be fully revoked when a deactivated user refreshes"
    )

    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog).where(
                AuditLog.description == "Token refresh failed: user deactivated"
            )
        )
        assert result.scalars().first() is not None, (
            "Audit log entry expected for deactivated-user refresh rejection"
        )


@pytest.mark.asyncio
async def test_deactivated_user_token_unusable_after_rejection(
    client, session_maker, seed
):
    """The revoked family stays dead on subsequent presentations."""
    family_id = str(uuid.uuid4())
    refresh_jwt, _ = await _mint_and_store_token(
        session_maker, seed, family_id=family_id
    )
    await _deactivate_user(session_maker, seed)

    first = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": refresh_jwt}
    )
    assert first.status_code == 401

    replay = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": refresh_jwt}
    )
    assert replay.status_code == 401, (
        "Replaying the token after deactivation rejection must stay 401"
    )
    assert await _active_token_count(session_maker, family_id) == 0


@pytest.mark.asyncio
async def test_deactivated_user_grace_window_replay_rejected(
    client, session_maker, seed
):
    """The concurrent-rotation grace window must not re-issue tokens to a
    user deactivated after their last successful rotation."""
    family_id = str(uuid.uuid4())
    refresh_jwt, _ = await _mint_and_store_token(
        session_maker, seed, family_id=family_id
    )

    # Rotate T1 → T2 while still active.
    rotated = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": refresh_jwt}
    )
    assert rotated.status_code == 200, (
        f"Active-user rotation should succeed: {rotated.status_code} {rotated.text}"
    )

    await _deactivate_user(session_maker, seed)

    # Present stale T1 within the grace window — must NOT replay the successor.
    replay = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": refresh_jwt}
    )
    assert replay.status_code == 401, (
        "Grace-window replay must be rejected for a deactivated user, got "
        f"{replay.status_code} {replay.text}"
    )
    assert "Account is disabled" in replay.text
    assert await _active_token_count(session_maker, family_id) == 0


@pytest.mark.asyncio
async def test_active_user_refresh_unaffected(client, session_maker, seed):
    """Regression guard: active users rotate normally."""
    family_id = str(uuid.uuid4())
    refresh_jwt, _ = await _mint_and_store_token(
        session_maker, seed, family_id=family_id
    )

    res = await client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_jwt})
    assert res.status_code == 200, f"{res.status_code} {res.text}"
    body = res.json()
    assert body["access_token"]
    assert body["refresh_token"]

    # Successor is usable.
    res2 = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": body["refresh_token"]}
    )
    assert res2.status_code == 200, f"{res2.status_code} {res2.text}"
    assert await _active_token_count(session_maker, family_id) == 1
