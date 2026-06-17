"""CHAOS-2498: login-failure audit + lockout-counter persistence.

The login failure paths call emit_audit_log() (db.add) and record_failed_attempt()
(db.flush) and then `raise HTTPException(...)`. Because get_postgres_session()
rolls back on exception, both the LOGIN_FAILED audit row and the failed-attempt
counter were being discarded on the raise. The fix commits before raising.

These tests use a session override that MIRRORS production rollback-on-exception
semantics (commit on clean exit, rollback on exception); without that, the bug
would be invisible to the test because the bare session would not roll back.
"""

from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.api.services.login_attempts import LOCKOUT_FAILURE_THRESHOLD
from dev_health_ops.models.audit import AuditAction, AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import LoginAttempt, Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User, Organization, Membership, AuditLog, LoginAttempt, RefreshToken
)

KNOWN_PASSWORD = "OldPassword@123"
# nosemgrep: generic.secrets.security.detected-bcrypt-hash.detected-bcrypt-hash
KNOWN_PASSWORD_HASH = "$2b$04$tgxalfE5Q58OGJE/0M0piOakqY90AzLsIFaz178yu6eMEkjMuYeJe"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "login-audit.db"
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
async def seeded_user(session_maker):
    user = User(
        id=uuid.uuid4(),
        email="loginuser@example.com",
        password_hash=KNOWN_PASSWORD_HASH,
        is_active=True,
        is_verified=True,
    )
    org = Organization(id=uuid.uuid4(), slug="login-org", name="Login Org")
    membership = Membership(user_id=user.id, org_id=org.id, role="member")
    async with session_maker() as session:
        session.add_all([user, org, membership])
        await session.commit()
    return user


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker, seeded_user):
    app = FastAPI()
    app.include_router(auth_router_module.router)

    @asynccontextmanager
    async def _session_override():
        # Mirror production get_postgres_session: commit on success, rollback on
        # exception. This is what makes the failure-path audit/counter rollback
        # observable in the test.
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(rate_limiter, "enabled", False)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_failed_login_persists_audit_row(client, session_maker, seeded_user):
    resp = await client.post(
        "/api/v1/auth/login",
        json={"email": seeded_user.email, "password": "WrongPassword@999"},
    )
    assert resp.status_code == 401

    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog).where(AuditLog.action == AuditAction.LOGIN_FAILED.value)
        )
        audit_log = result.scalar_one()

    assert audit_log.status == "failure"
    assert audit_log.resource_id == str(seeded_user.id)


@pytest.mark.asyncio
async def test_failed_login_persists_attempt_counter(
    client, session_maker, seeded_user
):
    """The lockout counter must survive the raise — otherwise lockout never fires."""
    await client.post(
        "/api/v1/auth/login",
        json={"email": seeded_user.email, "password": "WrongPassword@999"},
    )

    async with session_maker() as session:
        result = await session.execute(
            select(LoginAttempt).where(
                func.lower(LoginAttempt.email) == seeded_user.email.lower()
            )
        )
        attempt = result.scalar_one()
    assert attempt.attempt_count == 1


@pytest.mark.asyncio
async def test_repeated_failures_accumulate_persisted_counter(
    client, session_maker, seeded_user
):
    """The counter accumulates across requests now that each failure commits.

    NOTE: we assert the persisted counter rather than driving an end-to-end 429.
    Reaching the lockout threshold makes check_lockout() read back
    LoginAttempt.locked_until, and aiosqlite returns it tz-naive (production
    Postgres keeps it tz-aware), which would raise on the naive/aware compare.
    That is a SQLite test-infra quirk, not a production code path.
    """
    for _ in range(LOCKOUT_FAILURE_THRESHOLD - 1):
        await client.post(
            "/api/v1/auth/login",
            json={"email": seeded_user.email, "password": "WrongPassword@999"},
        )

    async with session_maker() as session:
        result = await session.execute(
            select(LoginAttempt).where(
                func.lower(LoginAttempt.email) == seeded_user.email.lower()
            )
        )
        attempt = result.scalar_one()
    assert attempt.attempt_count == LOCKOUT_FAILURE_THRESHOLD - 1


@pytest.mark.asyncio
async def test_successful_login_after_failures_clears_counter(
    client, session_maker, seeded_user
):
    """A correct password clears the persisted failed-attempt counter."""
    await client.post(
        "/api/v1/auth/login",
        json={"email": seeded_user.email, "password": "WrongPassword@999"},
    )
    resp = await client.post(
        "/api/v1/auth/login",
        json={"email": seeded_user.email, "password": KNOWN_PASSWORD},
    )
    assert resp.status_code == 200

    async with session_maker() as session:
        count = (
            await session.execute(select(func.count()).select_from(LoginAttempt))
        ).scalar_one()
    assert count == 0


@pytest.mark.asyncio
async def test_wrong_org_after_valid_credentials_persists_clear_and_audit(
    client, session_maker, seeded_user
):
    """Valid password + non-member org → 401, but the cleared counter and a
    LOGIN_FAILED audit row must survive the raise (the auth-pushback path)."""
    other_org_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(Organization(id=other_org_id, slug="other-org", name="Other Org"))
        await session.commit()

    # Accumulate a failed attempt so there is a counter to clear.
    await client.post(
        "/api/v1/auth/login",
        json={"email": seeded_user.email, "password": "WrongPassword@999"},
    )

    # Correct password, but an org the user is not a member of.
    resp = await client.post(
        "/api/v1/auth/login",
        json={
            "email": seeded_user.email,
            "password": KNOWN_PASSWORD,
            "org_id": str(other_org_id),
        },
    )
    assert resp.status_code == 401
    assert "not a member" in resp.json()["detail"]["message"]

    async with session_maker() as session:
        # clear_attempts() ran on the valid-password check and must have persisted.
        attempt_count = (
            await session.execute(select(func.count()).select_from(LoginAttempt))
        ).scalar_one()
        audit = (
            (
                await session.execute(
                    select(AuditLog).where(
                        AuditLog.action == AuditAction.LOGIN_FAILED.value
                    )
                )
            )
            .scalars()
            .all()
        )
    assert attempt_count == 0
    assert any(
        a.status == "failure"
        and a.description == "Login failed: not a member of the selected organization"
        for a in audit
    )
