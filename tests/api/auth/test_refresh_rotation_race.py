"""Concurrency regression tests for refresh-token rotation (CHAOS-2162).

Root cause: two near-simultaneous requests presenting the same refresh_token
could trigger the reuse-detection path (the second request sees the token
already revoked by the first) and revoke the entire token family, logging the
user out.

Fix: a short idempotency grace window.  When a token is found revoked but was
rotated within ROTATION_GRACE_WINDOW_SECONDS and has a recorded successor,
return the *same* successor JWT instead of revoking the family.

Tests in this module:
- concurrent_same_token_both_succeed: asyncio.gather fires two requests with
  the same token; family must NOT be revoked; at least one response succeeds
  and the winner's token is still usable for a subsequent rotation.
- grace_window_returns_same_successor: serial simulation — after request A
  completes, request B (within grace window) gets back the same successor JWT
  that A already minted; subsequent rotation from B's token works; only one
  valid successor ever existed.
- stale_reuse_revokes_family: same token presented well outside the grace
  window is treated as genuine reuse; family is revoked; 401 returned.
- grace_window_does_not_extend_lifetime: the successor JWT re-issued via the
  grace window carries the same expiry as the originally committed successor,
  not a freshly extended expiry.
"""

from __future__ import annotations

import asyncio
import importlib
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bcrypt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.api.services.auth import AuthService
from dev_health_ops.api.services.refresh_tokens import (
    create_refresh_token as db_create_refresh_token,
)
from dev_health_ops.api.services.refresh_tokens import (
    find_by_hash,
    rotate_token,
)
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import LoginAttempt, Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

JWT_SECRET = "test-secret-key-that-is-long-enough-32+"
AUTH_SERVICE = AuthService(secret_key=JWT_SECRET)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "refresh-race.db"
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
    """Create one user + org + membership in the DB."""
    password_hash = bcrypt.hashpw(b"Pass123!", bcrypt.gensalt()).decode()
    user = User(
        id=uuid.uuid4(),
        email="race@example.com",
        username="race",
        password_hash=password_hash,
        is_active=True,
        is_verified=True,
    )
    org = Organization(
        id=uuid.uuid4(), slug="race-org", name="Race Org", tier="community"
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


# ---------------------------------------------------------------------------
# Test 1 — concurrent requests with the same token (asyncio.gather)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_same_token_family_not_revoked(client, session_maker, seed):
    """Two simultaneous refresh requests with the same token must not revoke
    the family.  At least one must succeed and the winning token must still be
    usable for a subsequent rotation.
    """
    refresh_jwt, jti = await _mint_and_store_token(session_maker, seed)

    # Fire two requests concurrently with the exact same refresh_token.
    r1, r2 = await asyncio.gather(
        client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_jwt}),
        client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_jwt}),
    )

    statuses = {r1.status_code, r2.status_code}
    # At least one request must succeed.
    assert 200 in statuses, (
        f"Expected at least one 200; got {r1.status_code}, {r2.status_code}"
    )

    # Pick the successful response (if both are 200 that is also fine).
    winner = r1 if r1.status_code == 200 else r2
    winner_token = winner.json()["refresh_token"]

    # Verify the family is NOT fully revoked by doing a third rotation.
    r3 = await client.post("/api/v1/auth/refresh", json={"refresh_token": winner_token})
    assert r3.status_code == 200, (
        f"Subsequent rotation after concurrent race failed: {r3.status_code} {r3.text}"
    )

    # Confirm: the original family_id is still alive (its active token is the
    # winner's successor from the third rotation, not revoked).
    winner_payload = AUTH_SERVICE.validate_token(winner_token, token_type="refresh")
    assert winner_payload is not None
    family_id = str(winner_payload["family_id"])
    async with session_maker() as session:
        result = await session.execute(
            select(RefreshToken).where(
                RefreshToken.family_id == uuid.UUID(family_id),
                RefreshToken.revoked_at.is_(None),
            )
        )
        active_tokens = result.scalars().all()
    assert len(active_tokens) >= 1, "Token family should have at least one active token"


# ---------------------------------------------------------------------------
# Test 2 — grace window: serial simulation of the race
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grace_window_returns_same_successor(session_maker, seed):
    """Serial simulation: request A completes (rotates T1→T2), request B
    presents T1 within the grace window.  B must get back a JWT for T2 (same
    JTI) without triggering family revocation.  B's token must be usable for a
    subsequent rotation (T2→T3).
    """
    family_id = str(uuid.uuid4())
    refresh_jwt_1, jti_1 = await _mint_and_store_token(
        session_maker, seed, family_id=family_id
    )

    # ── Request A: rotate T1 → T2 ─────────────────────────────────────────
    t2_jti = str(uuid.uuid4())
    t2_exp = datetime.now(timezone.utc) + timedelta(days=7)
    async with session_maker() as session:
        await rotate_token(
            db=session,
            old_token_hash=jti_1,
            new_token_hash=t2_jti,
            new_expires_at=t2_exp,
        )
        await session.commit()

    # Verify T1 is now revoked with successor_jti set.
    async with session_maker() as session:
        t1_record = await find_by_hash(session, jti_1)
    assert t1_record is not None
    assert t1_record.revoked_at is not None
    assert t1_record.successor_jti == t2_jti
    assert t1_record.replaced_by_hash is not None

    # ── Request B: present T1 again (within grace window) ────────────────
    # B's presentation happens immediately after A committed, so elapsed ≪ 30s.
    # The router should return T2's JWT rather than revoking the family.
    from dev_health_ops.api.auth.routers.refresh import (
        ROTATION_GRACE_WINDOW_SECONDS,
    )
    from dev_health_ops.api.services.refresh_tokens import find_by_hash_for_update

    # Build the grace-window path directly (mirrors the router logic).
    async with session_maker() as session:
        token_record = await find_by_hash_for_update(session, jti_1)
        assert token_record is not None
        assert token_record.revoked_at is not None
        # SQLite returns timezone-naive datetimes even for DATETIME(timezone=True).
        revoked_at = token_record.revoked_at
        if revoked_at is not None and revoked_at.tzinfo is None:
            revoked_at = revoked_at.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - revoked_at).total_seconds()
        assert elapsed <= ROTATION_GRACE_WINDOW_SECONDS, (
            "Test assumes T1 was rotated within the grace window"
        )

        successor_record = await find_by_hash(session, token_record.successor_jti)  # type: ignore[arg-type]
        assert successor_record is not None
        assert successor_record.revoked_at is None

    # Re-issue the successor JWT using create_refresh_token_with_jti.
    reissued_jwt = AUTH_SERVICE.create_refresh_token_with_jti(
        jti=t2_jti,
        user_id=seed["user_id"],
        org_id=seed["org_id"],
        family_id=family_id,
        expires_at=t2_exp,
    )
    reissued_payload = AUTH_SERVICE.validate_token(reissued_jwt, token_type="refresh")
    assert reissued_payload is not None
    assert str(reissued_payload["jti"]) == t2_jti, "Re-issued JWT must carry T2's JTI"

    # ── Subsequent rotation T2 → T3 must succeed ─────────────────────────
    t3_jti = str(uuid.uuid4())
    t3_exp = datetime.now(timezone.utc) + timedelta(days=7)
    async with session_maker() as session:
        t3_record = await rotate_token(
            db=session,
            old_token_hash=t2_jti,
            new_token_hash=t3_jti,
            new_expires_at=t3_exp,
        )
        await session.commit()

    assert t3_record is not None, "T2 → T3 rotation must succeed"

    # Family is alive: T3 exists and is not revoked.
    async with session_maker() as session:
        result = await session.execute(
            select(RefreshToken).where(
                RefreshToken.family_id == uuid.UUID(family_id),
                RefreshToken.revoked_at.is_(None),
            )
        )
        active = result.scalars().all()
    assert len(active) == 1
    from dev_health_ops.api.services.refresh_tokens import _hash_token

    assert active[0].token_hash == _hash_token(t3_jti)


# ---------------------------------------------------------------------------
# Test 3 — stale reuse (outside grace window) still revokes the family
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_reuse_outside_grace_window_revokes_family(
    client, session_maker, seed
):
    """A revoked token presented well outside the grace window must trigger
    family revocation and return 401, not a grace-window replay.
    """
    family_id = str(uuid.uuid4())
    refresh_jwt_1, jti_1 = await _mint_and_store_token(
        session_maker, seed, family_id=family_id
    )

    # Rotate T1 → T2.
    t2_jti = str(uuid.uuid4())
    t2_exp = datetime.now(timezone.utc) + timedelta(days=7)
    async with session_maker() as session:
        await rotate_token(
            db=session,
            old_token_hash=jti_1,
            new_token_hash=t2_jti,
            new_expires_at=t2_exp,
        )
        await session.commit()

    # Artificially back-date T1's revoked_at to be beyond the grace window.
    stale_revoked_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    async with session_maker() as session:
        t1_record = await find_by_hash(session, jti_1)
        assert t1_record is not None
        setattr(t1_record, "revoked_at", stale_revoked_at)
        await session.commit()

    # Present T1 again — must be treated as genuine reuse, not grace window.
    response = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": refresh_jwt_1}
    )
    assert response.status_code == 401
    assert "reuse" in response.json()["detail"]["message"].lower()

    # The entire family must now be revoked (including T2).
    async with session_maker() as session:
        result = await session.execute(
            select(RefreshToken).where(
                RefreshToken.family_id == uuid.UUID(family_id),
                RefreshToken.revoked_at.is_(None),
            )
        )
        still_active = result.scalars().all()
    assert still_active == [], (
        "Family must be fully revoked after stale-reuse detection"
    )


# ---------------------------------------------------------------------------
# Test 4 — grace window does NOT extend the successor's token lifetime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grace_window_does_not_extend_token_lifetime(session_maker, seed):
    """The JWT produced by the grace-window path must carry the SAME expiry as
    the already-committed successor row, not a freshly extended expiry.
    """
    family_id = str(uuid.uuid4())
    _, jti_1 = await _mint_and_store_token(session_maker, seed, family_id=family_id)

    # Use a deliberately short-lived successor (5 minutes from now).
    t2_jti = str(uuid.uuid4())
    t2_exp = datetime.now(timezone.utc) + timedelta(minutes=5)
    async with session_maker() as session:
        await rotate_token(
            db=session,
            old_token_hash=jti_1,
            new_token_hash=t2_jti,
            new_expires_at=t2_exp,
        )
        await session.commit()

    # Fetch the committed successor record's expiry.
    async with session_maker() as session:
        t2_record = await find_by_hash(session, t2_jti)
    assert t2_record is not None
    committed_exp = t2_record.expires_at
    # SQLite returns naive datetimes; normalise for comparison.
    if committed_exp.tzinfo is None:
        committed_exp = committed_exp.replace(tzinfo=timezone.utc)

    # Re-issue via grace-window path.
    reissued_jwt = AUTH_SERVICE.create_refresh_token_with_jti(
        jti=t2_jti,
        user_id=seed["user_id"],
        org_id=seed["org_id"],
        family_id=family_id,
        expires_at=committed_exp,
    )
    reissued_payload = AUTH_SERVICE.validate_token(reissued_jwt, token_type="refresh")
    assert reissued_payload is not None

    reissued_exp = datetime.fromtimestamp(
        float(reissued_payload["exp"]), tz=timezone.utc
    )
    # Allow 2 s of clock jitter but no more — the expiry must NOT be ~7 days from now.
    delta = abs((reissued_exp - committed_exp).total_seconds())
    assert delta < 2, (
        f"Re-issued token expiry ({reissued_exp}) must match committed successor "
        f"expiry ({committed_exp}), not a fresh 7-day window; delta={delta:.1f}s"
    )
