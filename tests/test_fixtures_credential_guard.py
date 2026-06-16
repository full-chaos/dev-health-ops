"""Tests for CHAOS-2458: fixture seeder must not clobber real users' password_hash.

Covers:
1. Seeding into a DB with an existing NON-fixture user of the same id/email does
   NOT change that user's password_hash.
2. Seeding still correctly creates/refreshes genuine fixture users.
3. overwrite_real_users=True bypasses the guard (explicit opt-in).
4. _merge_fixture_user handles missing default_password gracefully (skips).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import bcrypt
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.fixtures.runner import _merge_fixture_user, _seed_auth_data
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

# Fixed fixture password used by the generator.
_FIXTURE_PASSWORD = "devhealth123"
_FIXTURE_HASH = bcrypt.hashpw(
    _FIXTURE_PASSWORD.encode("utf-8"), bcrypt.gensalt()
).decode("utf-8")

# A completely different password representing a real user's credential.
_REAL_PASSWORD = "s3cr3t-real-password-not-demo"
_REAL_HASH = bcrypt.hashpw(_REAL_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode(
    "utf-8"
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "credential-guard.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(User, Organization, Membership, OrgLicense),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


def _fixture_user_id() -> uuid.UUID:
    """Deterministic UUID matching what the generator produces for admin@devhealth.example."""
    return uuid.uuid5(_NS, "admin@devhealth.example")


def _make_fixture_user(password_hash: str = _FIXTURE_HASH) -> User:
    """Build a fixture User object as the seeder would."""
    return User(
        id=_fixture_user_id(),
        email="admin@devhealth.example",
        username="admin",
        password_hash=password_hash,
        full_name="Admin User",
        auth_provider="local",
        is_active=True,
        is_verified=True,
        is_superuser=True,
    )


def _make_org() -> Organization:
    org_id = uuid.uuid5(_NS, "default-org")
    return Organization(
        id=org_id,
        slug="default-org",
        name="Demo Org",
        tier="enterprise",
        is_active=True,
    )


def _build_user_data(password_hash: str = _FIXTURE_HASH) -> dict:
    """Build a minimal user_data dict matching the generator's output shape."""
    org = _make_org()
    user = _make_fixture_user(password_hash)
    now = datetime.now(timezone.utc)
    from dev_health_ops.licensing.types import LicenseTier

    membership = Membership(
        id=uuid.uuid5(user.id, str(org.id)),
        user_id=user.id,
        org_id=org.id,
        role="owner",
        joined_at=now,
    )
    license_row = OrgLicense(
        org_id=org.id,
        tier=LicenseTier.ENTERPRISE.value,
        license_type="saas",
        licensed_users=None,
        licensed_repos=None,
        issued_at=now,
    )
    license_row.id = uuid.uuid5(org.id, "org-license")
    return {
        "organizations": [org],
        "users": [user],
        "memberships": [membership],
        "licenses": [license_row],
        "default_password": _FIXTURE_PASSWORD,
    }


# ---------------------------------------------------------------------------
# Core guard tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_real_user_password_not_overwritten(session_maker):
    """Seeding must NOT overwrite password_hash of a pre-existing real user.

    Scenario: a real user was registered with a strong password.  Their UUID
    happens to collide with the fixture admin UUID (e.g. the DB was previously
    seeded and the user kept that id).  Re-running the seeder must leave their
    credential intact.
    """
    user_id = _fixture_user_id()

    # Pre-populate DB with a "real" user at the fixture UUID but with a
    # non-fixture password hash.
    async with session_maker() as session:
        real_user = User(
            id=user_id,
            email="admin@devhealth.example",
            username="admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=True,
        )
        session.add(real_user)
        await session.commit()

    # Run the fixture seeder (default: overwrite_real_users=False).
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    # The password_hash must be unchanged.
    async with session_maker() as session:
        user = await session.get(User, user_id)
        assert user is not None
        assert user.password_hash == _REAL_HASH, (
            "Fixture seeder must not overwrite a real user's password_hash"
        )
        # Verify the real hash still works for the real password.
        assert bcrypt.checkpw(
            _REAL_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )
        # And does NOT work for the fixture password.
        assert not bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )


@pytest.mark.asyncio
async def test_fixture_user_created_when_absent(session_maker):
    """Seeding into an empty DB must create the fixture user correctly."""
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        user = await session.get(User, _fixture_user_id())
        assert user is not None
        assert user.email == "admin@devhealth.example"
        # The stored hash must verify against the fixture password.
        assert bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )


@pytest.mark.asyncio
async def test_fixture_user_refreshed_on_reseed(session_maker):
    """Re-seeding a genuine fixture user (same fixture hash) must succeed.

    This covers the idempotent re-seed path: the existing row has the fixture
    hash, so the guard recognises it as fixture-owned and allows the merge.
    """
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    # Mutate a non-credential field to verify the merge actually ran.
    async with session_maker() as session:
        user = await session.get(User, _fixture_user_id())
        user.full_name = "Mutated Name"
        await session.commit()

    # Re-seed — should restore full_name and keep the fixture hash.
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        user = await session.get(User, _fixture_user_id())
        assert user is not None
        assert user.full_name == "Admin User"
        assert bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )


@pytest.mark.asyncio
async def test_overwrite_real_users_opt_in_clobbers_hash(session_maker):
    """overwrite_real_users=True must bypass the guard (explicit opt-in path)."""
    user_id = _fixture_user_id()

    async with session_maker() as session:
        real_user = User(
            id=user_id,
            email="admin@devhealth.example",
            username="admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=True,
        )
        session.add(real_user)
        await session.commit()

    # Explicit opt-in: caller accepts credential clobber.
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data(), overwrite_real_users=True)

    async with session_maker() as session:
        user = await session.get(User, user_id)
        assert user is not None
        # Hash must now be the fixture hash (clobbered).
        assert bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )


# ---------------------------------------------------------------------------
# _merge_fixture_user unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_merge_fixture_user_inserts_when_absent(session_maker):
    """_merge_fixture_user inserts a new row when no collision exists."""
    user = _make_fixture_user()
    async with session_maker() as session:
        # Ensure org exists so FK is satisfied (if enforced).
        org = _make_org()
        await session.merge(org)
        await session.commit()

        await _merge_fixture_user(
            session,
            user,
            default_password=_FIXTURE_PASSWORD,
            overwrite_real_users=False,
        )
        await session.commit()

    async with session_maker() as session:
        result = await session.get(User, user.id)
        assert result is not None
        assert result.email == "admin@devhealth.example"


@pytest.mark.asyncio
async def test_merge_fixture_user_skips_real_user(session_maker):
    """_merge_fixture_user must not touch a row whose hash is not the fixture hash."""
    user_id = _fixture_user_id()

    async with session_maker() as session:
        real_user = User(
            id=user_id,
            email="admin@devhealth.example",
            username="admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=True,
        )
        session.add(real_user)
        await session.commit()

    fixture_user = _make_fixture_user()  # has _FIXTURE_HASH
    async with session_maker() as session:
        await _merge_fixture_user(
            session,
            fixture_user,
            default_password=_FIXTURE_PASSWORD,
            overwrite_real_users=False,
        )
        await session.commit()

    async with session_maker() as session:
        user = await session.get(User, user_id)
        assert user.password_hash == _REAL_HASH


@pytest.mark.asyncio
async def test_merge_fixture_user_no_default_password_skips_existing(session_maker):
    """When default_password is None, existing rows must not be overwritten."""
    user_id = _fixture_user_id()

    async with session_maker() as session:
        real_user = User(
            id=user_id,
            email="admin@devhealth.example",
            username="admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=True,
        )
        session.add(real_user)
        await session.commit()

    fixture_user = _make_fixture_user()
    async with session_maker() as session:
        await _merge_fixture_user(
            session,
            fixture_user,
            default_password=None,  # no password provided
            overwrite_real_users=False,
        )
        await session.commit()

    async with session_maker() as session:
        user = await session.get(User, user_id)
        assert user.password_hash == _REAL_HASH


@pytest.mark.asyncio
async def test_seed_auth_data_default_is_safe(session_maker):
    """Calling _seed_auth_data without keyword args must use the safe default."""
    import inspect

    sig = inspect.signature(_seed_auth_data)
    param = sig.parameters.get("overwrite_real_users")
    assert param is not None, "_seed_auth_data must have overwrite_real_users param"
    assert param.default is False, (
        "overwrite_real_users must default to False (safe default)"
    )


@pytest.mark.asyncio
async def test_guard_emits_warning_for_real_user(session_maker, caplog):
    """The guard must log a warning when it skips a real user."""
    import logging

    user_id = _fixture_user_id()

    async with session_maker() as session:
        real_user = User(
            id=user_id,
            email="admin@devhealth.example",
            username="admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=True,
        )
        session.add(real_user)
        await session.commit()

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.fixtures.runner"):
        async with session_maker() as session:
            await _seed_auth_data(session, _build_user_data())

    assert any("FIXTURES-GUARD" in record.message for record in caplog.records), (
        f"Expected FIXTURES-GUARD warning; got: {[r.message for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_multiple_users_partial_guard(session_maker):
    """When user_data has multiple users, guard applies per-user independently.

    - user A: real user (different hash) → must be skipped
    - user B: absent → must be inserted
    """
    from dev_health_ops.licensing.types import LicenseTier

    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    org = _make_org()
    now = datetime.now(timezone.utc)

    user_a_id = uuid.uuid5(ns, "alice@example.com")
    user_b_id = uuid.uuid5(ns, "bob@example.com")

    # Pre-seed user A with a real (non-fixture) hash.
    async with session_maker() as session:
        await session.merge(org)
        real_a = User(
            id=user_a_id,
            email="alice@example.com",
            username="alice",
            password_hash=_REAL_HASH,
            full_name="Real Alice",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=False,
        )
        session.add(real_a)
        await session.commit()

    fixture_a = User(
        id=user_a_id,
        email="alice@example.com",
        username="alice",
        password_hash=_FIXTURE_HASH,
        full_name="Fixture Alice",
        auth_provider="local",
        is_active=True,
        is_verified=True,
        is_superuser=False,
    )
    fixture_b = User(
        id=user_b_id,
        email="bob@example.com",
        username="bob",
        password_hash=_FIXTURE_HASH,
        full_name="Fixture Bob",
        auth_provider="local",
        is_active=True,
        is_verified=True,
        is_superuser=False,
    )

    license_row = OrgLicense(
        org_id=org.id,
        tier=LicenseTier.ENTERPRISE.value,
        license_type="saas",
        licensed_users=None,
        licensed_repos=None,
        issued_at=now,
    )
    license_row.id = uuid.uuid5(org.id, "org-license")

    user_data = {
        "organizations": [org],
        "users": [fixture_a, fixture_b],
        "memberships": [],
        "licenses": [license_row],
        "default_password": _FIXTURE_PASSWORD,
    }

    async with session_maker() as session:
        await _seed_auth_data(session, user_data)

    async with session_maker() as session:
        a = await session.get(User, user_a_id)
        b = await session.get(User, user_b_id)

    # User A: real hash must be preserved.
    assert a is not None
    assert a.password_hash == _REAL_HASH, "Real user A's hash must not be overwritten"

    # User B: fixture user must be created.
    assert b is not None
    assert bcrypt.checkpw(
        _FIXTURE_PASSWORD.encode("utf-8"), b.password_hash.encode("utf-8")
    ), "Fixture user B must be seeded with the fixture hash"
