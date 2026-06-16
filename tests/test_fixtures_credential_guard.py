"""Tests for CHAOS-2458: fixture seeder must not clobber real auth graph rows.

Covers:
1. Seeding into a DB with an existing NON-fixture user of the same id/email does
   NOT change that user's password_hash.
2. Seeding still correctly creates genuine fixture users and memberships.
3. overwrite_real_users=True bypasses the guard (explicit opt-in).
4. Existing users/orgs/licenses are treated as real without bcrypt ownership checks.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import bcrypt
import pytest
import pytest_asyncio
from sqlalchemy import func, select
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
_REAL_DEMO_PASSWORD_HASH = bcrypt.hashpw(
    _FIXTURE_PASSWORD.encode("utf-8"), bcrypt.gensalt()
).decode("utf-8")


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
async def test_real_user_not_added_to_fixture_org_or_membership(session_maker):
    """A skipped real user must not receive fixture org membership."""
    user_id = _fixture_user_id()
    org = _make_org()

    async with session_maker() as session:
        session.add(
            User(
                id=user_id,
                email="admin@devhealth.example",
                username="admin",
                password_hash=_REAL_HASH,
                full_name="Real Admin",
                auth_provider="local",
                is_active=True,
                is_verified=True,
                is_superuser=False,
            )
        )
        await session.commit()

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        memberships = (
            (
                await session.execute(
                    select(Membership).where(
                        Membership.user_id == user_id,
                        Membership.org_id == org.id,
                    )
                )
            )
            .scalars()
            .all()
        )
        user = await session.get(User, user_id)

    assert user is not None
    assert user.is_superuser is False
    assert memberships == []


@pytest.mark.asyncio
async def test_real_user_with_demo_password_is_still_not_overwritten(session_maker):
    """Password equality is not an ownership signal for existing rows."""
    user_id = _fixture_user_id()

    async with session_maker() as session:
        session.add(
            User(
                id=user_id,
                email="admin@devhealth.example",
                username="admin",
                password_hash=_REAL_DEMO_PASSWORD_HASH,
                full_name="Real Admin Using Demo Password",
                auth_provider="local",
                is_active=True,
                is_verified=False,
                is_superuser=False,
            )
        )
        await session.commit()

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        user = await session.get(User, user_id)
        memberships = (
            (
                await session.execute(
                    select(Membership).where(Membership.user_id == user_id)
                )
            )
            .scalars()
            .all()
        )

    assert user is not None
    assert user.password_hash == _REAL_DEMO_PASSWORD_HASH
    assert user.full_name == "Real Admin Using Demo Password"
    assert user.is_verified is False
    assert user.is_superuser is False
    assert memberships == []


@pytest.mark.asyncio
async def test_existing_org_license_not_replaced(session_maker):
    """A pre-existing tenant keeps its org and license state by default."""
    from dev_health_ops.licensing.types import LicenseTier

    org = _make_org()
    real_license_id = uuid.uuid5(org.id, "real-license")

    async with session_maker() as session:
        session.add(
            Organization(
                id=org.id,
                slug=org.slug,
                name="Real Tenant",
                tier="community",
                is_active=True,
            )
        )
        real_license = OrgLicense(
            org_id=org.id,
            tier=LicenseTier.COMMUNITY.value,
            license_type="self-hosted",
            licensed_users=3,
            licensed_repos=2,
            issued_at=datetime.now(timezone.utc),
        )
        real_license.id = real_license_id
        session.add(real_license)
        await session.commit()

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        saved_org = await session.get(Organization, org.id)
        licenses = (
            (
                await session.execute(
                    select(OrgLicense).where(OrgLicense.org_id == org.id)
                )
            )
            .scalars()
            .all()
        )

    assert saved_org is not None
    assert saved_org.name == "Real Tenant"
    assert saved_org.tier == "community"
    assert len(licenses) == 1
    assert licenses[0].id == real_license_id
    assert licenses[0].tier == LicenseTier.COMMUNITY.value
    assert licenses[0].license_type == "self-hosted"
    assert licenses[0].licensed_users == 3


@pytest.mark.asyncio
async def test_fixture_user_created_when_absent(session_maker):
    """Seeding into an empty DB must create the fixture user auth graph."""
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        user = await session.get(User, _fixture_user_id())
        assert user is not None
        memberships = (
            (
                await session.execute(
                    select(Membership).where(Membership.user_id == user.id)
                )
            )
            .scalars()
            .all()
        )
        licenses = (await session.execute(select(OrgLicense))).scalars().all()
        assert user.email == "admin@devhealth.example"
        # The stored hash must verify against the fixture password.
        assert bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )
        assert len(memberships) == 1
        assert memberships[0].role == "owner"
        assert len(licenses) == 1
        assert licenses[0].tier == "enterprise"


@pytest.mark.asyncio
async def test_fixture_user_not_refreshed_on_default_reseed(session_maker):
    """Re-seeding existing rows is a safe no-op by default."""
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    # Mutate a non-credential field to verify the merge actually ran.
    async with session_maker() as session:
        user = await session.get(User, _fixture_user_id())
        user.full_name = "Mutated Name"
        await session.commit()

    # Re-seed — should not mutate any existing user fields by default.
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        user = await session.get(User, _fixture_user_id())
        assert user is not None
        assert user.full_name == "Mutated Name"
        assert bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )


@pytest.mark.asyncio
async def test_overwrite_real_users_opt_in_clobbers_hash(session_maker):
    """overwrite_real_users=True must bypass the guard (explicit opt-in path)."""
    from dev_health_ops.licensing.types import LicenseTier

    user_id = _fixture_user_id()
    org = _make_org()

    async with session_maker() as session:
        session.add(
            Organization(
                id=org.id,
                slug=org.slug,
                name="Real Tenant",
                tier="community",
                is_active=True,
            )
        )
        real_user = User(
            id=user_id,
            email="admin@devhealth.example",
            username="admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=False,
            is_superuser=False,
        )
        session.add(real_user)
        real_license = OrgLicense(
            org_id=org.id,
            tier=LicenseTier.COMMUNITY.value,
            license_type="self-hosted",
            licensed_users=3,
            licensed_repos=2,
            issued_at=datetime.now(timezone.utc),
        )
        real_license.id = uuid.uuid5(org.id, "real-license")
        session.add(real_license)
        await session.commit()

    # Explicit opt-in: caller accepts credential clobber.
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data(), overwrite_real_users=True)

    async with session_maker() as session:
        user = await session.get(User, user_id)
        saved_org = await session.get(Organization, org.id)
        memberships = (
            (
                await session.execute(
                    select(Membership).where(Membership.user_id == user_id)
                )
            )
            .scalars()
            .all()
        )
        licenses = (
            (
                await session.execute(
                    select(OrgLicense).where(OrgLicense.org_id == org.id)
                )
            )
            .scalars()
            .all()
        )
        assert user is not None
        # Hash must now be the fixture hash (clobbered).
        assert bcrypt.checkpw(
            _FIXTURE_PASSWORD.encode("utf-8"), user.password_hash.encode("utf-8")
        )
        assert user.full_name == "Admin User"
        assert user.is_verified is True
        assert user.is_superuser is True
        assert saved_org is not None
        assert saved_org.name == "Demo Org"
        assert saved_org.tier == "enterprise"
        assert len(memberships) == 1
        assert memberships[0].role == "owner"
        assert len(licenses) == 1
        assert licenses[0].tier == LicenseTier.ENTERPRISE.value
        assert licenses[0].license_type == "saas"
        assert licenses[0].licensed_users is None


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

        status = await _merge_fixture_user(
            session,
            user,
            overwrite_real_users=False,
        )
        await session.commit()
        assert status == "inserted"

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
        status = await _merge_fixture_user(
            session,
            fixture_user,
            overwrite_real_users=False,
        )
        await session.commit()
        assert status == "skipped_existing"

    async with session_maker() as session:
        user = await session.get(User, user_id)
        assert user.password_hash == _REAL_HASH


@pytest.mark.asyncio
async def test_merge_fixture_user_skips_existing_without_password_check(session_maker):
    """Existing rows are skipped without using password ownership checks."""
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
        status = await _merge_fixture_user(
            session,
            fixture_user,
            overwrite_real_users=False,
        )
        await session.commit()
        assert status == "skipped_existing"

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


@pytest.mark.asyncio
async def test_mixed_case_real_identity_not_duplicated(session_maker):
    """A real user with mixed-case email/username must be detected
    case-insensitively (matching the auth layer's lower() semantics) so the
    seeder does not insert a duplicate lowercase demo account for the same
    identity (CHAOS-2458 review follow-up)."""
    real_id = uuid.uuid4()  # distinct from the deterministic fixture uuid5
    async with session_maker() as session:
        real_user = User(
            id=real_id,
            email="Admin@DevHealth.example",
            username="Admin",
            password_hash=_REAL_HASH,
            full_name="Real Admin",
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=True,
        )
        session.add(real_user)
        await session.commit()

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        rows = (
            (
                await session.execute(
                    select(User).where(
                        func.lower(User.email) == "admin@devhealth.example"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1, "Seeder must not duplicate a mixed-case real identity"
        assert rows[0].id == real_id
        assert rows[0].password_hash == _REAL_HASH
        # The lowercase fixture UUID must NOT have been inserted as a new row.
        assert await session.get(User, _fixture_user_id()) is None


@pytest.mark.asyncio
async def test_mixed_case_real_org_slug_not_duplicated(session_maker):
    """A real org with a mixed-case slug must be detected case-insensitively
    (matching the org service's lower(slug) lookup) so the seeder does not
    insert a duplicate tenant + fixture license/memberships (CHAOS-2458
    review follow-up)."""
    real_org_id = uuid.uuid4()  # distinct from the deterministic fixture uuid5
    async with session_maker() as session:
        real_org = Organization(
            id=real_org_id,
            slug="Default-Org",
            name="Real Tenant",
            tier="enterprise",
            is_active=True,
        )
        session.add(real_org)
        await session.commit()

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        orgs = (
            (
                await session.execute(
                    select(Organization).where(
                        func.lower(Organization.slug) == "default-org"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(orgs) == 1, "Seeder must not duplicate a mixed-case real org slug"
        assert orgs[0].id == real_org_id
        # The deterministic fixture org id must NOT have been inserted.
        assert await session.get(Organization, _make_org().id) is None
        # No fixture license rows should exist for the duplicate tenant.
        licenses = (await session.execute(select(OrgLicense))).scalars().all()
        assert all(lic.org_id == real_org_id for lic in licenses) or not licenses
