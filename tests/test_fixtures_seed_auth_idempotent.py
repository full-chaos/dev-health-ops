"""Regression test for CHAOS-1717: re-running ``_seed_auth_data`` must not crash.

Before this fix, a second call would raise
``UniqueViolationError: uq_membership_user_org`` because
``session.merge(membership)`` autoflushed a pending INSERT before the
merge's SELECT resolved, even though the row already existed with a
deterministic primary key.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.fixtures.demo_identity import (
    ONBOARDED_ADMIN_USER_EMAIL,
    ONBOARDED_ADMIN_USER_FULL_NAME,
    ONBOARDED_ADMIN_USER_USERNAME,
    ONBOARDING_ORGLESS_USER_EMAIL,
    ONBOARDING_ORGLESS_USER_FULL_NAME,
    ONBOARDING_ORGLESS_USER_USERNAME,
)
from dev_health_ops.fixtures.runner import _seed_auth_data
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "seed-auth-idempotent.db"
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


def _build_user_data() -> dict:
    """Build the same user_data shape the fixtures generator produces.

    Two users matching the fixture journeys: one orgless onboarding identity
    and one onboarded admin bound to one org/license. Deterministic UUIDs make
    repeated invocations land on identical PKs.
    """
    target_org_id = uuid.UUID("ae600a94-76bc-4166-bf36-051ee4247c73")

    admin = User(
        id=uuid.uuid5(_NS, ONBOARDED_ADMIN_USER_EMAIL),
        email=ONBOARDED_ADMIN_USER_EMAIL,
        username=ONBOARDED_ADMIN_USER_USERNAME,
        password_hash="$2b$12$dummy",
        full_name=ONBOARDED_ADMIN_USER_FULL_NAME,
        auth_provider="local",
        is_active=True,
        is_verified=True,
        is_superuser=True,
    )
    onboarding = User(
        id=uuid.uuid5(_NS, ONBOARDING_ORGLESS_USER_EMAIL),
        email=ONBOARDING_ORGLESS_USER_EMAIL,
        username=ONBOARDING_ORGLESS_USER_USERNAME,
        password_hash="$2b$12$dummy",
        full_name=ONBOARDING_ORGLESS_USER_FULL_NAME,
        auth_provider="local",
        is_active=True,
        is_verified=True,
        is_superuser=False,
    )
    org = Organization(
        id=target_org_id,
        slug="fixture-ae600a94",
        name="Fixture Org",
        tier="enterprise",
        is_active=True,
    )
    now = datetime.now(timezone.utc)
    admin_membership = Membership(
        id=uuid.uuid5(admin.id, str(org.id)),
        user_id=admin.id,
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
        "users": [onboarding, admin],
        "memberships": [admin_membership],
        "licenses": [license_row],
    }


@pytest.mark.asyncio
async def test_seed_auth_data_is_idempotent_on_rerun(session_maker):
    """Second invocation must not raise UniqueViolationError."""

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    # Second invocation: deterministic PKs collide with existing rows. The
    # pre-fix code raised UniqueViolationError here. Post-fix must succeed.
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    # And a third for paranoia.
    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    # Final state: exactly the expected row counts, no duplicates.
    async with session_maker() as session:
        orgs = (await session.execute(select(Organization))).scalars().all()
        users = (await session.execute(select(User))).scalars().all()
        memberships = (await session.execute(select(Membership))).scalars().all()
        licenses = (await session.execute(select(OrgLicense))).scalars().all()

    assert len(orgs) == 1
    assert len(users) == 2
    assert len(memberships) == 1
    assert {m.role for m in memberships} == {"owner"}
    assert len(licenses) == 1


@pytest.mark.asyncio
async def test_seed_auth_data_first_invocation_writes_everything(session_maker):
    """Sanity: the first call still seeds the expected rows."""

    async with session_maker() as session:
        await _seed_auth_data(session, _build_user_data())

    async with session_maker() as session:
        memberships = (await session.execute(select(Membership))).scalars().all()

    assert len(memberships) == 1
    user_org_pairs = {(m.user_id, m.org_id) for m in memberships}
    assert len(user_org_pairs) == 1


@pytest.mark.asyncio
async def test_seed_auth_data_handles_empty_memberships(session_maker):
    """An empty membership list must not break the upsert path."""

    data = _build_user_data()
    data["memberships"] = []
    async with session_maker() as session:
        await _seed_auth_data(session, data)

    async with session_maker() as session:
        memberships = (await session.execute(select(Membership))).scalars().all()
    assert memberships == []


@pytest.mark.asyncio
async def test_seed_auth_data_uses_upsert_for_memberships(tmp_path):
    """Guard against accidental revert to ``session.merge(membership)``.

    The SQLite-backed idempotency tests above only catch part of the bug:
    SQLite's constraint-check timing differs from Postgres + asyncpg, so a
    naive ``session.merge(membership)`` flow can still pass on SQLite while
    crashing in production. This test inspects the actual SQL emitted by
    ``_seed_auth_data`` and asserts the membership statement carries the
    ``ON CONFLICT ... DO NOTHING`` clause that makes the seed truly
    idempotent on Postgres.
    """
    from sqlalchemy import event

    db_path = tmp_path / "upsert-capture.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(User, Organization, Membership, OrgLicense),
            )
        )

    captured_sql: list[str] = []

    def _record(conn, cursor, statement, parameters, context, executemany):  # noqa: ARG001
        captured_sql.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", _record)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with maker() as session:
            await _seed_auth_data(session, _build_user_data())
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _record)
        await engine.dispose()

    membership_inserts = [sql for sql in captured_sql if "INTO memberships" in sql]
    assert membership_inserts, (
        "expected at least one INSERT INTO memberships statement; "
        f"captured {len(captured_sql)} statements: {captured_sql}"
    )
    assert any(
        "ON CONFLICT" in sql and "DO NOTHING" in sql for sql in membership_inserts
    ), (
        "membership INSERT must use ON CONFLICT DO NOTHING for idempotency; "
        f"got: {membership_inserts}"
    )
