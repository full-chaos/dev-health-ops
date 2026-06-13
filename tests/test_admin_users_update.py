"""Tests for the `dev-hops admin users update` CLI command."""

from __future__ import annotations

import argparse
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.cli import _update_user_async
from dev_health_ops.api.services.users import UserService, _hash_password
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of


def _ns(**overrides: object) -> argparse.Namespace:
    """Build an argparse.Namespace with every `users update` flag defaulted."""
    defaults: dict[str, object] = {
        "db": None,
        "id": None,
        "email": None,
        "username": None,
        "new_email": None,
        "new_username": None,
        "full_name": None,
        "password": None,
        "verified": None,
        "superuser": None,
        "active": None,
        "membership_org": None,
        "role": None,
        "remove_from_org": None,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "admin-users-update.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(User, Organization, Membership, RefreshToken),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded(session_maker):
    user = User(
        id=uuid.uuid4(),
        email="alice@example.com",
        username="alice",
        full_name="Alice Alpha",
        password_hash=_hash_password("original-password"),
        is_active=True,
        is_verified=False,
        is_superuser=False,
    )
    org_a = Organization(id=uuid.uuid4(), slug="org-a", name="Org A")
    org_b = Organization(id=uuid.uuid4(), slug="org-b", name="Org B")

    async with session_maker() as session:
        session.add_all([user, org_a, org_b])
        # Alice is already a viewer of org-a.
        session.add(
            Membership(
                id=uuid.uuid4(),
                org_id=org_a.id,
                user_id=user.id,
                role="viewer",
            )
        )
        await session.commit()

    return {
        "user_id": str(user.id),
        "org_a_id": str(org_a.id),
        "org_b_id": str(org_b.id),
    }


async def _run(session_maker, ns: argparse.Namespace) -> int:
    session = session_maker()
    with patch(
        "dev_health_ops.api.admin.cli._get_session",
        return_value=session,
    ):
        return await _update_user_async(ns)


@pytest.mark.asyncio
async def test_update_password(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", password="brand-new-password"),
    )
    assert rc == 0

    async with session_maker() as session:
        svc = UserService(session)
        assert await svc.verify_password(seeded["user_id"], "brand-new-password")
        assert not await svc.verify_password(seeded["user_id"], "original-password")


@pytest.mark.asyncio
async def test_password_too_short_rolls_back(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", password="short"),
    )
    assert rc == 1

    async with session_maker() as session:
        svc = UserService(session)
        # Original password must remain intact after the rollback.
        assert await svc.verify_password(seeded["user_id"], "original-password")


@pytest.mark.asyncio
async def test_update_validated_and_user_type(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", verified=True, superuser=True),
    )
    assert rc == 0

    async with session_maker() as session:
        user = await UserService(session).get_by_id(seeded["user_id"])
        assert user is not None
        assert user.is_verified is True
        assert user.is_superuser is True


@pytest.mark.asyncio
async def test_deactivate_via_no_active(session_maker, seeded):
    rc = await _run(session_maker, _ns(email="alice@example.com", active=False))
    assert rc == 0

    async with session_maker() as session:
        user = await UserService(session).get_by_id(seeded["user_id"])
        assert user is not None
        assert user.is_active is False


@pytest.mark.asyncio
async def test_add_to_org_by_slug_with_role(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", membership_org="org-b", role="admin"),
    )
    assert rc == 0

    async with session_maker() as session:
        from dev_health_ops.api.services.users import MembershipService

        role = await MembershipService(session).get_user_role(
            seeded["org_b_id"], seeded["user_id"]
        )
        assert role == "admin"


@pytest.mark.asyncio
async def test_update_existing_org_role(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", membership_org="org-a", role="owner"),
    )
    assert rc == 0

    async with session_maker() as session:
        from dev_health_ops.api.services.users import MembershipService

        role = await MembershipService(session).get_user_role(
            seeded["org_a_id"], seeded["user_id"]
        )
        assert role == "owner"


@pytest.mark.asyncio
async def test_remove_from_org(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", remove_from_org="org-a"),
    )
    assert rc == 0

    async with session_maker() as session:
        from dev_health_ops.api.services.users import MembershipService

        membership = await MembershipService(session).get_membership(
            seeded["org_a_id"], seeded["user_id"]
        )
        assert membership is None


@pytest.mark.asyncio
async def test_no_action_specified_errors(session_maker, seeded):
    rc = await _run(session_maker, _ns(email="alice@example.com"))
    assert rc == 1


@pytest.mark.asyncio
async def test_role_without_org_errors(session_maker, seeded):
    rc = await _run(session_maker, _ns(email="alice@example.com", role="admin"))
    assert rc == 1


@pytest.mark.asyncio
async def test_user_not_found(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="nobody@example.com", verified=True),
    )
    assert rc == 1


@pytest.mark.asyncio
async def test_unknown_org_errors(session_maker, seeded):
    rc = await _run(
        session_maker,
        _ns(email="alice@example.com", membership_org="does-not-exist"),
    )
    assert rc == 1
