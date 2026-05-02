"""E2E tests for billing email pipeline with real SQLite DB.

These tests verify the full flow from org owner lookup through email dispatch,
using a real SQLite database for User/Organization/Membership queries and
mocked email service to capture sent emails.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.git import Base
from dev_health_ops.models.users import MemberRole, Membership, Organization, User
from tests._helpers import tables_of

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "billing-e2e.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda c: Base.metadata.create_all(
                c,
                tables=tables_of(User, Organization, Membership),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded(session_maker):
    org = Organization(id=uuid.uuid4(), slug="acme", name="Acme Corp", tier="team")
    owner = User(
        id=uuid.uuid4(),
        email="owner@acme.com",
        full_name="Alice Owner",
        is_active=True,
    )
    member = User(
        id=uuid.uuid4(),
        email="member@acme.com",
        full_name="Bob Member",
        is_active=True,
    )

    async with session_maker() as session:
        session.add_all([org, owner, member])
        session.add_all(
            [
                Membership(
                    org_id=org.id,
                    user_id=owner.id,
                    role=MemberRole.OWNER.value,
                    created_at=datetime.now(timezone.utc) - timedelta(days=10),
                ),
                Membership(
                    org_id=org.id,
                    user_id=member.id,
                    role="member",
                    created_at=datetime.now(timezone.utc),
                ),
            ]
        )
        await session.commit()

    return {
        "org_id": str(org.id),
        "org_name": "Acme Corp",
        "org_tier": "team",
        "owner_email": "owner@acme.com",
        "owner_name": "Alice Owner",
    }


# ---------------------------------------------------------------------------
# E2E: get_org_owner_email with real DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_get_org_owner_email_real_db(session_maker, seeded):
    from dev_health_ops.api.services.billing_emails import get_org_owner_email

    async with session_maker() as db:
        result = await get_org_owner_email(db, uuid.UUID(seeded["org_id"]))

    assert result is not None
    email, full_name, org_name = result
    assert email == "owner@acme.com"
    assert full_name == "Alice Owner"
    assert org_name == "Acme Corp"


@pytest.mark.asyncio
async def test_e2e_get_org_owner_email_no_owner_real_db(session_maker):
    """Org with zero memberships returns None."""
    from dev_health_ops.api.services.billing_emails import get_org_owner_email

    lonely_org_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(Organization(id=lonely_org_id, slug="lonely", name="Lonely Org"))
        await session.commit()

    async with session_maker() as db:
        result = await get_org_owner_email(db, lonely_org_id)

    assert result is None


@pytest.mark.asyncio
async def test_e2e_get_org_owner_email_multiple_owners_returns_earliest(session_maker):
    """When multiple owners exist, returns the one with earliest created_at."""
    from dev_health_ops.api.services.billing_emails import get_org_owner_email

    org = Organization(id=uuid.uuid4(), slug="multi", name="Multi Owner Inc")
    first_owner = User(
        id=uuid.uuid4(), email="first@multi.com", full_name="First", is_active=True
    )
    second_owner = User(
        id=uuid.uuid4(), email="second@multi.com", full_name="Second", is_active=True
    )

    async with session_maker() as session:
        session.add_all([org, first_owner, second_owner])
        session.add_all(
            [
                Membership(
                    org_id=org.id,
                    user_id=first_owner.id,
                    role=MemberRole.OWNER.value,
                    created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
                Membership(
                    org_id=org.id,
                    user_id=second_owner.id,
                    role=MemberRole.OWNER.value,
                    created_at=datetime(2025, 6, 15, tzinfo=timezone.utc),
                ),
            ]
        )
        await session.commit()

    async with session_maker() as db:
        result = await get_org_owner_email(db, org.id)

    assert result is not None
    assert result[0] == "first@multi.com"
    assert result[1] == "First"
    assert result[2] == "Multi Owner Inc"


# ---------------------------------------------------------------------------
# E2E: send functions with real DB + mocked email service
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_send_invoice_receipt_real_db(session_maker, seeded):
    """Full pipeline: real DB owner lookup -> mocked email service."""
    from dev_health_ops.api.services.billing_emails import send_invoice_receipt

    mock_email_svc = MagicMock()
    mock_email_svc.send_template_email = AsyncMock()

    @asynccontextmanager
    async def patched_session():
        async with session_maker() as session:
            yield session

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            patched_session,
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_svc,
        ),
    ):
        await send_invoice_receipt(
            uuid.UUID(seeded["org_id"]),
            4900,
            "usd",
            "https://invoice.stripe.com/i/test",
        )

    mock_email_svc.send_template_email.assert_awaited_once()
    call_kwargs = mock_email_svc.send_template_email.call_args.kwargs
    assert call_kwargs["to_address"] == "owner@acme.com"
    assert call_kwargs["template_name"] == "invoice_receipt"
    assert call_kwargs["context"]["full_name"] == "Alice Owner"
    assert call_kwargs["context"]["org_name"] == "Acme Corp"
    assert call_kwargs["context"]["amount"] == "49.00"
    assert call_kwargs["context"]["currency"] == "USD"
    assert call_kwargs["context"]["invoice_url"] == "https://invoice.stripe.com/i/test"


@pytest.mark.asyncio
async def test_e2e_send_subscription_changed_real_db(session_maker, seeded):
    """Full pipeline: real DB -> subscription changed email with tier names."""
    from dev_health_ops.api.services.billing_emails import send_subscription_changed

    mock_email_svc = MagicMock()
    mock_email_svc.send_template_email = AsyncMock()

    @asynccontextmanager
    async def patched_session():
        async with session_maker() as session:
            yield session

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            patched_session,
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_svc,
        ),
    ):
        await send_subscription_changed(
            uuid.UUID(seeded["org_id"]), "team", "enterprise"
        )

    call_kwargs = mock_email_svc.send_template_email.call_args.kwargs
    assert call_kwargs["to_address"] == "owner@acme.com"
    assert call_kwargs["template_name"] == "subscription_changed"
    assert call_kwargs["context"]["old_tier"] == "team"
    assert call_kwargs["context"]["new_tier"] == "enterprise"


@pytest.mark.asyncio
async def test_e2e_send_no_owner_skips_email(session_maker):
    """Send function with org that has no owner -> email NOT sent."""
    from dev_health_ops.api.services.billing_emails import send_invoice_receipt

    lonely_org_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(Organization(id=lonely_org_id, slug="empty", name="Empty Org"))
        await session.commit()

    mock_email_svc = MagicMock()
    mock_email_svc.send_template_email = AsyncMock()

    @asynccontextmanager
    async def patched_session():
        async with session_maker() as session:
            yield session

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            patched_session,
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_svc,
        ),
    ):
        await send_invoice_receipt(lonely_org_id, 1000, "usd", "https://example.com")

    mock_email_svc.send_template_email.assert_not_awaited()
