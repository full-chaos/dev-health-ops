from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.billing_emails import (
    get_org_owner_email,
    send_invoice_receipt,
    send_payment_failed,
    send_subscription_cancelled,
    send_subscription_changed,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import MemberRole, Membership, Organization, User


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "billing-emails.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


def _mock_db_session(
    *, owner: tuple[str, str | None] | None, org_name: str = "Acme Corp"
):
    mock_session = AsyncMock()
    owner_result = MagicMock()

    if owner is None:
        owner_result.first.return_value = None
        mock_session.execute = AsyncMock(return_value=owner_result)
        return mock_session

    owner_email, full_name = owner
    owner_row = MagicMock(email=owner_email, full_name=full_name)
    owner_result.first.return_value = owner_row

    org_row = MagicMock(name=org_name)
    org_result = MagicMock()
    org_result.first.return_value = org_row

    mock_session.execute = AsyncMock(side_effect=[owner_result, org_result])
    return mock_session


def _mock_postgres_session_ctx(mock_session: AsyncMock):
    @asynccontextmanager
    async def _ctx():
        yield mock_session

    return _ctx()


@pytest.mark.asyncio
async def test_get_org_owner_email_found(session_maker):
    org_id = uuid.uuid4()
    owner_id = uuid.uuid4()

    org = Organization(id=org_id, slug="acme", name="Acme Corp")
    owner = User(
        id=owner_id,
        email="owner@example.com",
        full_name="Owner Person",
        is_active=True,
    )
    membership = Membership(
        org_id=org_id,
        user_id=owner_id,
        role=MemberRole.OWNER.value,
    )

    async with session_maker() as db:
        db.add_all([org, owner, membership])
        await db.commit()

    async with session_maker() as db:
        result = await get_org_owner_email(db, org_id)

    assert result == ("owner@example.com", "Owner Person", "Acme Corp")


@pytest.mark.asyncio
async def test_get_org_owner_email_not_found(session_maker):
    org_id = uuid.uuid4()

    org = Organization(id=org_id, slug="no-owner", name="No Owner Org")
    member = User(id=uuid.uuid4(), email="member@example.com", is_active=True)
    membership = Membership(
        org_id=org_id,
        user_id=member.id,
        role="member",
    )

    async with session_maker() as db:
        db.add_all([org, member, membership])
        await db.commit()

    async with session_maker() as db:
        result = await get_org_owner_email(db, org_id)

    assert result is None


@pytest.mark.asyncio
async def test_get_org_owner_email_multiple_owners_returns_first(session_maker):
    org_id = uuid.uuid4()
    first_owner_id = uuid.uuid4()
    second_owner_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    org = Organization(id=org_id, slug="two-owners", name="Two Owners")
    first_owner = User(
        id=first_owner_id,
        email="first-owner@example.com",
        full_name="First Owner",
        is_active=True,
    )
    second_owner = User(
        id=second_owner_id,
        email="second-owner@example.com",
        full_name="Second Owner",
        is_active=True,
    )
    first_membership = Membership(
        org_id=org_id,
        user_id=first_owner_id,
        role=MemberRole.OWNER.value,
        created_at=now,
    )
    second_membership = Membership(
        org_id=org_id,
        user_id=second_owner_id,
        role=MemberRole.OWNER.value,
        created_at=now + timedelta(minutes=5),
    )

    async with session_maker() as db:
        db.add_all(
            [org, first_owner, second_owner, first_membership, second_membership]
        )
        await db.commit()

    async with session_maker() as db:
        result = await get_org_owner_email(db, org_id)

    assert result == ("first-owner@example.com", "First Owner", "Two Owners")


@pytest.mark.asyncio
async def test_send_invoice_receipt_calls_email_service():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_invoice_receipt(org_id, 4900, "usd", "https://example.com")

    mock_email_service.send_template_email.assert_awaited_once()
    call_kwargs = mock_email_service.send_template_email.await_args.kwargs
    assert call_kwargs["template_name"] == "invoice_receipt"
    assert call_kwargs["context"]["amount"] == "49.00"
    assert call_kwargs["context"]["currency"] == "USD"


@pytest.mark.asyncio
async def test_send_invoice_receipt_missing_owner():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=None)
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_invoice_receipt(org_id, 4900, "usd", "https://example.com")

    mock_email_service.send_template_email.assert_not_called()


@pytest.mark.asyncio
async def test_send_invoice_receipt_email_failure():
    """Verify email service errors propagate (Celery handles retry)."""
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock(side_effect=RuntimeError("boom"))

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await send_invoice_receipt(org_id, 4900, "usd", "https://example.com")

    mock_email_service.send_template_email.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_payment_failed_calls_email_service():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_payment_failed(org_id, 999, "eur", 3)

    mock_email_service.send_template_email.assert_awaited_once()
    call_kwargs = mock_email_service.send_template_email.await_args.kwargs
    assert call_kwargs["template_name"] == "payment_failed"
    assert call_kwargs["context"]["amount"] == "9.99"
    assert call_kwargs["context"]["attempt_count"] == "3"
    assert call_kwargs["context"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_send_subscription_changed_calls_email_service():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_subscription_changed(org_id, "team", "enterprise")

    mock_email_service.send_template_email.assert_awaited_once()
    call_kwargs = mock_email_service.send_template_email.await_args.kwargs
    assert call_kwargs["template_name"] == "subscription_changed"
    assert call_kwargs["context"]["old_tier"] == "team"
    assert call_kwargs["context"]["new_tier"] == "enterprise"


@pytest.mark.asyncio
async def test_send_subscription_cancelled_calls_email_service():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_subscription_cancelled(org_id, "team")

    mock_email_service.send_template_email.assert_awaited_once()
    call_kwargs = mock_email_service.send_template_email.await_args.kwargs
    assert call_kwargs["template_name"] == "subscription_cancelled"
    assert call_kwargs["context"]["tier"] == "team"


@pytest.mark.asyncio
async def test_amount_formatting_zero():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_invoice_receipt(org_id, 0, "usd", "")

    call_kwargs = mock_email_service.send_template_email.call_args.kwargs
    assert call_kwargs["context"]["amount"] == "0.00"


@pytest.mark.asyncio
async def test_amount_formatting_cents():
    org_id = uuid.uuid4()
    mock_session = _mock_db_session(owner=("owner@test.com", "Test Owner"))
    mock_email_service = MagicMock()
    mock_email_service.send_template_email = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.services.billing_emails.get_postgres_session",
            return_value=_mock_postgres_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.api.services.billing_emails.get_email_service",
            return_value=mock_email_service,
        ),
    ):
        await send_invoice_receipt(org_id, 4900, "usd", "")

    call_kwargs = mock_email_service.send_template_email.call_args.kwargs
    assert call_kwargs["context"]["amount"] == "49.00"
