import logging
import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.email import get_email_service
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.users import MemberRole, Membership, Organization, User

logger = logging.getLogger(__name__)


async def get_org_owner_email(
    db: AsyncSession, org_id: uuid.UUID
) -> tuple[str, str, str] | None:
    """Return (email, full_name, org_name) of the Organization owner, or None if not found."""
    result = await db.execute(
        select(User.email, User.full_name)
        .join(Membership, Membership.user_id == User.id)
        .where(Membership.org_id == org_id)
        .where(Membership.role == MemberRole.OWNER.value)
        .order_by(Membership.created_at)
        .limit(1)
    )
    row = result.first()
    if row is None:
        logger.warning("No owner found for org_id=%s, skipping email", org_id)
        return None
    org_result = await db.execute(
        select(Organization.name).where(Organization.id == org_id)
    )
    org_row = org_result.first()
    org_name = str(org_row.name) if org_row else ""
    return (str(row.email), str(row.full_name) if row.full_name else "there", org_name)


async def send_invoice_receipt(
    org_id: uuid.UUID,
    amount_cents: int,
    currency: str,
    invoice_url: str,
) -> None:
    """Send invoice receipt email to org owner.

    Raises on email service failure so Celery can retry.
    Returns silently if no org owner is found (not retryable).
    """
    async with get_postgres_session() as db:
        owner = await get_org_owner_email(db, org_id)
        if owner is None:
            return
        to_email, full_name, org_name = owner
        amount_str = f"{Decimal(amount_cents) / 100:.2f}"
        email_service = get_email_service()
        await email_service.send_template_email(
            to_address=to_email,
            subject="Invoice receipt",
            template_name="invoice_receipt",
            context={
                "full_name": full_name,
                "org_name": org_name,
                "amount": amount_str,
                "currency": currency.upper(),
                "invoice_url": invoice_url,
            },
        )


async def send_payment_failed(
    org_id: uuid.UUID,
    amount_cents: int,
    currency: str,
    attempt_count: int,
) -> None:
    """Send payment failed email to org owner.

    Raises on email service failure so Celery can retry.
    Returns silently if no org owner is found (not retryable).
    """
    async with get_postgres_session() as db:
        owner = await get_org_owner_email(db, org_id)
        if owner is None:
            return
        to_email, full_name, org_name = owner
        amount_str = f"{Decimal(amount_cents) / 100:.2f}"
        email_service = get_email_service()
        await email_service.send_template_email(
            to_address=to_email,
            subject="Invoice payment failed",
            template_name="payment_failed",
            context={
                "full_name": full_name,
                "org_name": org_name,
                "amount": amount_str,
                "currency": currency.upper(),
                "attempt_count": str(attempt_count),
            },
        )


async def send_subscription_changed(
    org_id: uuid.UUID,
    old_tier: str,
    new_tier: str,
) -> None:
    """Send subscription changed email to org owner.

    Raises on email service failure so Celery can retry.
    Returns silently if no org owner is found (not retryable).
    """
    async with get_postgres_session() as db:
        owner = await get_org_owner_email(db, org_id)
        if owner is None:
            return
        to_email, full_name, org_name = owner
        email_service = get_email_service()
        await email_service.send_template_email(
            to_address=to_email,
            subject="Subscription changed",
            template_name="subscription_changed",
            context={
                "full_name": full_name,
                "org_name": org_name,
                "old_tier": old_tier,
                "new_tier": new_tier,
            },
        )


async def send_subscription_cancelled(
    org_id: uuid.UUID,
    tier: str,
) -> None:
    """Send subscription cancelled email to org owner.

    Raises on email service failure so Celery can retry.
    Returns silently if no org owner is found (not retryable).
    """
    async with get_postgres_session() as db:
        owner = await get_org_owner_email(db, org_id)
        if owner is None:
            return
        to_email, full_name, org_name = owner
        email_service = get_email_service()
        await email_service.send_template_email(
            to_address=to_email,
            subject="Subscription cancelled",
            template_name="subscription_cancelled",
            context={
                "full_name": full_name,
                "org_name": org_name,
                "tier": tier,
            },
        )
