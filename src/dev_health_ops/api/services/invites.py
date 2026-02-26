from __future__ import annotations

import hashlib
import hmac
import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.email import get_email_service
from dev_health_ops.models.org_invite import OrgInvite
from dev_health_ops.models.users import Membership


def _token_secret() -> str:
    return (
        os.getenv("JWT_SECRET_KEY")
        or os.getenv("SETTINGS_ENCRYPTION_KEY")
        or "dev-key-not-for-prod"
    )


def _sign_token(token_id: uuid.UUID) -> str:
    return hmac.new(
        _token_secret().encode("utf-8"),
        token_id.hex.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _build_token(token_id: uuid.UUID) -> str:
    return f"{token_id.hex}.{_sign_token(token_id)}"


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _validate_signed_token(token: str) -> str | None:
    token_id_str, separator, signature = token.partition(".")
    if not separator or not token_id_str or not signature:
        return None
    try:
        token_id = uuid.UUID(token_id_str)
    except ValueError:
        return None
    expected = _sign_token(token_id)
    if not hmac.compare_digest(signature, expected):
        return None
    return token


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


async def create_invite(
    db: AsyncSession,
    org_id: uuid.UUID,
    email: str,
    role: str,
    invited_by_id: uuid.UUID,
    ttl_hours: int = 72,
) -> tuple[OrgInvite, str]:
    now = datetime.now(timezone.utc)
    normalized_email = email.lower().strip()

    existing_result = await db.execute(
        select(OrgInvite).where(
            OrgInvite.org_id == org_id,
            func.lower(OrgInvite.email) == normalized_email,
            OrgInvite.status == "pending",
            OrgInvite.expires_at >= now,
        )
    )
    existing = existing_result.scalar_one_or_none()
    if existing is not None:
        raise ValueError("A pending invite already exists for this email")

    token_id = uuid.uuid4()
    token = _build_token(token_id)
    invite = OrgInvite(
        id=token_id,
        org_id=org_id,
        email=normalized_email,
        role=role or "member",
        token_hash=_hash_token(token),
        invited_by_id=invited_by_id,
        status="pending",
        expires_at=now + timedelta(hours=ttl_hours),
    )
    db.add(invite)
    await db.flush()
    return invite, token


async def validate_invite(db: AsyncSession, token: str) -> OrgInvite | None:
    validated_token = _validate_signed_token(token)
    if validated_token is None:
        return None

    token_hash = _hash_token(validated_token)
    result = await db.execute(
        select(OrgInvite).where(OrgInvite.token_hash == token_hash)
    )
    invite = result.scalar_one_or_none()
    if invite is None:
        return None
    if invite.status != "pending":
        return None

    now = datetime.now(timezone.utc)
    if _as_utc(invite.expires_at) < now:
        invite.status = "expired"
        invite.updated_at = now
        await db.flush()
        return None
    return invite


async def accept_invite(
    db: AsyncSession,
    invite: OrgInvite,
    user_id: uuid.UUID,
) -> Membership:
    now = datetime.now(timezone.utc)
    if invite.status != "pending" or _as_utc(invite.expires_at) < now:
        raise ValueError("Invite is not valid")

    existing_membership_result = await db.execute(
        select(Membership).where(
            Membership.org_id == invite.org_id,
            Membership.user_id == user_id,
        )
    )
    existing_membership = existing_membership_result.scalar_one_or_none()
    if existing_membership is not None:
        raise ValueError("User is already a member of this organization")

    membership = Membership(
        id=uuid.uuid4(),
        user_id=user_id,
        org_id=invite.org_id,
        role=invite.role,
        invited_by_id=invite.invited_by_id,
        joined_at=now,
        created_at=now,
        updated_at=now,
    )
    db.add(membership)

    invite.status = "accepted"
    invite.accepted_at = now
    invite.updated_at = now

    await db.flush()
    return membership


async def send_invite_email(
    *,
    to_email: str,
    org_name: str,
    inviter_name: str,
    token: str,
) -> None:
    base_url = os.getenv("APP_BASE_URL", "http://localhost:8000").rstrip("/")
    accept_url = f"{base_url}/accept-invite?token={quote(token)}"
    email_service = get_email_service()
    await email_service.send_template_email(
        to_address=to_email,
        subject=f"You're invited to join {org_name}",
        template_name="invite",
        context={
            "org_name": org_name,
            "inviter_name": inviter_name,
            "accept_url": accept_url,
        },
    )
