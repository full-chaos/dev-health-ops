from __future__ import annotations

import hashlib
import hmac
import importlib
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models.users import User


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


async def create_email_verification_token(
    db: AsyncSession,
    user_id: uuid.UUID,
    ttl_hours: int = 24,
) -> str:
    email_verification_token_model = importlib.import_module(
        "dev_health_ops.models.email_verification_token"
    ).EmailVerificationToken
    token_id = uuid.uuid4()
    token = _build_token(token_id)

    await db.execute(
        delete(email_verification_token_model).where(
            email_verification_token_model.user_id == user_id
        )
    )
    db.add(
        email_verification_token_model(
            id=token_id,
            user_id=user_id,
            token_hash=_hash_token(token),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=ttl_hours),
        )
    )
    await db.flush()
    return token


async def verify_email_token(db: AsyncSession, token: str) -> User | None:
    email_verification_token_model = importlib.import_module(
        "dev_health_ops.models.email_verification_token"
    ).EmailVerificationToken
    validated_token = _validate_signed_token(token)
    if validated_token is None:
        return None

    token_hash = _hash_token(validated_token)
    now = datetime.now(timezone.utc)
    token_result = await db.execute(
        select(email_verification_token_model).where(
            email_verification_token_model.token_hash == token_hash,
            email_verification_token_model.expires_at >= now,
        )
    )
    token_record = token_result.scalar_one_or_none()
    if token_record is None:
        return None

    user_result = await db.execute(select(User).where(User.id == token_record.user_id))
    user: Any | None = user_result.scalar_one_or_none()
    if user is None:
        await db.delete(token_record)
        await db.flush()
        return None

    user.is_verified = True
    user.updated_at = now
    await db.execute(
        delete(email_verification_token_model).where(
            email_verification_token_model.user_id == user.id
        )
    )
    await db.flush()
    return user


async def send_verification_email(
    *,
    to_email: str,
    full_name: str | None,
    token: str,
) -> None:
    base_url = os.getenv("APP_BASE_URL", "http://localhost:3000").rstrip("/")
    verification_url = f"{base_url}/auth/verify?token={quote(token)}"
    email_service = importlib.import_module(
        "dev_health_ops.api.services.email"
    ).get_email_service()
    await email_service.send_template_email(
        to_address=to_email,
        subject="Verify your email address",
        template_name="verification",
        context={
            "full_name": full_name or to_email,
            "verification_url": verification_url,
        },
    )
