from __future__ import annotations

import hashlib
import hmac
import importlib
import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import bcrypt
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


async def create_password_reset_token(
    db: AsyncSession,
    user_id: uuid.UUID,
    ttl_hours: int = 1,
) -> str:
    password_reset_token_model = importlib.import_module(
        "dev_health_ops.models.password_reset_token"
    ).PasswordResetToken
    token_id = uuid.uuid4()
    token = _build_token(token_id)

    await db.execute(
        delete(password_reset_token_model).where(
            password_reset_token_model.user_id == user_id
        )
    )
    db.add(
        password_reset_token_model(
            id=token_id,
            user_id=user_id,
            token_hash=_hash_token(token),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=ttl_hours),
        )
    )
    await db.flush()
    return token


async def reset_password_with_token(
    db: AsyncSession,
    token: str,
    new_password: str,
) -> User | None:
    password_reset_token_model = importlib.import_module(
        "dev_health_ops.models.password_reset_token"
    ).PasswordResetToken
    validated_token = _validate_signed_token(token)
    if validated_token is None:
        return None

    token_hash = _hash_token(validated_token)
    now = datetime.now(timezone.utc)
    token_result = await db.execute(
        select(password_reset_token_model).where(
            password_reset_token_model.token_hash == token_hash,
            password_reset_token_model.expires_at >= now,
        )
    )
    token_record = token_result.scalar_one_or_none()
    if token_record is None:
        return None

    user_result = await db.execute(select(User).where(User.id == token_record.user_id))
    user = user_result.scalar_one_or_none()
    if user is None:
        await db.delete(token_record)
        await db.flush()
        return None

    password_hash = bcrypt.hashpw(
        new_password.encode("utf-8"),
        bcrypt.gensalt(),
    ).decode("utf-8")
    user.password_hash = password_hash
    user.updated_at = now

    revoke_all_for_user = importlib.import_module(
        "dev_health_ops.api.services.refresh_tokens"
    ).revoke_all_for_user
    await revoke_all_for_user(db, str(user.id))

    await db.execute(
        delete(password_reset_token_model).where(
            password_reset_token_model.user_id == user.id
        )
    )
    await db.flush()
    return user


async def send_password_reset_email(
    *,
    to_email: str,
    full_name: str | None,
    token: str,
) -> None:
    base_url = os.getenv("APP_BASE_URL", "http://localhost:3000").rstrip("/")
    reset_url = f"{base_url}/auth/reset-password?token={quote(token)}"
    email_service = importlib.import_module(
        "dev_health_ops.api.services.email"
    ).get_email_service()
    await email_service.send_template_email(
        to_address=to_email,
        subject="Reset your password",
        template_name="password_reset",
        context={
            "full_name": full_name or to_email,
            "reset_url": reset_url,
        },
    )
