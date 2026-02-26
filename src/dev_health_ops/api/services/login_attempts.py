from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import dev_health_ops.models.users as user_models

LOGIN_ATTEMPT_MODEL: Any = getattr(user_models, "LoginAttempt")

LOCKOUT_FAILURE_THRESHOLD = 5
LOCKOUT_DURATION = timedelta(minutes=15)


def _normalize_email(email: str) -> str:
    return email.lower().strip()


async def _get_attempt(db: AsyncSession, email: str) -> Any | None:
    normalized_email = _normalize_email(email)
    stmt = select(LOGIN_ATTEMPT_MODEL).where(
        func.lower(LOGIN_ATTEMPT_MODEL.email) == normalized_email
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def record_failed_attempt(db: AsyncSession, email: str) -> None:
    normalized_email = _normalize_email(email)
    now = datetime.now(timezone.utc)
    attempt = await _get_attempt(db, normalized_email)

    if attempt is None:
        attempt = LOGIN_ATTEMPT_MODEL(
            email=normalized_email,
            attempt_count=1,
            first_attempt_at=now,
            locked_until=None,
            created_at=now,
            updated_at=now,
        )
        db.add(attempt)
        await db.flush()
        return

    if attempt.locked_until is not None and attempt.locked_until > now:
        attempt.updated_at = now
        await db.flush()
        return

    if attempt.locked_until is not None and attempt.locked_until <= now:
        attempt.attempt_count = 0
        attempt.first_attempt_at = None
        attempt.locked_until = None

    if attempt.attempt_count == 0:
        attempt.first_attempt_at = now

    attempt.attempt_count += 1
    if attempt.attempt_count >= LOCKOUT_FAILURE_THRESHOLD:
        if attempt.first_attempt_at is None:
            attempt.first_attempt_at = now
        attempt.locked_until = now + LOCKOUT_DURATION

    attempt.updated_at = now
    await db.flush()


async def check_lockout(db: AsyncSession, email: str) -> bool:
    now = datetime.now(timezone.utc)
    attempt = await _get_attempt(db, email)
    if attempt is None or attempt.locked_until is None:
        return False

    if attempt.locked_until > now:
        return True

    attempt.attempt_count = 0
    attempt.first_attempt_at = None
    attempt.locked_until = None
    attempt.updated_at = now
    await db.flush()
    return False


async def clear_attempts(db: AsyncSession, email: str) -> None:
    attempt = await _get_attempt(db, email)
    if attempt is None:
        return

    await db.delete(attempt)
    await db.flush()


async def get_lockout_remaining_seconds(db: AsyncSession, email: str) -> int:
    attempt = await _get_attempt(db, email)
    if attempt is None or attempt.locked_until is None:
        return 0

    remaining = attempt.locked_until - datetime.now(timezone.utc)
    if remaining.total_seconds() <= 0:
        return 0
    return int(remaining.total_seconds())
