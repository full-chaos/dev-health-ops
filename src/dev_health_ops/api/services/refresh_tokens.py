from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.refresh_token import RefreshToken


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def create_refresh_token(
    db: AsyncSession,
    user_id: str,
    org_id: str,
    token_hash: str,
    family_id: str,
    expires_at: datetime,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> RefreshToken:
    record = RefreshToken(
        user_id=uuid.UUID(user_id),
        org_id=uuid.UUID(org_id),
        token_hash=_hash_token(token_hash),
        family_id=uuid.UUID(family_id),
        expires_at=expires_at,
        ip_address=ip_address,
        user_agent=user_agent,
    )
    db.add(record)
    await db.flush()
    return record


async def find_by_hash(db: AsyncSession, token_hash: str) -> RefreshToken | None:
    stmt = select(RefreshToken).where(
        RefreshToken.token_hash == _hash_token(token_hash)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def rotate_token(
    db: AsyncSession,
    old_token_hash: str,
    new_token_hash: str,
    new_expires_at: datetime,
) -> RefreshToken | None:
    old_record = await find_by_hash(db, old_token_hash)
    if old_record is None:
        return None

    new_hash = _hash_token(new_token_hash)
    now = datetime.now(timezone.utc)

    old_record.replaced_by_hash = new_hash
    old_record.revoked_at = now

    new_record = RefreshToken(
        user_id=old_record.user_id,
        org_id=old_record.org_id,
        token_hash=new_hash,
        family_id=old_record.family_id,
        expires_at=new_expires_at,
        ip_address=old_record.ip_address,
        user_agent=old_record.user_agent,
    )
    db.add(new_record)
    await db.flush()
    return new_record


async def revoke_token(db: AsyncSession, token_hash: str) -> bool:
    now = datetime.now(timezone.utc)
    stmt = (
        update(RefreshToken)
        .where(
            RefreshToken.token_hash == _hash_token(token_hash),
            RefreshToken.revoked_at.is_(None),
        )
        .values(revoked_at=now)
    )
    result = await db.execute(stmt)
    return bool(result.rowcount)


async def revoke_all_for_user(db: AsyncSession, user_id: str) -> int:
    stmt = (
        update(RefreshToken)
        .where(
            RefreshToken.user_id == uuid.UUID(user_id),
            RefreshToken.revoked_at.is_(None),
        )
        .values(revoked_at=datetime.now(timezone.utc))
    )
    result = await db.execute(stmt)
    return int(result.rowcount or 0)


async def revoke_family(db: AsyncSession, family_id: str) -> int:
    stmt = (
        update(RefreshToken)
        .where(
            RefreshToken.family_id == uuid.UUID(family_id),
            RefreshToken.revoked_at.is_(None),
        )
        .values(revoked_at=datetime.now(timezone.utc))
    )
    result = await db.execute(stmt)
    return int(result.rowcount or 0)


async def cleanup_expired(db: AsyncSession) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    stmt = delete(RefreshToken).where(RefreshToken.expires_at < cutoff)
    result = await db.execute(stmt)
    return int(result.rowcount or 0)
