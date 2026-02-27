"""In-process TTL cache for active impersonation sessions.

Caches active ImpersonationSession lookups to avoid a DB query on every
request. TTL is 30 seconds — short enough for responsive UX while cutting
per-request DB cost during impersonation. The cache is unbounded (no
max-size eviction); entries are evicted only when their TTL expires.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from dev_health_ops.models.impersonation import ImpersonationSession

_TTL_SECONDS: float = 30.0
# {admin_user_id: (session_or_none, expiry_timestamp)}
_cache: dict[str, tuple[Optional["ImpersonationSession"], float]] = {}


def _is_expired(expiry: float) -> bool:
    return time.monotonic() > expiry


async def get_active_session(admin_user_id: str) -> Optional["ImpersonationSession"]:
    """Return the active ImpersonationSession for admin_user_id, or None.

    Checks in-memory cache first; falls back to DB on miss.
    Returns None if no session is active or DB lookup fails (fail-open).
    """
    entry = _cache.get(admin_user_id)
    if entry is not None:
        session, expiry = entry
        if not _is_expired(expiry):
            return session
        # Expired — remove from cache and fall through to DB
        del _cache[admin_user_id]

    # DB lookup
    session = await _load_from_db(admin_user_id)
    _cache[admin_user_id] = (session, time.monotonic() + _TTL_SECONDS)
    return session


async def _load_from_db(admin_user_id: str) -> Optional["ImpersonationSession"]:
    """Query Postgres for an active impersonation session. Fail-open on error."""
    try:
        from dev_health_ops.db import get_postgres_session
        from dev_health_ops.models.impersonation import ImpersonationSession
        from sqlalchemy import select
        from datetime import datetime, timezone

        async with get_postgres_session() as session:
            now = datetime.now(timezone.utc)
            stmt = (
                select(ImpersonationSession)
                .where(ImpersonationSession.admin_user_id == admin_user_id)
                .where(ImpersonationSession.ended_at.is_(None))
                .where(ImpersonationSession.expires_at > now)
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
    except Exception:
        # Fail-open: if DB is unavailable, impersonation doesn't activate
        return None


def invalidate(admin_user_id: str) -> None:
    """Remove the cached entry for admin_user_id (call after start/stop)."""
    _cache.pop(admin_user_id, None)
