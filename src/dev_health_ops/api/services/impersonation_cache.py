"""Valkey-backed shared cache for active impersonation sessions (CHAOS-2328).

The previous implementation was a per-process dict with a 30s TTL: stopping
an impersonation invalidated only the worker that handled the request, so
every OTHER api replica/worker kept serving the stale impersonation context
(wrong-org scoping) for up to 30s. Valkey is shared infrastructure for all
api processes, so an explicit DEL on start/stop is observed by every replica
on its next request.

Failure policy is fail-correct, not fail-stale: when Valkey is unavailable
the cache layer is bypassed and every lookup goes straight to Postgres. A
short circuit breaker bounds the per-request connect cost during an outage.
Only when Postgres is ALSO unavailable does impersonation silently not
activate (same as before).
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Entries self-expire as a drift bound (e.g. manual DB edits); correctness
# comes from the explicit DEL in the start/stop endpoints.
_TTL_SECONDS = 30

_KEY_PREFIX = "impersonation:active:"

# Negative-cache marker: "this admin has no active impersonation session".
# Caching the absence matters — it is the overwhelmingly common case for
# superuser traffic and saves a Postgres query per request.
_NONE_SENTINEL = b"none"

# Sentinel distinguishing "cache miss" from "cached None" in _cache_get.
_MISS: Any = object()

# Circuit breaker: after a Valkey error, skip the cache for a short window
# instead of paying the connect timeout on every request.
_CIRCUIT_SECONDS = 5.0
_circuit_open_until = 0.0

_client: Any | None = None


@dataclass(frozen=True)
class CachedImpersonationSession:
    """Plain snapshot of an active ImpersonationSession (no ORM identity).

    Carried across process boundaries via Valkey, so it must stay a simple
    serializable value object. ``target_email`` is denormalized here so the
    status endpoint never needs a Postgres read on the hot path.
    """

    id: str
    admin_user_id: str
    target_user_id: str
    target_org_id: str
    target_role: str
    target_email: str | None
    expires_at: datetime


def _key(admin_user_id: str) -> str:
    return f"{_KEY_PREFIX}{admin_user_id}"


def _get_client() -> Any | None:
    """Lazily create the shared Valkey client; None when unconfigured."""
    global _client
    if _client is not None:
        return _client
    redis_url = os.getenv("REDIS_URL", "")
    if not redis_url:
        return None
    import valkey.asyncio as aioredis

    _client = aioredis.from_url(
        redis_url,
        socket_connect_timeout=0.5,
        socket_timeout=0.5,
    )
    return _client


def _circuit_is_open() -> bool:
    return time.monotonic() < _circuit_open_until


def _trip_circuit(exc: Exception) -> None:
    global _circuit_open_until
    _circuit_open_until = time.monotonic() + _CIRCUIT_SECONDS
    logger.warning(
        "Valkey impersonation cache unavailable, bypassing for %.0fs: %s",
        _CIRCUIT_SECONDS,
        exc,
    )


def _serialize(session: CachedImpersonationSession | None) -> bytes:
    if session is None:
        return _NONE_SENTINEL
    return json.dumps(
        {
            "id": session.id,
            "admin_user_id": session.admin_user_id,
            "target_user_id": session.target_user_id,
            "target_org_id": session.target_org_id,
            "target_role": session.target_role,
            "target_email": session.target_email,
            "expires_at": session.expires_at.isoformat(),
        }
    ).encode()


def _deserialize(raw: bytes) -> CachedImpersonationSession | None:
    if raw == _NONE_SENTINEL:
        return None
    data = json.loads(raw)
    return CachedImpersonationSession(
        id=data["id"],
        admin_user_id=data["admin_user_id"],
        target_user_id=data["target_user_id"],
        target_org_id=data["target_org_id"],
        target_role=data["target_role"],
        target_email=data.get("target_email"),
        expires_at=datetime.fromisoformat(data["expires_at"]),
    )


async def _cache_get(admin_user_id: str) -> Any:
    """Return the cached value, or _MISS when absent/unavailable."""
    client = _get_client()
    if client is None or _circuit_is_open():
        return _MISS
    try:
        raw = await client.get(_key(admin_user_id))
    except Exception as exc:
        _trip_circuit(exc)
        return _MISS
    if raw is None:
        return _MISS
    try:
        return _deserialize(raw)
    except Exception:
        logger.warning("Corrupt impersonation cache entry for %s", admin_user_id)
        return _MISS


async def _cache_set(
    admin_user_id: str, session: CachedImpersonationSession | None
) -> None:
    client = _get_client()
    if client is None or _circuit_is_open():
        return
    try:
        await client.set(_key(admin_user_id), _serialize(session), ex=_TTL_SECONDS)
    except Exception as exc:
        _trip_circuit(exc)


async def get_active_session(
    admin_user_id: str,
) -> CachedImpersonationSession | None:
    """Return the active impersonation session for admin_user_id, or None.

    Checks the shared Valkey cache first; falls back to Postgres on miss or
    Valkey outage. Returns None when no session is active or both stores are
    unavailable (fail-open: impersonation does not activate).
    """
    cached = await _cache_get(admin_user_id)
    if cached is not _MISS:
        return cached

    session = await _load_from_db(admin_user_id)
    await _cache_set(admin_user_id, session)
    return session


async def _load_from_db(admin_user_id: str) -> CachedImpersonationSession | None:
    """Query Postgres for an active impersonation session. Fail-open on error."""
    try:
        from datetime import timezone

        from sqlalchemy import select

        from dev_health_ops.db import get_postgres_session
        from dev_health_ops.models.impersonation import ImpersonationSession
        from dev_health_ops.models.users import User

        async with get_postgres_session() as session:
            now = datetime.now(timezone.utc)
            stmt = (
                select(ImpersonationSession, User.email)
                .join(User, User.id == ImpersonationSession.target_user_id)
                .where(ImpersonationSession.admin_user_id == admin_user_id)
                .where(ImpersonationSession.ended_at.is_(None))
                .where(ImpersonationSession.expires_at > now)
                .limit(1)
            )
            result = await session.execute(stmt)
            row = result.first()
            if row is None:
                return None
            record, target_email = row
            return CachedImpersonationSession(
                id=str(record.id),
                admin_user_id=str(record.admin_user_id),
                target_user_id=str(record.target_user_id),
                target_org_id=str(record.target_org_id),
                target_role=str(record.target_role),
                target_email=str(target_email) if target_email else None,
                expires_at=record.expires_at,
            )
    except Exception:
        # Fail-open: if DB is unavailable, impersonation doesn't activate
        return None


async def invalidate(admin_user_id: str) -> None:
    """Remove the shared cache entry for admin_user_id (call after start/stop).

    Callers must COMMIT their transaction before invalidating — otherwise a
    racing reader can re-fill the shared cache from the pre-commit DB state
    and re-poison every replica for up to the TTL.
    """
    client = _get_client()
    if client is None or _circuit_is_open():
        return
    try:
        await client.delete(_key(admin_user_id))
    except Exception as exc:
        # Stale entries now survive at most _TTL_SECONDS — log loudly.
        _trip_circuit(exc)
        logger.error(
            "Failed to invalidate impersonation cache for %s — replicas may "
            "serve stale impersonation context for up to %ss",
            admin_user_id,
            _TTL_SECONDS,
        )
