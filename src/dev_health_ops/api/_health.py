"""Database-URL resolution and health-check helpers.

Extracted from ``api.main`` to keep ``main.py`` focused on composition. These
helpers are re-exported from ``api.main`` to preserve any
``monkeypatch.setattr("dev_health_ops.api.main._check_*", ...)`` style usage
in the test suite.
"""

from __future__ import annotations

import asyncio
import os
from urllib.parse import urlparse

from dev_health_ops.api.middleware.rate_limit import LIMITER_BACKEND
from dev_health_ops.db import get_postgres_uri

from .queries.client import clickhouse_client, query_dicts

DEFAULT_CLICKHOUSE_URI = "clickhouse://localhost:8123/default"


def _db_url() -> str:
    """Return the configured DATABASE URL or raise if none is set."""
    dsn = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn

    raise RuntimeError(
        "Database configuration is missing: set DATABASE_URI or DATABASE_URL "
        "(e.g. 'clickhouse://localhost:8123/default')."
    )


def _postgres_url() -> str | None:
    """Return the Postgres URL, falling back to ``DATABASE_URI``/``DATABASE_URL``."""
    uri = os.getenv("POSTGRES_URI")
    if uri:
        return uri
    fallback = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if fallback and "postgres" in fallback.lower():
        return fallback
    return None


def _clickhouse_url() -> str:
    """Return the ClickHouse URL or a local-development default."""
    return os.getenv("CLICKHOUSE_URI") or DEFAULT_CLICKHOUSE_URI


def _analytics_db_url() -> str:
    """Return the ClickHouse URL required for analytics queries, or raise.

    Unlike :func:`_clickhouse_url`, this helper does not fall back to a default
    — analytics endpoints fail fast when ``CLICKHOUSE_URI`` is missing.
    """
    uri = os.getenv("CLICKHOUSE_URI")
    if not uri:
        raise RuntimeError(
            "CLICKHOUSE_URI is required for analytics queries "
            "(e.g. 'clickhouse://localhost:8123/default')."
        )
    return uri


def _check_sqlalchemy_health(dsn: str) -> bool:
    """Sync SQLAlchemy ping. Returns ``True`` on success, ``False`` on any error."""
    from sqlalchemy import create_engine, text

    engine = create_engine(dsn, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        engine.dispose()


async def _check_sqlalchemy_health_async(dsn: str) -> bool:
    """Async SQLAlchemy ping. Returns ``True`` on success, ``False`` on any error."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(dsn, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()


def _dsn_uses_async_driver(dsn: str) -> bool:
    """Return True when ``dsn`` uses an async SQLAlchemy driver (asyncpg/aiosqlite)."""
    scheme = urlparse(dsn).scheme.lower()
    return "+asyncpg" in scheme or "+aiosqlite" in scheme


async def _check_postgres_health() -> tuple[str, str]:
    """Check Postgres connectivity and required schema revision.

    Returns ``("postgres", status)``. ``status`` is ``"down"`` both for raw
    connectivity failure and for a reachable database missing the required
    application schema revision (CHAOS-3299) -- this closes the window the
    one-time ``_lifespan`` startup check cannot: multi-replica rollouts where
    one replica boots successfully before a migration completes elsewhere, or
    an operator manually rolling the database back without restarting the
    process. Re-checked on every probe.
    """
    if not _postgres_url():
        return "postgres", "not_configured"
    uri = get_postgres_uri()
    if not uri:
        return "postgres", "not_configured"
    if _dsn_uses_async_driver(uri):
        ok = await _check_sqlalchemy_health_async(uri)
    else:
        ok = await asyncio.to_thread(_check_sqlalchemy_health, uri)
    if not ok:
        return "postgres", "down"
    from dev_health_ops.migrate import application_schema_status

    try:
        satisfied, _current_heads = await application_schema_status(uri)
    except Exception:
        return "postgres", "down"
    return "postgres", "ok" if satisfied else "down"


async def _check_clickhouse_health() -> tuple[str, str]:
    """Check ClickHouse connectivity. Returns ``("clickhouse", status)``."""
    uri = _clickhouse_url()
    if not uri:
        return "clickhouse", "not_configured"
    try:
        async with clickhouse_client(uri) as sink:
            rows = await query_dicts(sink, "SELECT 1 AS ok", {})
        return "clickhouse", "ok" if rows else "down"
    except Exception:
        return "clickhouse", "down"


async def _check_redis_health() -> tuple[str, str]:
    """Ping Redis directly to verify connectivity."""
    redis_url = os.getenv("REDIS_URL", "")
    if not redis_url:
        return "redis", "not_configured"
    try:
        import valkey.asyncio as aioredis

        client = aioredis.from_url(redis_url, socket_connect_timeout=2)
        try:
            await client.ping()
            return "redis", "ok"
        finally:
            await client.aclose()
    except Exception:
        return "redis", "down"


async def _check_celery_health() -> tuple[str, str]:
    """Inspect active Celery workers via the broker."""
    try:
        from dev_health_ops.workers.celery_app import celery_app

        # Use inspect with a very short timeout so health checks stay fast.
        inspect = celery_app.control.inspect(timeout=1.5)
        active = await asyncio.to_thread(inspect.ping)
        if active:
            return "celery", "ok"
        return "celery", "no_workers"
    except Exception:
        return "celery", "down"


def _expected_worker_groups() -> list[str] | None:
    """Return the worker groups this deployment expects to be running.

    Sourced from ``EXPECTED_WORKER_GROUPS`` (comma-separated, e.g.
    ``"heavy,ops,sync,sync-provider"`` -- the ``--worker-group`` values
    ``cmd/dev-health-worker`` is deployed with). Full Chaos's own production
    is Go-only and sets this; self-hosted/customer deployments that still run
    Celery leave it unset, so ``/health/workers`` stays Celery-authoritative
    for them (CHAOS-3942).

    Returns ``None`` when the variable is unset (no Go fleet declared). Returns
    a (possibly empty) list when it IS set -- an empty result there means the
    value was present but malformed (e.g. ``","`` or whitespace), which the
    caller must treat as a misconfiguration, not as "no groups declared".
    """
    raw = os.environ.get("EXPECTED_WORKER_GROUPS")
    if raw is None:
        return None
    return [group.strip() for group in raw.split(",") if group.strip()]


async def _check_go_worker_presence(expected_groups: list[str]) -> dict[str, str]:
    """Check each expected Go worker group for a live heartbeat row.

    Queries ``public.worker_instances`` directly -- the table
    ``internal/jobruntime/worker_presence.go`` writes TTL heartbeats into.
    Mirrors ``ReadWorkerPresence``'s Go semantics: a group counts as present
    when it has a row with ``expires_at > now()``, regardless of whether that
    row is ``accepting`` or ``draining`` (draining is a live worker finishing
    in-flight work during a rolling deploy, not an absent one).

    Returns ``{group: "ok" | "absent"}`` for every group in
    ``expected_groups``. If Postgres itself is unreachable -- including a
    raw ``postgresql://`` DSN that ``create_async_engine`` can't drive
    without normalization -- every group is reported ``"unknown"`` -- never
    ``"ok"`` -- so a database outage cannot masquerade as a healthy worker
    fleet.
    """
    uri = get_postgres_uri()
    if not uri:
        return {group: "unknown" for group in expected_groups}

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    try:
        engine = create_async_engine(uri, pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT DISTINCT worker_group FROM public.worker_instances "
                        "WHERE expires_at > statement_timestamp()"
                    )
                )
                live_groups = {row[0] for row in result}
        finally:
            await engine.dispose()
    except Exception:
        return {group: "unknown" for group in expected_groups}

    return {
        group: "ok" if group in live_groups else "absent" for group in expected_groups
    }


async def _check_rate_limiter_backend() -> tuple[str, str]:
    """Report the configured rate-limiter storage backend (redis / memory / noop).

    Informational only — not included in the overall health status.
    """
    return "rate_limiter", LIMITER_BACKEND


__all__ = [
    "DEFAULT_CLICKHOUSE_URI",
    "_analytics_db_url",
    "_check_celery_health",
    "_check_clickhouse_health",
    "_check_go_worker_presence",
    "_check_postgres_health",
    "_check_rate_limiter_backend",
    "_check_redis_health",
    "_check_sqlalchemy_health",
    "_check_sqlalchemy_health_async",
    "_clickhouse_url",
    "_db_url",
    "_dsn_uses_async_driver",
    "_expected_worker_groups",
    "_postgres_url",
]
