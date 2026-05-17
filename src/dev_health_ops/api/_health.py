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
    """Check Postgres connectivity. Returns ``("postgres", status)``."""
    uri = _postgres_url()
    if not uri:
        return "postgres", "not_configured"
    if _dsn_uses_async_driver(uri):
        ok = await _check_sqlalchemy_health_async(uri)
    else:
        ok = await asyncio.to_thread(_check_sqlalchemy_health, uri)
    return "postgres", "ok" if ok else "down"


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


__all__ = [
    "DEFAULT_CLICKHOUSE_URI",
    "_analytics_db_url",
    "_check_celery_health",
    "_check_clickhouse_health",
    "_check_postgres_health",
    "_check_redis_health",
    "_check_sqlalchemy_health",
    "_check_sqlalchemy_health_async",
    "_clickhouse_url",
    "_db_url",
    "_dsn_uses_async_driver",
    "_postgres_url",
]
