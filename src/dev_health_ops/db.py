"""Dual-database connection management for Dev Health Ops.

This module provides session factories for both the semantic layer (PostgreSQL)
and analytics layer (ClickHouse).

Environment Variables:
    POSTGRES_URI: PostgreSQL connection string for semantic data (preferred)
    DATABASE_URI: PostgreSQL connection string (general-purpose alias)
    DATABASE_URL: PostgreSQL connection string (legacy alias)
    CLICKHOUSE_URI: ClickHouse connection string for analytics data
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

_postgres_engine: AsyncEngine | None = None
_clickhouse_engine: AsyncEngine | None = None


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _pgbouncer_transaction_mode() -> bool:
    """True when the semantic DB is reached through PgBouncer in transaction mode.

    Transaction pooling multiplexes a small set of server connections across many
    clients, which breaks server-side prepared statements. When enabled we let
    PgBouncer own the pool (NullPool) and stop asyncpg from caching/naming
    prepared statements. See docs/ops/database-connection-pooling.md.
    """
    return _is_truthy(os.getenv("PGBOUNCER_TRANSACTION_MODE"))


def _pg_pool_size() -> tuple[int, int]:
    """(pool_size, max_overflow) for the direct-connection path, env-overridable."""

    def _int_env(name: str, default: int) -> int:
        raw = os.getenv(name)
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    return _int_env("POSTGRES_POOL_SIZE", 20), _int_env("POSTGRES_MAX_OVERFLOW", 10)


def _async_postgres_engine_kwargs(uri: str) -> dict[str, Any]:
    """Build create_async_engine kwargs for the semantic Postgres engine.

    Behind PgBouncer transaction mode: NullPool + disabled asyncpg statement
    cache + unique prepared-statement names (requires SQLAlchemy >= 2.0.18).
    Otherwise: a SQLAlchemy QueuePool with pre-ping and env-tunable sizing.
    """
    is_postgres = uri.startswith("postgresql+")
    if is_postgres and _pgbouncer_transaction_mode():
        return {
            "poolclass": NullPool,
            "connect_args": {
                "statement_cache_size": 0,
                "prepared_statement_name_func": lambda: f"__asyncpg_{uuid4()}__",
            },
        }
    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if is_postgres:
        pool_size, max_overflow = _pg_pool_size()
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max_overflow
    return kwargs


def get_postgres_uri() -> str | None:
    """Get PostgreSQL connection URI with fallback chain."""
    uri = os.getenv("POSTGRES_URI")
    if uri:
        return _ensure_async_postgres(uri)

    fallback = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if fallback:
        return _ensure_async_postgres(fallback)

    return None


def get_clickhouse_uri() -> str | None:
    """Get ClickHouse connection URI."""
    return os.getenv("CLICKHOUSE_URI")


def _ensure_async_postgres(uri: str) -> str:
    """Ensure semantic DB URIs use an async driver."""
    if uri.startswith("postgresql://"):
        uri = uri.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif uri.startswith("sqlite://") and not uri.startswith("sqlite+aiosqlite://"):
        return uri.replace("sqlite://", "sqlite+aiosqlite://", 1)
    return _normalize_asyncpg_postgres_query(uri)


def _normalize_asyncpg_postgres_query(uri: str) -> str:
    if not uri.startswith("postgresql+asyncpg://"):
        return uri

    url = make_url(uri)
    query = dict(url.query)
    original_query = query.copy()
    sslmode = query.pop("sslmode", None)
    query.pop("channel_binding", None)

    if sslmode is not None and "ssl" not in query:
        query["ssl"] = sslmode

    if query == original_query:
        return uri

    return url.set(query=query).render_as_string(hide_password=False)


def get_postgres_engine() -> AsyncEngine:
    """Get or create the PostgreSQL async engine."""
    global _postgres_engine
    if _postgres_engine is None:
        uri = get_postgres_uri()
        if not uri:
            raise RuntimeError(
                "PostgreSQL URI not configured. Set POSTGRES_URI environment variable."
            )
        _postgres_engine = create_async_engine(
            uri, **_async_postgres_engine_kwargs(uri)
        )
    return _postgres_engine


def get_clickhouse_engine() -> AsyncEngine:
    """Get or create the ClickHouse async engine."""
    global _clickhouse_engine
    if _clickhouse_engine is None:
        uri = get_clickhouse_uri()
        if not uri:
            raise RuntimeError(
                "ClickHouse URI not configured. Set CLICKHOUSE_URI environment variable."
            )
        _clickhouse_engine = create_async_engine(uri)
    return _clickhouse_engine


@asynccontextmanager
async def get_postgres_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for PostgreSQL sessions."""
    engine = get_postgres_engine()
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_clickhouse_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for ClickHouse sessions."""
    engine = get_clickhouse_engine()
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def postgres_session_dependency() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for PostgreSQL sessions."""
    async with get_postgres_session() as session:
        yield session


async def clickhouse_session_dependency() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for ClickHouse sessions."""
    async with get_clickhouse_session() as session:
        yield session


def reset_async_engines() -> None:
    """Dispose global async engines so they are recreated on the next event loop.

    Call before ``asyncio.run()`` in Celery workers to avoid
    'Future attached to a different loop' errors.
    """
    global _postgres_engine, _clickhouse_engine
    for engine in (_postgres_engine, _clickhouse_engine):
        if engine is not None:
            engine.sync_engine.dispose()
    _postgres_engine = None
    _clickhouse_engine = None


async def close_engines() -> None:
    """Close all database engines. Call on application shutdown."""
    global _postgres_engine, _clickhouse_engine
    if _postgres_engine:
        await _postgres_engine.dispose()
        _postgres_engine = None
    if _clickhouse_engine:
        await _clickhouse_engine.dispose()
        _clickhouse_engine = None


def require_postgres_uri() -> str:
    """Get PostgreSQL URI or raise with helpful error message."""
    uri = get_postgres_uri()
    if not uri:
        raise RuntimeError(
            "PostgreSQL URI not configured.\n"
            "Set POSTGRES_URI environment variable or pass --db flag.\n"
            "Example: postgresql+asyncpg://user:pass@localhost:5432/devhealth"
        )
    return uri


def require_clickhouse_uri() -> str:
    """Get ClickHouse URI or raise with helpful error message."""
    uri = get_clickhouse_uri()
    if not uri:
        raise RuntimeError(
            "ClickHouse URI not configured.\n"
            "Set CLICKHOUSE_URI environment variable or pass --analytics-db flag.\n"
            "Example: clickhouse://ch:ch@localhost:8123/default"
        )
    return uri


def resolve_db_uri(ns) -> str:
    uri = getattr(ns, "db", None)
    if uri:
        return _ensure_async_postgres(uri)
    return require_postgres_uri()


def resolve_sink_uri(ns) -> str:
    uri = getattr(ns, "analytics_db", None)
    if uri:
        _validate_sink_uri(uri, ns)
        return uri
    uri = require_clickhouse_uri()
    _validate_sink_uri(uri, ns)
    return uri


def _validate_sink_uri(uri: str, ns) -> None:
    try:
        validate_sink_uri_scheme(uri)
    except ValueError as exc:
        parser = getattr(ns, "_leaf_parser", None)
        if parser is not None:
            parser.error(str(exc))
        raise


def validate_sink_uri_scheme(uri: str) -> None:
    from dev_health_ops.metrics.sinks.factory import detect_backend

    detect_backend(uri)


_postgres_sync_engine: Engine | None = None


def _get_sync_postgres_uri() -> str | None:
    uri = os.getenv("POSTGRES_URI")
    if uri:
        if uri.startswith("postgresql+asyncpg://"):
            return uri.replace("postgresql+asyncpg://", "postgresql://", 1)
        return uri

    fallback = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if fallback:
        if "asyncpg" in fallback:
            return fallback.replace("+asyncpg", "", 1)
        return fallback

    return None


def _ensure_sync_postgres(uri: str) -> str:
    if uri.startswith("postgresql+asyncpg://"):
        return uri.replace("postgresql+asyncpg://", "postgresql://", 1)
    if uri.startswith("sqlite+aiosqlite://"):
        return uri.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return uri


def get_postgres_sync_engine(uri: str | None = None) -> Engine:
    global _postgres_sync_engine
    if uri is not None:
        sync_uri = _ensure_sync_postgres(uri)
        if _pgbouncer_transaction_mode():
            return create_engine(sync_uri, poolclass=NullPool)
        if not sync_uri.startswith("postgresql"):
            return create_engine(sync_uri)
        pool_size, max_overflow = _pg_pool_size()
        return create_engine(
            sync_uri,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
        )
    if _postgres_sync_engine is None:
        uri = _get_sync_postgres_uri()
        if not uri:
            raise RuntimeError(
                "PostgreSQL URI not configured. Set POSTGRES_URI environment variable."
            )
        if _pgbouncer_transaction_mode():
            # PgBouncer owns the pool; psycopg keeps no server-side prepared
            # statements by default, so only the pool class needs to change.
            _postgres_sync_engine = create_engine(uri, poolclass=NullPool)
        else:
            pool_size, max_overflow = _pg_pool_size()
            _postgres_sync_engine = create_engine(
                uri,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
            )
    return _postgres_sync_engine


def reset_sync_engine() -> None:
    """Dispose and clear the cached global sync Postgres engine.

    ``get_postgres_sync_engine()`` caches the engine keyed off POSTGRES_URI on
    first use. Tests that monkeypatch POSTGRES_URI per test must reset it so the
    next caller binds to the current env instead of a stale engine pointing at a
    previous test's database -- the cross-test / cross-xdist-worker pollution
    behind CHAOS-2586.
    """
    global _postgres_sync_engine
    if _postgres_sync_engine is not None:
        _postgres_sync_engine.dispose()
        _postgres_sync_engine = None


@contextmanager
def get_postgres_session_sync() -> Generator[Session, None, None]:
    engine = get_postgres_sync_engine()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_postgres_session_sync_for_uri(uri: str) -> Generator[Session, None, None]:
    engine = get_postgres_sync_engine(uri)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()
