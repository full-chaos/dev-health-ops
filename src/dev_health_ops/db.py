"""Dual-database connection management for Dev Health Ops.

This module provides session factories for both the semantic layer (PostgreSQL)
and analytics layer (ClickHouse).

Environment Variables:
    POSTGRES_URI: PostgreSQL connection string for semantic data
    CLICKHOUSE_URI: ClickHouse connection string for analytics data
    DATABASE_URI: Legacy fallback (defaults to ClickHouse behavior)
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker, Session


_postgres_engine: AsyncEngine | None = None
_clickhouse_engine: AsyncEngine | None = None


def get_postgres_uri() -> str | None:
    """Get PostgreSQL connection URI with fallback chain."""
    uri = os.getenv("POSTGRES_URI")
    if uri:
        return _ensure_async_postgres(uri)

    fallback = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if fallback and "postgres" in fallback.lower():
        return _ensure_async_postgres(fallback)

    return None


def get_clickhouse_uri() -> str | None:
    """Get ClickHouse connection URI with fallback chain."""
    uri = os.getenv("CLICKHOUSE_URI")
    if uri:
        return uri

    fallback = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if fallback and "clickhouse" in fallback.lower():
        return fallback

    return None


def _ensure_async_postgres(uri: str) -> str:
    """Ensure PostgreSQL URI uses asyncpg driver."""
    if uri.startswith("postgresql://"):
        return uri.replace("postgresql://", "postgresql+asyncpg://", 1)
    return uri


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
            uri,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
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
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
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
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
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
        return uri
    return require_clickhouse_uri()


_postgres_sync_engine: Engine | None = None


def _get_sync_postgres_uri() -> str | None:
    uri = os.getenv("POSTGRES_URI")
    if uri:
        if uri.startswith("postgresql+asyncpg://"):
            return uri.replace("postgresql+asyncpg://", "postgresql://", 1)
        return uri

    fallback = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if fallback and "postgres" in fallback.lower():
        if "asyncpg" in fallback:
            return fallback.replace("+asyncpg", "", 1)
        return fallback

    return None


def get_postgres_sync_engine() -> Engine:
    global _postgres_sync_engine
    if _postgres_sync_engine is None:
        uri = _get_sync_postgres_uri()
        if not uri:
            raise RuntimeError(
                "PostgreSQL URI not configured. Set POSTGRES_URI environment variable."
            )
        _postgres_sync_engine = create_engine(
            uri,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return _postgres_sync_engine


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
