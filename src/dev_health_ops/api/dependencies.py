"""Shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.db import get_postgres_session

__all__ = ["get_postgres_session_dep"]


async def get_postgres_session_dep() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async postgres session, managing lifecycle via ``get_postgres_session``."""
    async with get_postgres_session() as session:
        yield session
