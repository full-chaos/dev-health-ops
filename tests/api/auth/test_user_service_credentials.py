from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.users import UserService
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import User
from tests._helpers import tables_of


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "user-service-credentials.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(User, RefreshToken),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_create_empty_string_password_raises(session_maker):
    async with session_maker() as session:
        svc = UserService(session)
        with pytest.raises(ValueError, match="Password must be at least 8 characters"):
            await svc.create(email="empty-password@example.com", password="")


@pytest.mark.asyncio
async def test_create_none_password_creates_passwordless_user(session_maker):
    async with session_maker() as session:
        svc = UserService(session)
        user = await svc.create(email="passwordless@example.com", password=None)
        await session.commit()

    async with session_maker() as session:
        result = await session.execute(select(User).where(User.id == user.id))
        created_user = result.scalar_one()

    assert created_user.password_hash is None
    assert created_user.token_version == 0
