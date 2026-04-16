"""Tests for shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_get_postgres_session_dep_yields_session() -> None:
    from dev_health_ops.api.dependencies import get_postgres_session_dep

    fake_session = MagicMock(spec=AsyncSession)

    class _FakeCtx:
        async def __aenter__(self) -> AsyncSession:
            return fake_session

        async def __aexit__(self, *a: object) -> None:
            return None

    with patch(
        "dev_health_ops.api.dependencies.get_postgres_session",
        return_value=_FakeCtx(),
    ):
        agen: AsyncGenerator[AsyncSession, None] = get_postgres_session_dep()
        session = await agen.__anext__()
        assert session is fake_session
        with pytest.raises(StopAsyncIteration):
            await agen.__anext__()
