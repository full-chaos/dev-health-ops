"""Verify get_current_user rejects JWTs for users not in the database."""

from __future__ import annotations

import uuid
from datetime import timedelta
from types import SimpleNamespace
from typing import TypedDict, Unpack
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.services.auth import AuthenticatedUser, AuthService


class _TokenOverrides(TypedDict, total=False):
    user_id: str
    email: str
    org_id: str
    role: str
    is_superuser: bool
    username: str | None
    full_name: str | None
    impersonating_user_id: str | None
    expires_delta: timedelta | None


def _make_token(
    auth_service: AuthService, **overrides: Unpack[_TokenOverrides]
) -> str:
    return auth_service.create_access_token(
        user_id=overrides.get("user_id", str(uuid.uuid4())),
        email=overrides.get("email", "ghost@example.com"),
        org_id=overrides.get("org_id", str(uuid.uuid4())),
        role=overrides.get("role", "member"),
        is_superuser=overrides.get("is_superuser", False),
        username=overrides.get("username"),
        full_name=overrides.get("full_name"),
        impersonating_user_id=overrides.get("impersonating_user_id"),
        expires_delta=overrides.get("expires_delta"),
    )


@pytest.fixture
def auth_service():
    return AuthService(secret_key="test-secret-for-db-check")


class _FakeResult:
    def __init__(self, row=None):
        self._row = row

    def one_or_none(self):
        return self._row


def _mock_session(execute_returns):
    session = AsyncMock()
    session.execute = AsyncMock(return_value=execute_returns)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _detail_message(detail: object) -> str:
    assert isinstance(detail, dict)
    message = detail["message"]
    assert isinstance(message, str)
    return message


@pytest.mark.asyncio
async def test_get_current_user_rejects_nonexistent_user(auth_service):
    from fastapi import HTTPException

    from dev_health_ops.api.auth.router import get_current_user

    token = _make_token(auth_service)
    header = f"Bearer {token}"

    session = _mock_session(_FakeResult(row=None))

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_ctx():
        yield session

    with (
        patch(
            "dev_health_ops.api.auth.router.get_auth_service", return_value=auth_service
        ),
        patch("dev_health_ops.api.auth.router.get_postgres_session", fake_ctx),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(authorization=header)
        assert exc_info.value.status_code == 401
        assert "no longer exists" in _detail_message(exc_info.value.detail)


@pytest.mark.asyncio
async def test_get_current_user_rejects_inactive_user(auth_service):
    from fastapi import HTTPException

    from dev_health_ops.api.auth.router import get_current_user

    user_id = uuid.uuid4()
    token = _make_token(auth_service, user_id=str(user_id))
    header = f"Bearer {token}"

    inactive_row = SimpleNamespace(id=user_id, is_active=False)
    session = _mock_session(_FakeResult(row=inactive_row))

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_ctx():
        yield session

    with (
        patch(
            "dev_health_ops.api.auth.router.get_auth_service", return_value=auth_service
        ),
        patch("dev_health_ops.api.auth.router.get_postgres_session", fake_ctx),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(authorization=header)
        assert exc_info.value.status_code == 401
        assert "disabled" in _detail_message(exc_info.value.detail)


@pytest.mark.asyncio
async def test_get_current_user_accepts_active_user(auth_service):
    from dev_health_ops.api.auth.router import get_current_user

    user_id = uuid.uuid4()
    token = _make_token(auth_service, user_id=str(user_id))
    header = f"Bearer {token}"

    active_row = SimpleNamespace(id=user_id, is_active=True)
    session = _mock_session(_FakeResult(row=active_row))

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_ctx():
        yield session

    with (
        patch(
            "dev_health_ops.api.auth.router.get_auth_service", return_value=auth_service
        ),
        patch("dev_health_ops.api.auth.router.get_postgres_session", fake_ctx),
    ):
        result = await get_current_user(authorization=header)
        assert isinstance(result, AuthenticatedUser)
        assert result.user_id == str(user_id)


@pytest.mark.asyncio
async def test_get_current_user_optional_returns_none_for_nonexistent(auth_service):
    from dev_health_ops.api.auth.router import get_current_user_optional

    token = _make_token(auth_service)
    header = f"Bearer {token}"

    session = _mock_session(_FakeResult(row=None))

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_ctx():
        yield session

    with (
        patch(
            "dev_health_ops.api.auth.router.get_auth_service", return_value=auth_service
        ),
        patch("dev_health_ops.api.auth.router.get_postgres_session", fake_ctx),
    ):
        result = await get_current_user_optional(authorization=header)
        assert result is None


@pytest.mark.asyncio
async def test_validate_endpoint_rejects_nonexistent_user(auth_service):
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from dev_health_ops.api.auth.router import router

    app = FastAPI()
    app.include_router(router)

    token = _make_token(auth_service)

    session = _mock_session(_FakeResult(row=None))

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_ctx():
        yield session

    with (
        patch(
            "dev_health_ops.api.auth.router.get_auth_service", return_value=auth_service
        ),
        patch("dev_health_ops.api.auth.router.get_postgres_session", fake_ctx),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/auth/validate",
                json={"token": token},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["valid"] is False
