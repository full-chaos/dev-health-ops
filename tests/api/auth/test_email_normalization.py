"""Verify register and login normalize email to lowercase."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.auth.router import router


@pytest.fixture
def app():
    _app = FastAPI()
    _app.include_router(router)
    return _app


def _mock_session(*, scalar_return=None, flush_side_effect=None):
    """Build an AsyncMock session that behaves like get_postgres_session()."""
    session = AsyncMock()

    fake_result = MagicMock()
    fake_result.scalar_one_or_none.return_value = scalar_return
    session.execute = AsyncMock(return_value=fake_result)
    session.flush = AsyncMock(side_effect=flush_side_effect)
    session.commit = AsyncMock()
    session.add = MagicMock()
    return session


@asynccontextmanager
async def _fake_session_ctx(session):
    yield session


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_stores_email_lowercase(app):
    """Registering with mixed-case email should store it lowercased."""
    user_id = uuid.uuid4()

    def _set_id_on_flush():
        """Simulate DB assigning an id on flush."""
        # The User object is the first positional arg to session.add
        user_obj = session.add.call_args_list[0][0][0]
        user_obj.id = user_id

    session = _mock_session(
        scalar_return=None,  # no existing user
        flush_side_effect=_set_id_on_flush,
    )

    # Second flush (for org) needs to set org.id too
    org_id = uuid.uuid4()

    call_count = 0

    async def _flush_side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # User flush
            user_obj = session.add.call_args_list[0][0][0]
            user_obj.id = user_id
        elif call_count == 2:
            # Org flush
            org_obj = session.add.call_args_list[1][0][0]
            org_obj.id = org_id

    session.flush = AsyncMock(side_effect=_flush_side_effect)

    with patch(
        "dev_health_ops.api.auth.router.get_postgres_session",
        lambda: _fake_session_ctx(session),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/auth/register",
                json={
                    "email": "Test@Example.COM",
                    "password": "securepassword123",
                    "full_name": "Test User",
                },
            )
            assert resp.status_code == 201, resp.text

    # Verify the User object was created with lowercased email
    user_obj = session.add.call_args_list[0][0][0]
    assert user_obj.email == "test@example.com"


@pytest.mark.asyncio
async def test_register_detects_existing_user_case_insensitive(app):
    """Registering with different case of existing email should be rejected."""
    existing_user = SimpleNamespace(
        id=uuid.uuid4(),
        email="test@example.com",
    )
    session = _mock_session(scalar_return=existing_user)

    with patch(
        "dev_health_ops.api.auth.router.get_postgres_session",
        lambda: _fake_session_ctx(session),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/auth/register",
                json={
                    "email": "TEST@Example.com",
                    "password": "securepassword123",
                },
            )
            assert resp.status_code == 400
            assert "already registered" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_login_finds_user_case_insensitive(app):
    """Login should find user regardless of email case."""
    import bcrypt

    password = "securepassword123"
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode(
        "utf-8"
    )

    user_id = uuid.uuid4()
    org_id = uuid.uuid4()

    existing_user = SimpleNamespace(
        id=user_id,
        email="test@example.com",
        username=None,
        full_name="Test User",
        password_hash=password_hash,
        is_active=True,
        is_superuser=False,
        last_login_at=None,
    )

    membership = SimpleNamespace(
        org_id=org_id,
        role="owner",
    )

    session = _mock_session(scalar_return=existing_user)

    # Login hits execute twice: once for user, once for membership
    call_count = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            result = MagicMock()
            result.scalar_one_or_none.return_value = existing_user
            return result
        else:
            result = MagicMock()
            result.scalar_one_or_none.return_value = membership
            return result

    session.execute = AsyncMock(side_effect=_execute_side_effect)

    with patch(
        "dev_health_ops.api.auth.router.get_postgres_session",
        lambda: _fake_session_ctx(session),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/auth/login",
                json={
                    "email": "TEST@Example.com",  # Different case than stored
                    "password": password,
                },
            )
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert "access_token" in data


@pytest.mark.asyncio
async def test_login_nonexistent_user_returns_401(app):
    """Login with email that doesn't exist should return 401."""
    session = _mock_session(scalar_return=None)

    with patch(
        "dev_health_ops.api.auth.router.get_postgres_session",
        lambda: _fake_session_ctx(session),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/auth/login",
                json={
                    "email": "nonexistent@example.com",
                    "password": "whatever",
                },
            )
            assert resp.status_code == 401
