from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import bcrypt
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.auth.router import router
from dev_health_ops.api.services.auth import AuthService


@pytest.fixture
def app():
    _app = FastAPI()
    _app.include_router(router)
    return _app


@asynccontextmanager
async def _fake_session_ctx(session):
    yield session


def _password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _user(
    *,
    email: str,
    password: str,
    is_verified: bool,
    auth_provider: str = "local",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        email=email,
        username=None,
        full_name="Test User",
        password_hash=_password_hash(password),
        is_active=True,
        is_verified=is_verified,
        auth_provider=auth_provider,
        is_superuser=False,
        last_login_at=None,
    )


def _membership(org_id: uuid.UUID | None = None) -> SimpleNamespace:
    return SimpleNamespace(org_id=org_id or uuid.uuid4(), role="owner")


@pytest.mark.asyncio
async def test_unverified_local_user_gets_verification_required(app):
    email = "unverified@example.com"
    password = "securepassword123"
    existing_user = _user(email=email, password=password, is_verified=False)
    primary_org_id = uuid.uuid4()

    session = AsyncMock()
    call_count = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalar_one_or_none.return_value = existing_user
        elif call_count in (2, 4):
            result.scalar_one_or_none.return_value = None
        elif call_count == 3:
            result.scalar_one_or_none.return_value = primary_org_id
        else:
            raise AssertionError("Unexpected DB execute call")
        return result

    session.execute = AsyncMock(side_effect=_execute_side_effect)
    session.commit = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.auth.router.get_postgres_session",
            lambda: _fake_session_ctx(session),
        ),
        patch(
            "dev_health_ops.api.auth.router.get_auth_service",
            lambda: AuthService(secret_key="verification-test-secret"),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/auth/login",
                json={"email": email, "password": password},
            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "email_verification_required"
    assert payload["email"] == email
    assert payload["message"] == "Please verify your email address before logging in"


@pytest.mark.asyncio
async def test_verified_local_user_can_login(app):
    email = "verified@example.com"
    password = "securepassword123"
    existing_user = _user(email=email, password=password, is_verified=True)
    membership = _membership()

    session = AsyncMock()
    call_count = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalar_one_or_none.return_value = existing_user
        elif call_count in (2, 4):
            result.scalar_one_or_none.return_value = None
        elif call_count == 3:
            result.scalar_one_or_none.return_value = None
        elif call_count == 5:
            result.scalar_one_or_none.return_value = membership
        else:
            raise AssertionError("Unexpected DB execute call")
        return result

    session.execute = AsyncMock(side_effect=_execute_side_effect)
    session.commit = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.auth.router.get_postgres_session",
            lambda: _fake_session_ctx(session),
        ),
        patch(
            "dev_health_ops.api.auth.router.get_auth_service",
            lambda: AuthService(secret_key="verification-test-secret"),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/auth/login",
                json={"email": email, "password": password},
            )

    assert response.status_code == 200
    payload = response.json()
    assert "access_token" in payload
    assert payload["needs_onboarding"] is False


@pytest.mark.asyncio
async def test_oauth_user_bypasses_verification(app):
    email = "oauth@example.com"
    password = "securepassword123"
    existing_user = _user(
        email=email,
        password=password,
        is_verified=False,
        auth_provider="github",
    )
    membership = _membership()

    session = AsyncMock()
    call_count = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalar_one_or_none.return_value = existing_user
        elif call_count in (2, 4):
            result.scalar_one_or_none.return_value = None
        elif call_count == 3:
            result.scalar_one_or_none.return_value = None
        elif call_count == 5:
            result.scalar_one_or_none.return_value = membership
        else:
            raise AssertionError("Unexpected DB execute call")
        return result

    session.execute = AsyncMock(side_effect=_execute_side_effect)
    session.commit = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.auth.router.get_postgres_session",
            lambda: _fake_session_ctx(session),
        ),
        patch(
            "dev_health_ops.api.auth.router.get_auth_service",
            lambda: AuthService(secret_key="verification-test-secret"),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/auth/login",
                json={"email": email, "password": password},
            )

    assert response.status_code == 200
    payload = response.json()
    assert "access_token" in payload


@pytest.mark.asyncio
async def test_verify_endpoint_marks_user_verified(app):
    user = SimpleNamespace(id=uuid.uuid4(), is_verified=False)

    async def _verify_token(db, token):
        if token != "valid-token":
            return None
        user.is_verified = True
        return user

    fake_email_verification = SimpleNamespace(verify_email_token=_verify_token)
    session = AsyncMock()
    session.commit = AsyncMock()

    with (
        patch(
            "dev_health_ops.api.auth.router.get_postgres_session",
            lambda: _fake_session_ctx(session),
        ),
        patch(
            "dev_health_ops.api.auth.router.importlib.import_module",
            return_value=fake_email_verification,
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/auth/verify", params={"token": "valid-token"}
            )

    assert response.status_code == 200
    assert user.is_verified is True
    assert response.json() == {
        "message": "Email verified successfully",
        "verified": True,
    }


@pytest.mark.asyncio
async def test_resend_verification_always_returns_200(app):
    fake_email_verification = SimpleNamespace(
        create_email_verification_token=AsyncMock(return_value="token"),
        send_verification_email=AsyncMock(),
    )
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=result)

    with (
        patch(
            "dev_health_ops.api.auth.router.get_postgres_session",
            lambda: _fake_session_ctx(session),
        ),
        patch(
            "dev_health_ops.api.auth.router.importlib.import_module",
            return_value=fake_email_verification,
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/auth/resend-verification",
                json={"email": "missing@example.com"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "message": "If an account exists with that email, a verification link has been sent",
        "verified": None,
    }
