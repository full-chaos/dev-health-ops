from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.email_verification_token import EmailVerificationToken
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import LoginAttempt, Membership, Organization, User

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

VALID_PASSWORD = "SecurePass123!"
VALID_EMAIL = "user@example.com"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "register.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
                    AuditLog.__table__,
                    LoginAttempt.__table__,
                    EmailVerificationToken.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker):
    app = FastAPI()
    app.include_router(auth_router_module.router)

    async def _validation_error_handler(request, exc: Exception):
        if isinstance(exc, RequestValidationError):
            errors = [str(error.get("msg", "Invalid value")) for error in exc.errors()]
        else:
            errors = ["Invalid value"]
        return JSONResponse(
            status_code=422,
            content={
                "detail": {
                    "message": "Validation failed",
                    "errors": errors,
                }
            },
        )

    app.add_exception_handler(RequestValidationError, _validation_error_handler)

    @asynccontextmanager
    async def _session_override():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", _session_override)
    monkeypatch.setattr(rate_limiter, "enabled", False)
    monkeypatch.setattr(
        "dev_health_ops.api.services.email_verification.send_verification_email",
        AsyncMock(),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client


@pytest.mark.asyncio
async def test_register_success_returns_201_with_user_and_org_ids(client):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": VALID_EMAIL, "password": VALID_PASSWORD},
    )
    assert response.status_code == 201
    data = response.json()
    assert "user_id" in data
    assert "org_id" in data
    assert data["message"] == "Registration successful"


@pytest.mark.asyncio
async def test_register_creates_unverified_user_in_db(client, session_maker):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "unverified@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 201

    async with session_maker() as session:
        result = await session.execute(
            select(User).where(User.email == "unverified@example.com")
        )
        user = result.scalar_one_or_none()

    assert user is not None
    assert user.is_verified is False


@pytest.mark.asyncio
async def test_register_creates_organization_in_db(client, session_maker):
    response = await client.post(
        "/api/v1/auth/register",
        json={
            "email": "orgtest@example.com",
            "password": VALID_PASSWORD,
            "org_name": "Test Corp",
        },
    )
    assert response.status_code == 201
    org_id = uuid.UUID(response.json()["org_id"])

    async with session_maker() as session:
        result = await session.execute(
            select(Organization).where(Organization.id == org_id)
        )
        org = result.scalar_one_or_none()

    assert org is not None
    assert org.name == "Test Corp"


@pytest.mark.asyncio
async def test_register_creates_owner_membership(client, session_maker):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "owner@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 201
    data = response.json()
    user_id = uuid.UUID(data["user_id"])
    org_id = uuid.UUID(data["org_id"])

    async with session_maker() as session:
        result = await session.execute(
            select(Membership).where(
                Membership.user_id == user_id,
                Membership.org_id == org_id,
            )
        )
        membership = result.scalar_one_or_none()

    assert membership is not None
    assert membership.role == "owner"


@pytest.mark.asyncio
async def test_register_with_explicit_org_name(client, session_maker):
    response = await client.post(
        "/api/v1/auth/register",
        json={
            "email": "acme@example.com",
            "password": VALID_PASSWORD,
            "org_name": "Acme Corp",
        },
    )
    assert response.status_code == 201
    org_id = uuid.UUID(response.json()["org_id"])

    async with session_maker() as session:
        result = await session.execute(
            select(Organization).where(Organization.id == org_id)
        )
        org = result.scalar_one_or_none()

    assert org is not None
    assert org.name == "Acme Corp"


@pytest.mark.asyncio
async def test_register_without_org_name_defaults_to_my_organization(
    client, session_maker
):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "default-org@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 201
    org_id = uuid.UUID(response.json()["org_id"])

    async with session_maker() as session:
        result = await session.execute(
            select(Organization).where(Organization.id == org_id)
        )
        org = result.scalar_one_or_none()

    assert org is not None
    assert org.name == "My Organization"


@pytest.mark.asyncio
async def test_register_with_full_name(client, session_maker):
    response = await client.post(
        "/api/v1/auth/register",
        json={
            "email": "fullname@example.com",
            "password": VALID_PASSWORD,
            "full_name": "Jane Doe",
        },
    )
    assert response.status_code == 201

    async with session_maker() as session:
        result = await session.execute(
            select(User).where(User.email == "fullname@example.com")
        )
        user = result.scalar_one_or_none()

    assert user is not None
    assert user.full_name == "Jane Doe"


@pytest.mark.asyncio
async def test_register_duplicate_email_returns_400(client):
    await client.post(
        "/api/v1/auth/register",
        json={"email": "dup@example.com", "password": VALID_PASSWORD},
    )
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "dup@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 400
    assert response.json()["detail"]["message"] == "Email already registered"


@pytest.mark.asyncio
async def test_register_email_case_insensitive_duplicate_detection(client):
    await client.post(
        "/api/v1/auth/register",
        json={"email": "TestCase@Example.com", "password": VALID_PASSWORD},
    )
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "testcase@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 400
    assert response.json()["detail"]["message"] == "Email already registered"


@pytest.mark.asyncio
async def test_register_password_too_short_returns_422(client):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "short@example.com", "password": "Short1"},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["message"] == "Password validation failed"
    assert response.json()["detail"]["errors"]


@pytest.mark.asyncio
async def test_register_password_no_digits_returns_422(client):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "nodigits@example.com", "password": "NoDigitsPassword!"},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["message"] == "Password validation failed"
    assert response.json()["detail"]["errors"]


@pytest.mark.asyncio
async def test_register_invalid_email_format_returns_422(client):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "not-an-email", "password": VALID_PASSWORD},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["message"] == "Validation failed"
    assert response.json()["detail"]["errors"]


@pytest.mark.asyncio
async def test_register_sends_verification_email(client, monkeypatch):
    mock_send = AsyncMock()
    monkeypatch.setattr(
        "dev_health_ops.api.services.email_verification.send_verification_email",
        mock_send,
    )
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "verify@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 201
    mock_send.assert_called_once()
    call_kwargs = mock_send.call_args.kwargs
    assert call_kwargs["to_email"] == "verify@example.com"


@pytest.mark.asyncio
async def test_register_email_send_failure_does_not_break_registration(
    client, monkeypatch
):
    mock_send = AsyncMock(side_effect=Exception("SMTP connection failed"))
    monkeypatch.setattr(
        "dev_health_ops.api.services.email_verification.send_verification_email",
        mock_send,
    )
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "emailfail@example.com", "password": VALID_PASSWORD},
    )
    assert response.status_code == 201
    data = response.json()
    assert "user_id" in data
    assert "org_id" in data
