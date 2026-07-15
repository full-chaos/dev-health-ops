from __future__ import annotations

import base64
import importlib
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.services import auth as auth_module
from dev_health_ops.api.services.auth import AuthenticatedUser, AuthService
from dev_health_ops.licensing import gating
from dev_health_ops.licensing.generator import generate_test_license
from dev_health_ops.models.internal_service_credential import (
    generate_internal_service_token,
)

billing_module = importlib.import_module("dev_health_ops.api.billing.router")
billing_router = billing_module.router
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


@asynccontextmanager
async def _authentication_database() -> AsyncIterator[None]:
    yield None


async def _entitlements(*_args: object, **_kwargs: object) -> dict[str, object]:
    return {
        "tier": "team",
        "features": {"agent_context_runtime": True},
        "limits": {},
        "is_licensed": True,
        "in_grace_period": False,
    }


def _app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    app = FastAPI()
    app.include_router(billing_router)
    database = AsyncMock()
    database.get.return_value = SimpleNamespace(is_active=True, is_superuser=False)
    database.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: object())

    async def _database_dependency() -> AsyncIterator[AsyncMock]:
        yield database

    app.dependency_overrides[billing_module.postgres_session_dependency] = (
        _database_dependency
    )
    app.state.billing_database = database
    monkeypatch.setattr(gating, "get_org_entitlements_from_db", _entitlements)
    return app


def _user(org_id: str, *, is_superuser: bool = False) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="member@example.com",
        org_id=org_id,
        role="member",
        is_superuser=is_superuser,
    )


def _acr_client_credential_token() -> str:
    return "fcacr_" + base64.urlsafe_b64encode(b"a" * 32).decode().rstrip("=")


def _license_key_token() -> str:
    return generate_test_license(org_id=str(uuid.uuid4()))


@pytest.mark.asyncio
async def test_billing_entitlements_rejects_anonymous_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(monkeypatch)
    org_id = uuid.uuid4()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/api/v1/billing/entitlements/{org_id}")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_billing_entitlements_rejects_other_organization_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(monkeypatch)
    app.dependency_overrides[billing_module.get_current_user] = lambda: _user(
        str(uuid.uuid4())
    )
    org_id = uuid.uuid4()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/api/v1/billing/entitlements/{org_id}")

    assert response.status_code == 403
    assert response.json() == {"detail": "Access forbidden"}


@pytest.mark.asyncio
async def test_billing_entitlements_returns_own_organization_entitlements(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(monkeypatch)
    org_id = uuid.uuid4()
    app.dependency_overrides[billing_module.get_current_user] = lambda: _user(
        str(org_id)
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/api/v1/billing/entitlements/{org_id}")

    assert response.status_code == 200
    assert response.json()["features"] == {"agent_context_runtime": True}


@pytest.mark.asyncio
async def test_billing_entitlements_allows_superuser_cross_organization_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(monkeypatch)
    app.dependency_overrides[billing_module.get_current_user] = lambda: _user(
        str(uuid.uuid4()), is_superuser=True
    )
    app.state.billing_database.get.return_value = SimpleNamespace(
        is_active=True, is_superuser=True
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/api/v1/billing/entitlements/{uuid.uuid4()}")

    assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token_factory",
    [
        _acr_client_credential_token,
        _license_key_token,
        generate_internal_service_token,
    ],
    ids=["acr-client-credential", "license-key", "internal-service"],
)
async def test_billing_entitlements_rejects_non_session_token_classes(
    monkeypatch: pytest.MonkeyPatch,
    token_factory: Callable[[], str],
) -> None:
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-entitlements-auth")
    monkeypatch.setattr(auth_module, "_auth_service", None)
    app = _app(monkeypatch)
    monkeypatch.setattr(
        auth_router_module, "get_postgres_session", _authentication_database
    )
    token = token_factory()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/api/v1/billing/entitlements/{uuid.uuid4()}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_billing_entitlements_rejects_expired_user_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "test-secret-key-for-entitlements-auth"
    monkeypatch.setenv("JWT_SECRET_KEY", secret)
    monkeypatch.setattr(auth_module, "_auth_service", None)
    app = _app(monkeypatch)
    monkeypatch.setattr(
        auth_router_module, "get_postgres_session", _authentication_database
    )
    expired_token = AuthService(secret_key=secret).create_access_token(
        user_id=str(uuid.uuid4()),
        email="expired@example.com",
        org_id=str(uuid.uuid4()),
        expires_delta=timedelta(seconds=-1),
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/api/v1/billing/entitlements/{uuid.uuid4()}",
            headers={"Authorization": f"Bearer {expired_token}"},
        )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_billing_entitlements_rejects_removed_member_and_demoted_superuser() -> (
    None
):
    org_id = uuid.uuid4()
    user = _user(str(org_id), is_superuser=True)
    database = AsyncMock()
    database.get.return_value = SimpleNamespace(is_active=True, is_superuser=False)
    database.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: None)

    with pytest.raises(HTTPException) as exc_info:
        await billing_module.require_billing_entitlement_access(
            org_id=org_id,
            user=user,
            db=database,
        )

    assert exc_info.value.status_code == 403
