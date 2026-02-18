from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.admin.impersonation import get_db_session, router
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthService, AuthenticatedUser


def _user(
    user_id: uuid.UUID,
    email: str,
    *,
    is_superuser: bool = False,
    username: str | None = None,
    full_name: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=user_id,
        email=email,
        is_superuser=is_superuser,
        username=username,
        full_name=full_name,
    )


def _membership(user_id: uuid.UUID, org_id: uuid.UUID, role: str) -> SimpleNamespace:
    return SimpleNamespace(
        user_id=user_id,
        org_id=org_id,
        role=role,
        created_at=datetime.now(timezone.utc),
    )


class _FakeResult:
    def __init__(self, one=None, many=None):
        self._one = one
        self._many = many or []

    def scalar_one_or_none(self):
        return self._one

    def scalars(self):
        return self

    def first(self):
        return self._many[0] if self._many else None


@pytest_asyncio.fixture
async def test_client(monkeypatch):
    app = FastAPI()
    app.include_router(router)

    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()

    async def _override_db_session():
        yield session

    current = {
        "user": AuthenticatedUser(
            user_id=str(uuid.uuid4()),
            email="admin@example.com",
            org_id=str(uuid.uuid4()),
            role="admin",
            is_superuser=True,
        )
    }

    async def _override_current_user():
        return current["user"]

    auth_service = AuthService(secret_key="test-secret")
    monkeypatch.setattr(
        "dev_health_ops.api.admin.impersonation.get_auth_service",
        lambda: auth_service,
    )

    app.dependency_overrides[get_db_session] = _override_db_session
    app.dependency_overrides[get_current_user] = _override_current_user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, session, auth_service, current

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_start_impersonation_superuser_can_impersonate_member(test_client):
    client, session, auth_service, current = test_client
    admin_org_id = uuid.uuid4()
    target_id = uuid.uuid4()

    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(admin_org_id),
        role="admin",
        is_superuser=True,
    )

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(one=_membership(target_id, admin_org_id, "member")),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    body = resp.json()
    payload = auth_service.validate_token(body["access_token"])
    assert payload is not None
    assert body["token_type"] == "bearer"
    assert body["expires_in"] == 3600
    assert body["impersonated_user"]["id"] == str(target_id)
    assert payload["sub"] == str(target_id)
    assert payload["impersonating_user_id"] == current["user"].user_id


@pytest.mark.asyncio
async def test_start_impersonation_non_superuser_forbidden(test_client):
    client, session, _, current = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
        is_superuser=False,
    )

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(uuid.uuid4())},
    )

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Superuser access required"
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_start_impersonation_cannot_impersonate_superuser(test_client):
    client, session, _, current = test_client
    target_id = uuid.uuid4()
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="root@example.com",
        org_id=current["user"].org_id,
        role="admin",
        is_superuser=True,
    )
    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "other-root@example.com", is_superuser=True)),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Cannot impersonate superuser"


@pytest.mark.asyncio
async def test_start_impersonation_target_not_found(test_client):
    client, session, _, current = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="root@example.com",
        org_id=current["user"].org_id,
        role="admin",
        is_superuser=True,
    )
    session.execute.side_effect = [_FakeResult(one=None)]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(uuid.uuid4())},
    )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Target user not found"


@pytest.mark.asyncio
async def test_start_impersonation_non_superuser_blocked_entirely(test_client):
    client, session, _, current = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
        is_superuser=False,
    )

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(uuid.uuid4())},
    )

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Superuser access required"
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_start_impersonation_cross_org_allowed_for_superuser(test_client):
    client, session, _, current = test_client
    caller_org_id = uuid.uuid4()
    target_org_id = uuid.uuid4()
    target_id = uuid.uuid4()
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="root@example.com",
        org_id=str(caller_org_id),
        role="owner",
        is_superuser=True,
    )

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(one=None),
        _FakeResult(many=[_membership(target_id, target_org_id, "viewer")]),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["impersonated_user"]["org_id"] == str(target_org_id)
    assert body["impersonated_user"]["role"] == "viewer"


@pytest.mark.asyncio
async def test_stop_impersonation_returns_real_admin_token(test_client):
    client, session, auth_service, current = test_client
    org_id = uuid.uuid4()
    real_admin_id = uuid.uuid4()
    impersonated_user_id = uuid.uuid4()

    current["user"] = AuthenticatedUser(
        user_id=str(impersonated_user_id),
        email="member@example.com",
        org_id=str(org_id),
        role="member",
        impersonated_by=str(real_admin_id),
    )

    session.execute.side_effect = [
        _FakeResult(one=_user(real_admin_id, "admin@example.com")),
        _FakeResult(one=_membership(real_admin_id, org_id, "admin")),
    ]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 200
    body = resp.json()
    payload = auth_service.validate_token(body["access_token"])
    assert payload is not None
    assert payload["sub"] == str(real_admin_id)
    assert "impersonating_user_id" not in payload


@pytest.mark.asyncio
async def test_stop_impersonation_fails_if_not_impersonating(test_client):
    client, session, _, current = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
    )

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Not currently impersonating"
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_status_when_impersonating(test_client):
    client, _, _, current = test_client
    real_admin_id = uuid.uuid4()
    impersonated_user_id = uuid.uuid4()
    current["user"] = AuthenticatedUser(
        user_id=str(impersonated_user_id),
        email="member@example.com",
        org_id=str(uuid.uuid4()),
        role="member",
        impersonated_by=str(real_admin_id),
    )

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    assert resp.json() == {
        "is_impersonating": True,
        "impersonated_user_id": str(impersonated_user_id),
        "real_user_id": str(real_admin_id),
    }


@pytest.mark.asyncio
async def test_status_when_not_impersonating(test_client):
    client, _, _, current = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
    )

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    assert resp.json() == {
        "is_impersonating": False,
        "impersonated_user_id": None,
        "real_user_id": None,
    }


@pytest.mark.asyncio
async def test_impersonation_token_contains_required_claims(test_client):
    client, session, auth_service, current = test_client
    org_id = uuid.uuid4()
    target_id = uuid.uuid4()
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(org_id),
        role="owner",
        is_superuser=True,
    )

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(one=_membership(target_id, org_id, "viewer")),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    token_payload = auth_service.validate_token(resp.json()["access_token"])
    assert token_payload is not None
    assert token_payload["sub"] == str(target_id)
    assert token_payload["org_id"] == str(org_id)
    assert token_payload["role"] == "viewer"
    assert token_payload["impersonating_user_id"] == current["user"].user_id


@pytest.mark.asyncio
async def test_impersonation_token_has_short_ttl(test_client):
    client, session, auth_service, _ = test_client
    org_id = uuid.uuid4()
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(one=_membership(target_id, org_id, "member")),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    token_payload = auth_service.validate_token(resp.json()["access_token"])
    assert token_payload is not None
    assert (token_payload["exp"] - token_payload["iat"]) == 3600
