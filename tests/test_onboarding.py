from __future__ import annotations

import uuid
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace

import bcrypt
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.services.auth import AuthService, AuthenticatedUser
from dev_health_ops.models.users import Membership, Organization

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


class FakeResult:
    def __init__(self, scalar=None, first_row=None):
        self._scalar = scalar
        self._first_row = first_row

    def scalar_one_or_none(self):
        return self._scalar

    def first(self):
        return self._first_row


class FakeSession:
    def __init__(self, execute_results: list[FakeResult]):
        self._execute_results = execute_results
        self._execute_index = 0
        self.added: list[object] = []
        self.commit_count = 0

    async def execute(self, _stmt):
        result = self._execute_results[self._execute_index]
        self._execute_index += 1
        return result

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        for obj in self.added:
            if getattr(obj, "id", None) is None:
                setattr(obj, "id", uuid.uuid4())

    async def commit(self):
        self.commit_count += 1


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth_router_module.router)
    return app


def _local_user(email: str, password: str) -> SimpleNamespace:
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode(
        "utf-8"
    )
    return SimpleNamespace(
        id=uuid.uuid4(),
        email=email,
        username="test-user",
        full_name="Test User",
        password_hash=password_hash,
        is_active=True,
        is_superuser=False,
        last_login_at=None,
    )


@pytest.mark.asyncio
async def test_login_without_membership_returns_needs_onboarding(monkeypatch):
    app = _make_app()
    auth_service = AuthService(secret_key="onboarding-test-secret")
    user = _local_user("orgless@example.com", "password123")
    session = FakeSession(
        execute_results=[
            FakeResult(scalar=user),
            FakeResult(scalar=None),
        ]
    )

    @asynccontextmanager
    async def fake_db():
        yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", fake_db)
    monkeypatch.setattr(auth_router_module, "get_auth_service", lambda: auth_service)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/auth/login",
            json={"email": "orgless@example.com", "password": "password123"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["needs_onboarding"] is True
    assert payload["user"]["org_id"] is None
    assert payload["user"]["role"] == "member"
    assert payload["access_token"]
    assert payload["refresh_token"]


@pytest.mark.asyncio
async def test_login_with_membership_returns_needs_onboarding_false(monkeypatch):
    app = _make_app()
    auth_service = AuthService(secret_key="onboarding-test-secret")
    user = _local_user("member@example.com", "password123")
    org_id = uuid.uuid4()
    membership = SimpleNamespace(org_id=org_id, role="admin")
    session = FakeSession(
        execute_results=[
            FakeResult(scalar=user),
            FakeResult(scalar=membership),
        ]
    )

    @asynccontextmanager
    async def fake_db():
        yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", fake_db)
    monkeypatch.setattr(auth_router_module, "get_auth_service", lambda: auth_service)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/auth/login",
            json={"email": "member@example.com", "password": "password123"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["needs_onboarding"] is False
    assert payload["user"]["org_id"] == str(org_id)
    assert payload["user"]["role"] == "admin"


@pytest.mark.asyncio
async def test_onboard_create_org_creates_org_membership_and_tokens(monkeypatch):
    app = _make_app()
    auth_service = AuthService(secret_key="onboarding-test-secret")
    user_id = uuid.uuid4()
    db_user = SimpleNamespace(
        id=user_id,
        email="newuser@example.com",
        username="newuser",
        full_name="New User",
        is_superuser=False,
    )
    session = FakeSession(
        execute_results=[
            FakeResult(scalar=db_user),
            FakeResult(first_row=None),
        ]
    )
    app.dependency_overrides[auth_router_module.get_current_user] = lambda: (
        AuthenticatedUser(
            user_id=str(user_id),
            email="newuser@example.com",
            org_id="",
            role="member",
        )
    )

    @asynccontextmanager
    async def fake_db():
        yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", fake_db)
    monkeypatch.setattr(auth_router_module, "get_auth_service", lambda: auth_service)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/auth/onboard",
            json={"action": "create_org", "org_name": "Acme Platform"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["org_name"] == "Acme Platform"
    assert payload["role"] == "owner"
    assert payload["org_id"]
    assert payload["access_token"]
    assert payload["refresh_token"]
    assert any(isinstance(item, Organization) for item in session.added)
    assert any(isinstance(item, Membership) for item in session.added)


@pytest.mark.asyncio
async def test_onboard_when_already_onboarded_returns_400(monkeypatch):
    app = _make_app()
    user_id = uuid.uuid4()
    db_user = SimpleNamespace(
        id=user_id,
        email="existing@example.com",
        username="existing",
        full_name="Existing User",
        is_superuser=False,
    )
    session = FakeSession(
        execute_results=[
            FakeResult(scalar=db_user),
            FakeResult(first_row=(uuid.uuid4(),)),
        ]
    )
    app.dependency_overrides[auth_router_module.get_current_user] = lambda: (
        AuthenticatedUser(
            user_id=str(user_id),
            email="existing@example.com",
            org_id="",
            role="member",
        )
    )

    @asynccontextmanager
    async def fake_db():
        yield session

    monkeypatch.setattr(auth_router_module, "get_postgres_session", fake_db)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/auth/onboard",
            json={"action": "create_org", "org_name": "Should Fail"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "Already onboarded"


@pytest.mark.asyncio
async def test_onboard_requires_authentication():
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/auth/onboard",
            json={"action": "create_org", "org_name": "Unauthorized"},
        )

    assert response.status_code == 401
