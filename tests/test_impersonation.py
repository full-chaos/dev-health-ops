from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.admin import impersonation as _imp_mod
from dev_health_ops.api.admin.impersonation import get_db_session, router
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser


def _user(
    user_id: uuid.UUID,
    email: str,
    *,
    is_superuser: bool = False,
    is_active: bool = True,
    username: str | None = None,
    full_name: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=user_id,
        email=email,
        is_superuser=is_superuser,
        is_active=is_active,
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


def _impersonation_session(
    admin_id: uuid.UUID,
    target_id: uuid.UUID,
    target_org_id: uuid.UUID,
    target_role: str = "member",
) -> SimpleNamespace:
    now = datetime.now(timezone.utc)
    sess = SimpleNamespace(
        id=uuid.uuid4(),
        admin_user_id=admin_id,
        target_user_id=target_id,
        target_org_id=target_org_id,
        target_role=target_role,
        expires_at=now + timedelta(hours=1),
        ended_at=None,
    )
    return sess


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

    mock_invalidate = MagicMock()
    monkeypatch.setattr(_imp_mod, "invalidate", mock_invalidate)

    app.dependency_overrides[get_db_session] = _override_db_session
    app.dependency_overrides[get_current_user] = _override_current_user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, session, current, mock_invalidate

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# POST /api/v1/admin/impersonate (start)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_impersonation_superuser_can_impersonate_member(test_client):
    client, session, current, mock_invalidate = test_client
    admin_org_id = uuid.UUID(current["user"].org_id)
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(many=[_membership(target_id, admin_org_id, "member")]),
        MagicMock(),  # update (end existing sessions) result — not used
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "active"
    assert "access_token" not in body
    assert body["target_user"]["id"] == str(target_id)
    assert body["target_user"]["email"] == "member@example.com"
    assert body["target_user"]["org_id"] == str(admin_org_id)
    assert body["target_user"]["role"] == "member"
    assert "expires_at" in body


@pytest.mark.asyncio
async def test_start_impersonation_non_superuser_forbidden(test_client):
    client, session, current, _ = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="regular@example.com",
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
    client, session, current, _ = test_client
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "other-root@example.com", is_superuser=True)),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Cannot impersonate a superuser"


@pytest.mark.asyncio
async def test_start_impersonation_target_not_found(test_client):
    client, session, current, _ = test_client
    session.execute.side_effect = [_FakeResult(one=None)]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(uuid.uuid4())},
    )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Target user not found"


@pytest.mark.asyncio
async def test_start_impersonation_self_impersonation_blocked(test_client):
    client, session, current, _ = test_client
    admin_id = uuid.UUID(current["user"].user_id)

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(admin_id)},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Cannot impersonate yourself"
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_start_impersonation_target_inactive(test_client):
    client, session, current, _ = test_client
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "inactive@example.com", is_active=False)),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Target user is not active"


@pytest.mark.asyncio
async def test_start_impersonation_no_membership(test_client):
    client, session, current, _ = test_client
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(many=[]),  # no membership
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Target user has no organization membership"


@pytest.mark.asyncio
async def test_start_impersonation_invalidates_cache(test_client):
    client, session, current, mock_invalidate = test_client
    admin_org_id = uuid.UUID(current["user"].org_id)
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(many=[_membership(target_id, admin_org_id, "member")]),
        MagicMock(),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    mock_invalidate.assert_called_once_with(current["user"].user_id)


@pytest.mark.asyncio
async def test_start_impersonation_creates_session_row(test_client):
    client, session, current, _ = test_client
    admin_org_id = uuid.UUID(current["user"].org_id)
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_user(target_id, "member@example.com")),
        _FakeResult(many=[_membership(target_id, admin_org_id, "member")]),
        MagicMock(),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    # session.add should be called with the new ImpersonationSession
    session.add.assert_called()


# ---------------------------------------------------------------------------
# POST /api/v1/admin/impersonate/stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_impersonation_works(test_client):
    client, session, current, mock_invalidate = test_client
    admin_id = uuid.UUID(current["user"].user_id)
    target_id = uuid.uuid4()
    target_org_id = uuid.uuid4()

    active = _impersonation_session(admin_id, target_id, target_org_id)
    session.execute.side_effect = [_FakeResult(one=active)]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "stopped"
    assert "access_token" not in body
    mock_invalidate.assert_called_once_with(current["user"].user_id)


@pytest.mark.asyncio
async def test_stop_impersonation_ends_session_in_db(test_client):
    client, session, current, _ = test_client
    admin_id = uuid.UUID(current["user"].user_id)
    target_id = uuid.uuid4()
    target_org_id = uuid.uuid4()

    active = _impersonation_session(admin_id, target_id, target_org_id)
    assert active.ended_at is None

    session.execute.side_effect = [_FakeResult(one=active)]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 200
    # The session object should have ended_at set
    assert active.ended_at is not None


@pytest.mark.asyncio
async def test_stop_impersonation_forbidden_for_non_superuser(test_client):
    client, session, current, _ = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="regular@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
        is_superuser=False,
    )

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 403
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_stop_impersonation_no_active_session(test_client):
    client, session, current, _ = test_client
    session.execute.side_effect = [_FakeResult(one=None)]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 400
    assert resp.json()["detail"] == "No active impersonation session"


# ---------------------------------------------------------------------------
# GET /api/v1/admin/impersonate/status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_non_superuser_returns_false_without_db_call(test_client):
    client, session, current, _ = test_client
    current["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="regular@example.com",
        org_id=str(uuid.uuid4()),
        role="member",
        is_superuser=False,
    )

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["is_impersonating"] is False
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_status_superuser_with_no_active_session(test_client):
    client, session, current, _ = test_client
    session.execute.side_effect = [_FakeResult(one=None)]

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["is_impersonating"] is False
    assert body.get("target_user_id") is None


@pytest.mark.asyncio
async def test_status_superuser_with_active_session(test_client):
    client, session, current, _ = test_client
    admin_id = uuid.UUID(current["user"].user_id)
    target_id = uuid.uuid4()
    target_org_id = uuid.uuid4()

    active = _impersonation_session(admin_id, target_id, target_org_id, "viewer")
    target = _user(target_id, "target@example.com")

    session.execute.side_effect = [
        _FakeResult(one=active),
        _FakeResult(one=target),
    ]

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["is_impersonating"] is True
    assert body["target_user_id"] == str(target_id)
    assert body["target_email"] == "target@example.com"
    assert body["target_org_id"] == str(target_org_id)
    assert body["expires_at"] is not None
