"""Tests for admin impersonation REST endpoints.

Tests POST /api/v1/admin/impersonate (start),
      POST /api/v1/admin/impersonate/stop,
      GET  /api/v1/admin/impersonate/status.

No real database — session is mocked via FastAPI dependency overrides.
Pattern follows tests/test_impersonation.py and tests/api/auth/* conventions.
"""

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
from dev_health_ops.api.services.impersonation_cache import CachedImpersonationSession

# ---------------------------------------------------------------------------
# Helpers — fake DB row factories
# ---------------------------------------------------------------------------


def _make_user(
    user_id: uuid.UUID,
    email: str,
    *,
    is_superuser: bool = False,
    is_active: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=user_id,
        email=email,
        is_superuser=is_superuser,
        is_active=is_active,
    )


def _make_membership(
    user_id: uuid.UUID,
    org_id: uuid.UUID,
    role: str = "member",
) -> SimpleNamespace:
    return SimpleNamespace(
        user_id=user_id,
        org_id=org_id,
        role=role,
        created_at=datetime.now(timezone.utc),
    )


def _make_session_row(
    admin_id: uuid.UUID,
    target_id: uuid.UUID,
    target_org_id: uuid.UUID,
    role: str = "member",
    *,
    ended_at: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        admin_user_id=admin_id,
        target_user_id=target_id,
        target_org_id=target_org_id,
        target_role=role,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ended_at=ended_at,
    )


class _FakeResult:
    """Minimal SQLAlchemy result mock."""

    def __init__(self, one=None, many=None):
        self._one = one
        self._many = many or []

    def scalar_one_or_none(self):
        return self._one

    def scalars(self):
        return self

    def first(self):
        return self._many[0] if self._many else None


# ---------------------------------------------------------------------------
# Fixture: test_client
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def test_client(monkeypatch):
    """FastAPI TestClient with mocked DB session and dependency overrides."""
    app = FastAPI()
    app.include_router(router)

    db_session = AsyncMock()
    db_session.add = MagicMock()
    db_session.flush = AsyncMock()

    async def _fake_db():
        yield db_session

    admin_state = {
        "user": AuthenticatedUser(
            user_id=str(uuid.uuid4()),
            email="admin@example.com",
            org_id=str(uuid.uuid4()),
            role="admin",
            is_superuser=True,
        )
    }

    async def _fake_current_user():
        return admin_state["user"]

    # Endpoints write the authoritative state through to the shared Valkey
    # cache after COMMIT (CHAOS-2328)
    mock_set_active = AsyncMock()
    monkeypatch.setattr(_imp_mod, "set_active_session", mock_set_active)

    app.dependency_overrides[get_db_session] = _fake_db
    app.dependency_overrides[get_current_user] = _fake_current_user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, db_session, admin_state, mock_set_active

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# POST /api/v1/admin/impersonate — start impersonation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_happy_path_returns_active_status(test_client):
    """Superuser can start impersonation; response contains status=active and target info."""
    client, session, admin_state, _ = test_client
    admin_org = uuid.UUID(admin_state["user"].org_id)
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_make_user(target_id, "target@example.com")),
        _FakeResult(many=[_make_membership(target_id, admin_org, "member")]),
        MagicMock(),  # update to end existing sessions
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "active"
    assert body["target_user"]["id"] == str(target_id)
    assert body["target_user"]["email"] == "target@example.com"
    assert body["target_user"]["org_id"] == str(admin_org)
    assert body["target_user"]["role"] == "member"
    assert "expires_at" in body
    # No JWT token emitted (middleware-based impersonation)
    assert "access_token" not in body


@pytest.mark.asyncio
async def test_start_target_not_found_returns_404(test_client):
    """Returns 404 when target user does not exist."""
    client, session, _, _ = test_client
    session.execute.side_effect = [_FakeResult(one=None)]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(uuid.uuid4())},
    )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Target user not found"


@pytest.mark.asyncio
async def test_start_target_is_superuser_returns_403(test_client):
    """Returns 403 when attempting to impersonate another superuser."""
    client, session, _, _ = test_client
    target_id = uuid.uuid4()
    session.execute.side_effect = [
        _FakeResult(
            one=_make_user(target_id, "other-super@example.com", is_superuser=True)
        ),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Cannot impersonate a superuser"


@pytest.mark.asyncio
async def test_start_self_impersonation_returns_400(test_client):
    """Returns 400 when the admin tries to impersonate themselves."""
    client, session, admin_state, _ = test_client
    self_id = admin_state["user"].user_id

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": self_id},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Cannot impersonate yourself"
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_start_non_superuser_caller_returns_403(test_client):
    """Returns 403 when caller does not have superuser flag."""
    client, session, admin_state, _ = test_client
    admin_state["user"] = AuthenticatedUser(
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
async def test_start_target_no_membership_returns_404(test_client):
    """Returns 404 when target user has no organization membership."""
    client, session, _, _ = test_client
    target_id = uuid.uuid4()
    session.execute.side_effect = [
        _FakeResult(one=_make_user(target_id, "orphan@example.com")),
        _FakeResult(many=[]),  # empty membership
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Target user has no organization membership"


@pytest.mark.asyncio
async def test_start_writes_authoritative_state_through_cache(test_client):
    """set_active_session() receives the new snapshot for this admin on success."""
    client, session, admin_state, mock_set_active = test_client
    admin_org = uuid.UUID(admin_state["user"].org_id)
    target_id = uuid.uuid4()

    session.execute.side_effect = [
        _FakeResult(one=_make_user(target_id, "member@example.com")),
        _FakeResult(many=[_make_membership(target_id, admin_org, "member")]),
        MagicMock(),
    ]

    resp = await client.post(
        "/api/v1/admin/impersonate",
        json={"target_user_id": str(target_id)},
    )

    assert resp.status_code == 200
    mock_set_active.assert_called_once()
    args = mock_set_active.call_args.args
    assert args[0] == admin_state["user"].user_id
    assert args[1] is not None
    assert args[1].target_user_id == str(target_id)


# ---------------------------------------------------------------------------
# POST /api/v1/admin/impersonate/stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_happy_path_returns_stopped_status(test_client):
    """Returns {"status": "stopped"} when an active session exists."""
    client, session, admin_state, mock_set_active = test_client
    admin_id = uuid.UUID(admin_state["user"].user_id)
    target_id = uuid.uuid4()
    target_org = uuid.uuid4()

    active = _make_session_row(admin_id, target_id, target_org)
    session.execute.side_effect = [_FakeResult(one=active)]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "stopped"
    # No JWT token emitted
    assert "access_token" not in body
    mock_set_active.assert_called_once_with(admin_state["user"].user_id, None)


@pytest.mark.asyncio
async def test_stop_commits_before_cache_write_through(test_client):
    """COMMIT must land before the shared-cache write-through (CHAOS-2328).

    Writing pre-commit state (or deleting before commit) would let stale
    impersonation state reach every replica for up to the cache TTL.
    """
    client, session, admin_state, mock_set_active = test_client
    admin_id = uuid.UUID(admin_state["user"].user_id)
    active = _make_session_row(admin_id, uuid.uuid4(), uuid.uuid4())
    session.execute.side_effect = [_FakeResult(one=active)]

    order: list[str] = []
    session.commit.side_effect = lambda: order.append("commit")
    mock_set_active.side_effect = lambda *_: order.append("write_through")

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 200
    assert order[:2] == ["commit", "write_through"]


@pytest.mark.asyncio
async def test_stop_no_active_session_returns_400(test_client):
    """Returns 400 when there is no active session to stop."""
    client, session, _, _ = test_client
    session.execute.side_effect = [_FakeResult(one=None)]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 400
    assert resp.json()["detail"] == "No active impersonation session"


@pytest.mark.asyncio
async def test_stop_marks_session_ended_at(test_client):
    """The active session row has ended_at set after a successful stop."""
    client, session, admin_state, _ = test_client
    admin_id = uuid.UUID(admin_state["user"].user_id)
    active = _make_session_row(admin_id, uuid.uuid4(), uuid.uuid4())
    assert active.ended_at is None

    session.execute.side_effect = [_FakeResult(one=active)]

    resp = await client.post("/api/v1/admin/impersonate/stop")

    assert resp.status_code == 200
    assert active.ended_at is not None


# ---------------------------------------------------------------------------
# GET /api/v1/admin/impersonate/status
# ---------------------------------------------------------------------------


def _make_cached(
    admin_id: uuid.UUID,
    target_id: uuid.UUID,
    target_org: uuid.UUID,
    role: str = "member",
    *,
    target_email: str | None = "target@example.com",
    expires_in: timedelta = timedelta(hours=1),
) -> CachedImpersonationSession:
    return CachedImpersonationSession(
        id=str(uuid.uuid4()),
        admin_user_id=str(admin_id),
        target_user_id=str(target_id),
        target_org_id=str(target_org),
        target_role=role,
        target_email=target_email,
        expires_at=datetime.now(timezone.utc) + expires_in,
    )


@pytest.mark.asyncio
async def test_status_when_impersonating_returns_true(test_client, monkeypatch):
    """Returns is_impersonating=true with target details from the shared cache."""
    client, session, admin_state, _ = test_client
    admin_id = uuid.UUID(admin_state["user"].user_id)
    target_id = uuid.uuid4()
    target_org = uuid.uuid4()

    cached = _make_cached(admin_id, target_id, target_org, "viewer")
    monkeypatch.setattr(_imp_mod, "get_active_session", AsyncMock(return_value=cached))

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["is_impersonating"] is True
    assert body["target_user_id"] == str(target_id)
    assert body["target_org_id"] == str(target_org)
    assert body["target_email"] == "target@example.com"
    assert body["expires_at"] is not None
    # The hot path must not touch Postgres (CHAOS-2328)
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_status_when_not_impersonating_returns_false(test_client, monkeypatch):
    """Returns is_impersonating=false when no active session exists."""
    client, session, _, _ = test_client
    monkeypatch.setattr(_imp_mod, "get_active_session", AsyncMock(return_value=None))

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["is_impersonating"] is False
    assert body.get("target_user_id") is None


@pytest.mark.asyncio
async def test_status_expired_cached_session_returns_false(test_client, monkeypatch):
    """A cached session past its expires_at reads as not impersonating."""
    client, _, admin_state, _ = test_client
    admin_id = uuid.UUID(admin_state["user"].user_id)

    expired = _make_cached(
        admin_id, uuid.uuid4(), uuid.uuid4(), expires_in=timedelta(seconds=-5)
    )
    monkeypatch.setattr(_imp_mod, "get_active_session", AsyncMock(return_value=expired))

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    assert resp.json()["is_impersonating"] is False


@pytest.mark.asyncio
async def test_status_non_superuser_returns_false_without_db(test_client):
    """Non-superuser gets is_impersonating=false without any DB query."""
    client, session, admin_state, _ = test_client
    admin_state["user"] = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="member@example.com",
        org_id=str(uuid.uuid4()),
        role="member",
        is_superuser=False,
    )

    resp = await client.get("/api/v1/admin/impersonate/status")

    assert resp.status_code == 200
    assert resp.json()["is_impersonating"] is False
    session.execute.assert_not_called()
