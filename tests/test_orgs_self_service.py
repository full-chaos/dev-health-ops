"""Tests for self-service organization profile endpoints (PATCH /api/v1/orgs/me)."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.orgs.router import router as orgs_router
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency


def _make_user(
    user_id: str = "user-1",
    org_id: str = "org-1",
    role: str = "owner",
    is_superuser: bool = False,
) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=user_id,
        email="test@example.com",
        org_id=org_id,
        role=role,
        is_superuser=is_superuser,
    )


class FakeOrg:
    """Minimal stand-in for the Organization ORM model."""

    def __init__(
        self,
        id: str = "org-1",
        slug: str = "test-org",
        name: str = "Test Org",
        description: str | None = "A test org",
        tier: str = "team",
        is_active: bool = True,
    ):
        self.id = id
        self.slug = slug
        self.name = name
        self.description = description
        self.tier = tier
        self.is_active = is_active
        self.settings = {}
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class FakeMembership:
    """Minimal stand-in for the Membership ORM model."""

    def __init__(self, role: str = "owner"):
        self.role = role


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(orgs_router)
    return app


@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def authed_app(mock_session):
    app = _build_app()

    async def _session_override():
        yield mock_session

    app.dependency_overrides[get_current_user] = lambda: _make_user()
    app.dependency_overrides[postgres_session_dependency] = _session_override
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def authed_client(authed_app):
    transport = ASGITransport(app=authed_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_get_own_org(authed_client):
    """GET /api/v1/orgs/me returns the user's org profile."""
    fake_org = FakeOrg()

    with patch("dev_health_ops.api.orgs.router.OrganizationService") as MockOrgSvc:
        MockOrgSvc.return_value.get_by_id = AsyncMock(return_value=fake_org)

        resp = await authed_client.get("/api/v1/orgs/me")

    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Test Org"
    assert data["slug"] == "test-org"
    assert data["tier"] == "team"


@pytest.mark.asyncio
async def test_update_own_org_as_owner(authed_client):
    """PATCH /api/v1/orgs/me succeeds for org owners."""
    updated_org = FakeOrg(name="Updated Org", description="New desc")

    with (
        patch("dev_health_ops.api.orgs.router.OrganizationService") as MockOrgSvc,
        patch("dev_health_ops.api.orgs.router.MembershipService") as MockMemSvc,
    ):
        MockMemSvc.return_value.get_membership = AsyncMock(
            return_value=FakeMembership(role="owner")
        )
        MockOrgSvc.return_value.update = AsyncMock(return_value=updated_org)

        resp = await authed_client.patch(
            "/api/v1/orgs/me",
            json={"name": "Updated Org", "description": "New desc"},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated Org"
    assert data["description"] == "New desc"

    # Verify only name and description were passed (no tier/settings/is_active)
    MockOrgSvc.return_value.update.assert_awaited_once_with(
        org_id="org-1",
        name="Updated Org",
        description="New desc",
    )


@pytest.mark.asyncio
async def test_update_own_org_rejected_for_member(mock_session):
    """PATCH /api/v1/orgs/me returns 403 for non-admin members."""
    app = _build_app()

    async def _session_override():
        yield mock_session

    app.dependency_overrides[get_current_user] = lambda: _make_user(role="member")
    app.dependency_overrides[postgres_session_dependency] = _session_override

    with patch("dev_health_ops.api.orgs.router.MembershipService") as MockMemSvc:
        MockMemSvc.return_value.get_membership = AsyncMock(
            return_value=FakeMembership(role="member")
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.patch(
                "/api/v1/orgs/me",
                json={"name": "Hijacked"},
            )

    assert resp.status_code == 403
    assert "admin or owner" in resp.json()["detail"]
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_update_own_org_no_org_context(mock_session):
    """PATCH /api/v1/orgs/me returns 400 if user has no org_id."""
    app = _build_app()

    async def _session_override():
        yield mock_session

    app.dependency_overrides[get_current_user] = lambda: _make_user(org_id="")
    app.dependency_overrides[postgres_session_dependency] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.patch(
            "/api/v1/orgs/me",
            json={"name": "No Org"},
        )

    assert resp.status_code == 400
    app.dependency_overrides.clear()
