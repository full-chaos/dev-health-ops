from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.billing import stripe_client
from dev_health_ops.api.billing.router import router
from dev_health_ops.licensing.types import LicenseTier


@pytest.fixture(autouse=True)
def _reset_price_map():
    stripe_client.reset_price_tier_map()
    yield
    stripe_client.reset_price_tier_map()


@pytest.fixture
def mock_auth_user():
    from dev_health_ops.api.services.auth import AuthenticatedUser

    return AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="trial-owner@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
    )


@pytest.fixture
def authed_app(mock_auth_user):
    from dev_health_ops.api.auth.router import get_current_user
    from dev_health_ops.db import postgres_session_dependency

    app = FastAPI()
    app.include_router(router)

    mock_session = AsyncMock()

    async def _session_override():
        yield mock_session

    app.dependency_overrides[get_current_user] = lambda: mock_auth_user
    app.dependency_overrides[postgres_session_dependency] = _session_override
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def authed_client(authed_app):
    transport = ASGITransport(app=authed_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _get_trial_days(tier: LicenseTier) -> int | None:
    resolver = getattr(stripe_client, "get_trial_days")
    return resolver(tier)


def test_get_trial_days_team_returns_configured_days(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TRIAL_DAYS", "21")
    assert _get_trial_days(LicenseTier.TEAM) == 21

    monkeypatch.delenv("TRIAL_DAYS", raising=False)
    assert _get_trial_days(LicenseTier.TEAM) == 14


def test_get_trial_days_enterprise_returns_none():
    assert _get_trial_days(LicenseTier.ENTERPRISE) is None


def test_get_trial_days_community_returns_none():
    assert _get_trial_days(LicenseTier.COMMUNITY) is None
