from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.mark.asyncio
async def test_checkout_includes_trial_for_team(authed_client):
    mock_checkout = SimpleNamespace(
        id="cs_test_trial_team", url="https://checkout.stripe.com/cs_test_trial_team"
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.has_had_trial",
            new=AsyncMock(return_value=False),
        ),
        patch.dict(
            "os.environ",
            {
                "STRIPE_PRICE_ID_TEAM": "price_team_123",
                "TRIAL_DAYS": "10",
                "APP_BASE_URL": "https://example.com",
            },
            clear=False,
        ),
    ):
        mock_client = MagicMock()
        mock_client.checkout.sessions.create.return_value = mock_checkout
        mock_client_fn.return_value = mock_client

        resp = await authed_client.post(
            "/api/v1/billing/checkout",
            json={
                "tier": "team",
                "success_url": "https://example.com/success",
                "cancel_url": "https://example.com/cancel",
            },
        )

    assert resp.status_code == 200
    _, kwargs = mock_client.checkout.sessions.create.call_args
    params = kwargs["params"]
    assert params["subscription_data"]["trial_period_days"] == 10


@pytest.mark.asyncio
async def test_checkout_excludes_trial_for_enterprise(authed_client):
    mock_checkout = SimpleNamespace(
        id="cs_test_enterprise", url="https://checkout.stripe.com/cs_test_enterprise"
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch.dict(
            "os.environ",
            {
                "STRIPE_PRICE_ID_ENTERPRISE": "price_enterprise_123",
                "APP_BASE_URL": "https://example.com",
            },
            clear=False,
        ),
    ):
        mock_client = MagicMock()
        mock_client.checkout.sessions.create.return_value = mock_checkout
        mock_client_fn.return_value = mock_client

        resp = await authed_client.post(
            "/api/v1/billing/checkout",
            json={
                "tier": "enterprise",
                "success_url": "https://example.com/success",
                "cancel_url": "https://example.com/cancel",
            },
        )

    assert resp.status_code == 200
    _, kwargs = mock_client.checkout.sessions.create.call_args
    params = kwargs["params"]
    assert "subscription_data" not in params


@pytest.mark.asyncio
async def test_checkout_strips_trial_for_previously_trialed_org(authed_client):
    mock_checkout = SimpleNamespace(
        id="cs_test_retrial", url="https://checkout.stripe.com/cs_test_retrial"
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.has_had_trial",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dev_health_ops.api.billing.router.BillingAuditService.log",
            new=AsyncMock(return_value=None),
        ),
        patch.dict(
            "os.environ",
            {
                "STRIPE_PRICE_ID_TEAM": "price_team_123",
                "TRIAL_DAYS": "14",
                "APP_BASE_URL": "https://example.com",
            },
            clear=False,
        ),
    ):
        mock_client = MagicMock()
        mock_client.checkout.sessions.create.return_value = mock_checkout
        mock_client_fn.return_value = mock_client

        resp = await authed_client.post(
            "/api/v1/billing/checkout",
            json={
                "tier": "team",
                "success_url": "https://example.com/success",
                "cancel_url": "https://example.com/cancel",
            },
        )

    assert resp.status_code == 200
    _, kwargs = mock_client.checkout.sessions.create.call_args
    params = kwargs["params"]
    assert "subscription_data" not in params
