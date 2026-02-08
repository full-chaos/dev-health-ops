"""Tests for the billing router (Stripe webhooks, checkout, portal, entitlements)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.billing.router import router
from dev_health_ops.api.billing.stripe_client import reset_price_tier_map


@pytest.fixture(autouse=True)
def _reset_price_map():
    reset_price_tier_map()
    yield
    reset_price_tier_map()


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def app():
    return _build_app()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _make_stripe_event(event_type: str, data_object: dict) -> SimpleNamespace:
    obj = SimpleNamespace(**data_object)
    return SimpleNamespace(
        type=event_type,
        data=SimpleNamespace(object=obj),
    )


# ---------------------------------------------------------------------------
# Webhook tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_signature(client):
    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
    ):
        from stripe import SignatureVerificationError

        mock_client.return_value.construct_event.side_effect = (
            SignatureVerificationError("bad sig", "sig_header")
        )
        resp = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "bad"},
        )
        assert resp.status_code == 400
        assert "Invalid Stripe signature" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_webhook_checkout_completed(client):
    event = _make_stripe_event(
        "checkout.session.completed",
        {
            "id": "cs_test_123",
            "metadata": {"org_id": "org-abc"},
            "customer": "cus_test",
        },
    )

    mock_line_items = SimpleNamespace(
        data=[SimpleNamespace(price=SimpleNamespace(id="price_team_123"))]
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch(
            "dev_health_ops.api.billing.router.get_private_key",
            return_value="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        ),
        patch("dev_health_ops.api.billing.router._persist_license") as mock_persist,
        patch.dict("os.environ", {"STRIPE_PRICE_ID_TEAM": "price_team_123"}),
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client.checkout.sessions.list_line_items.return_value = mock_line_items
        mock_client_fn.return_value = mock_client

        resp = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        mock_persist.assert_awaited_once()


@pytest.mark.asyncio
async def test_webhook_subscription_deleted(client):
    event = _make_stripe_event(
        "customer.subscription.deleted",
        {
            "metadata": {"org_id": "org-abc"},
            "customer": "cus_test",
        },
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router._revoke_license") as mock_revoke,
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client

        resp = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )
        assert resp.status_code == 200
        mock_revoke.assert_awaited_once_with("org-abc")


@pytest.mark.asyncio
async def test_webhook_payment_failed(client):
    event = _make_stripe_event(
        "invoice.payment_failed",
        {"customer": "cus_test"},
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client

        resp = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_webhook_unhandled_event(client):
    event = _make_stripe_event("some.unknown.event", {"id": "evt_123"})

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client

        resp = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Checkout tests
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_auth_user():
    from dev_health_ops.api.services.auth import AuthenticatedUser

    return AuthenticatedUser(
        user_id="user-1",
        email="test@example.com",
        org_id="org-abc",
        role="admin",
    )


@pytest.fixture
def authed_app(mock_auth_user):
    from dev_health_ops.api.auth.router import get_current_user

    app = _build_app()
    app.dependency_overrides[get_current_user] = lambda: mock_auth_user
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def authed_client(authed_app):
    transport = ASGITransport(app=authed_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_checkout_invalid_tier(authed_client):
    resp = await authed_client.post(
        "/api/v1/billing/checkout",
        json={
            "tier": "nonexistent",
            "success_url": "https://example.com/success",
            "cancel_url": "https://example.com/cancel",
        },
    )
    assert resp.status_code == 400
    assert "Invalid tier" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_checkout_no_price_configured(authed_client):
    resp = await authed_client.post(
        "/api/v1/billing/checkout",
        json={
            "tier": "team",
            "success_url": "https://example.com/success",
            "cancel_url": "https://example.com/cancel",
        },
    )
    assert resp.status_code == 400
    assert "No price configured" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_checkout_success(authed_client):
    mock_session = SimpleNamespace(
        id="cs_test", url="https://checkout.stripe.com/cs_test"
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch.dict("os.environ", {"STRIPE_PRICE_ID_TEAM": "price_team_123"}),
    ):
        mock_client = MagicMock()
        mock_client.checkout.sessions.create.return_value = mock_session
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
        data = resp.json()
        assert data["session_id"] == "cs_test"
        assert data["url"] == "https://checkout.stripe.com/cs_test"


@pytest.mark.asyncio
async def test_checkout_requires_auth(client):
    resp = await client.post(
        "/api/v1/billing/checkout",
        json={
            "tier": "team",
            "success_url": "https://example.com/success",
            "cancel_url": "https://example.com/cancel",
        },
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Portal tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_portal_no_customer(authed_client):
    with patch(
        "dev_health_ops.api.billing.router._get_customer_id",
        return_value=None,
    ):
        resp = await authed_client.post("/api/v1/billing/portal")
        assert resp.status_code == 404
        assert "No billing account" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_portal_success(authed_client):
    mock_portal = SimpleNamespace(url="https://billing.stripe.com/session/test")

    with (
        patch(
            "dev_health_ops.api.billing.router._get_customer_id",
            return_value="cus_test",
        ),
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
    ):
        mock_client = MagicMock()
        mock_client.billing_portal.sessions.create.return_value = mock_portal
        mock_client_fn.return_value = mock_client

        resp = await authed_client.post("/api/v1/billing/portal")
        assert resp.status_code == 200
        assert resp.json()["url"] == "https://billing.stripe.com/session/test"


# ---------------------------------------------------------------------------
# Entitlements tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entitlements_returns_current(client):
    mock_entitlements = {
        "tier": "community",
        "features": {"basic_analytics": True, "team_dashboard": False},
        "limits": {"users": 5, "repos": 3, "api_rate": 60},
        "is_licensed": False,
        "in_grace_period": False,
    }

    with patch(
        "dev_health_ops.api.billing.router.get_entitlements",
        return_value=mock_entitlements,
    ):
        resp = await client.get("/api/v1/billing/entitlements/org-abc")
        assert resp.status_code == 200
        data = resp.json()
        assert data["tier"] == "community"
        assert data["is_licensed"] is False
        assert data["limits"]["users"] == 5


# ---------------------------------------------------------------------------
# stripe_client unit tests
# ---------------------------------------------------------------------------


def test_map_price_id_to_tier():
    from dev_health_ops.api.billing.stripe_client import map_price_id_to_tier

    with patch.dict(
        "os.environ",
        {"STRIPE_PRICE_ID_TEAM": "price_t", "STRIPE_PRICE_ID_ENTERPRISE": "price_e"},
    ):
        reset_price_tier_map()
        from dev_health_ops.licensing.types import LicenseTier

        assert map_price_id_to_tier("price_t") == LicenseTier.TEAM
        assert map_price_id_to_tier("price_e") == LicenseTier.ENTERPRISE
        assert map_price_id_to_tier("price_unknown") is None


def test_get_tier_from_line_items():
    from dev_health_ops.api.billing.stripe_client import get_tier_from_line_items
    from dev_health_ops.licensing.types import LicenseTier

    with patch.dict("os.environ", {"STRIPE_PRICE_ID_ENTERPRISE": "price_e"}):
        reset_price_tier_map()
        items = [{"price": {"id": "price_e"}}]
        assert get_tier_from_line_items(items) == LicenseTier.ENTERPRISE

    reset_price_tier_map()
    assert get_tier_from_line_items([]) == LicenseTier.TEAM


def test_get_tier_price_id():
    from dev_health_ops.api.billing.stripe_client import get_tier_price_id
    from dev_health_ops.licensing.types import LicenseTier

    with patch.dict("os.environ", {"STRIPE_PRICE_ID_TEAM": "price_t"}):
        reset_price_tier_map()
        assert get_tier_price_id(LicenseTier.TEAM) == "price_t"
        assert get_tier_price_id(LicenseTier.ENTERPRISE) is None
