"""Tests for the billing router (Stripe webhooks, checkout, portal, entitlements)."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Protocol, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.billing.router import SignatureVerificationError, router
from dev_health_ops.api.billing.stripe_client import reset_price_tier_map
from tests._helpers import tables_of


class _CallableCeleryTask(Protocol):
    def __call__(
        self, email_type: str | None = None, org_id: str | None = None, **kwargs: Any
    ) -> dict: ...
    def push_request(self, **kwargs: Any) -> None: ...
    def pop_request(self) -> None: ...


@pytest.fixture(autouse=True)
def _reset_price_map():
    reset_price_tier_map()
    yield
    reset_price_tier_map()


@pytest.fixture(autouse=True)
def _billing_env():
    with patch.dict("os.environ", {"APP_BASE_URL": "https://example.com"}):
        yield


@pytest.fixture(autouse=True)
def _billing_worker_route():
    with patch(
        "dev_health_ops.api.billing.router._route_billing_notification",
        new=AsyncMock(return_value="celery"),
    ):
        yield


def _assert_durable_billing_dispatch(mock_task: MagicMock) -> None:
    mock_task.delay.assert_called_once()
    args, kwargs = mock_task.delay.call_args
    assert args == ()
    assert set(kwargs) == {"durable_notification_id"}
    assert (
        str(uuid.UUID(kwargs["durable_notification_id"]))
        == kwargs["durable_notification_id"]
    )


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
async def test_webhook_subscription_trial_will_end_sends_expiring_email(client):
    event = _make_stripe_event(
        "customer.subscription.trial_will_end",
        {
            "metadata": {"org_id": "00000000-0000-0000-0000-000000000001"},
            "customer": "cus_test",
            "trial_end": 1_893_456_000,
        },
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch(
            "dev_health_ops.api.billing.router._enqueue_billing_notification",
            new_callable=AsyncMock,
        ) as mock_enqueue,
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
        assert resp.json() == {"status": "ok"}

        mock_enqueue.assert_awaited_once()
        awaited = mock_enqueue.await_args
        assert awaited is not None
        args, kwargs = awaited
        assert args == (
            "trial_expiring",
            "00000000-0000-0000-0000-000000000001",
        )
        assert kwargs["days_remaining"] >= 0
        assert kwargs["trial_end_date"] == "2030-01-01"


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
async def test_entitlements_org_endpoint_returns_per_org_state(client, app):
    from dev_health_ops.api.auth.router import get_current_user
    from dev_health_ops.api.services.auth import AuthenticatedUser
    from dev_health_ops.db import postgres_session_dependency

    async def _override_session():
        yield AsyncMock()

    app.dependency_overrides[postgres_session_dependency] = _override_session
    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser(
        user_id="00000000-0000-0000-0000-000000000002",
        email="member@example.com",
        org_id="00000000-0000-0000-0000-000000000001",
        role="member",
    )

    mock_entitlements = {
        "tier": "team",
        "features": {"team_dashboard": True},
        "limits": {"users": 25, "repos": 20, "api_rate": 300},
        "is_licensed": True,
        "in_grace_period": False,
        "is_trialing": True,
        "trial_ends_at": "2026-03-31T00:00:00+00:00",
    }

    mock_gating = SimpleNamespace(
        get_org_entitlements_from_db=AsyncMock(return_value=mock_entitlements)
    )

    try:
        with patch(
            "dev_health_ops.api.billing.router.importlib.import_module",
            return_value=mock_gating,
        ):
            resp = await client.get(
                "/api/v1/billing/entitlements/00000000-0000-0000-0000-000000000001"
            )
    finally:
        app.dependency_overrides.pop(postgres_session_dependency, None)
        app.dependency_overrides.pop(get_current_user, None)

    assert resp.status_code == 200
    body = resp.json()
    assert body["tier"] == "team"
    assert body["is_trialing"] is True
    assert body["trial_ends_at"] == "2026-03-31T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Webhook -> email integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_invoice_paid_sends_receipt_email(client):
    from contextlib import asynccontextmanager

    event = _make_stripe_event(
        "invoice.paid",
        {
            "metadata": {"org_id": "00000000-0000-0000-0000-000000000001"},
            "amount_due": 4900,
            "currency": "usd",
            "hosted_invoice_url": "https://invoice.stripe.com/i/test",
        },
    )
    event.id = "evt_test_123"

    mock_db = AsyncMock()
    mock_db.commit = AsyncMock()
    mock_db.rollback = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    mock_inv_svc = MagicMock()
    mock_inv_svc.is_duplicate_event = AsyncMock(return_value=False)
    mock_invoice = MagicMock(
        id="00000000-0000-0000-0000-000000000111",
        stripe_invoice_id="in_test",
        status="paid",
    )
    mock_inv_svc.upsert_invoice = AsyncMock(return_value=mock_invoice)
    mock_inv_svc.upsert_line_items = AsyncMock()
    mock_inv_svc.mark_paid = AsyncMock()

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_postgres_session", mock_session),
        patch("dev_health_ops.api.billing.router.invoice_service", mock_inv_svc),
        patch(
            "dev_health_ops.api.billing.router.send_billing_notification",
        ) as mock_task,
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
        assert resp.json() == {"status": "ok"}
        _assert_durable_billing_dispatch(mock_task)


@pytest.mark.asyncio
async def test_webhook_invoice_payment_failed_sends_email(client):
    from contextlib import asynccontextmanager

    event = _make_stripe_event(
        "invoice.payment_failed",
        {
            "metadata": {"org_id": "00000000-0000-0000-0000-000000000001"},
            "amount_due": 4900,
            "currency": "usd",
            "attempt_count": 3,
        },
    )
    event.id = "evt_test_123"

    mock_db = AsyncMock()
    mock_db.commit = AsyncMock()
    mock_db.rollback = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    mock_inv_svc = MagicMock()
    mock_inv_svc.is_duplicate_event = AsyncMock(return_value=False)
    mock_invoice = MagicMock(
        id="00000000-0000-0000-0000-000000000222",
        stripe_invoice_id="in_test",
        status="open",
    )
    mock_inv_svc.upsert_invoice = AsyncMock(return_value=mock_invoice)
    mock_inv_svc.upsert_line_items = AsyncMock()
    mock_inv_svc.mark_paid = AsyncMock()

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_postgres_session", mock_session),
        patch("dev_health_ops.api.billing.router.invoice_service", mock_inv_svc),
        patch(
            "dev_health_ops.api.billing.router.send_billing_notification",
        ) as mock_task,
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
        assert resp.json() == {"status": "ok"}
        _assert_durable_billing_dispatch(mock_task)


@pytest.mark.asyncio
async def test_webhook_subscription_deleted_sends_cancelled_email(client):
    from contextlib import asynccontextmanager

    event = _make_stripe_event(
        "customer.subscription.deleted",
        {
            "metadata": {"org_id": "00000000-0000-0000-0000-000000000001"},
            "customer": "cus_test",
        },
    )

    mock_result = MagicMock()
    mock_result.first.return_value = SimpleNamespace(tier="team")

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_postgres_session", mock_session),
        patch(
            "dev_health_ops.api.billing.router._process_subscription_event",
            new_callable=AsyncMock,
        ),
        patch(
            "dev_health_ops.api.billing.router._revoke_license", new_callable=AsyncMock
        ),
        patch(
            "dev_health_ops.api.billing.router.send_billing_notification",
        ) as mock_task,
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
        assert resp.json() == {"status": "ok"}
        _assert_durable_billing_dispatch(mock_task)


@pytest.mark.asyncio
async def test_webhook_subscription_updated_sends_changed_email(client):
    from contextlib import asynccontextmanager

    from dev_health_ops.licensing.types import LicenseTier

    event = _make_stripe_event(
        "customer.subscription.updated",
        {
            "metadata": {"org_id": "00000000-0000-0000-0000-000000000001"},
            "customer": "cus_test",
            "items": SimpleNamespace(
                data=[SimpleNamespace(price=SimpleNamespace(id="price_enterprise_123"))]
            ),
        },
    )

    mock_result = MagicMock()
    mock_result.first.return_value = SimpleNamespace(tier="team")

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_postgres_session", mock_session),
        patch(
            "dev_health_ops.api.billing.router._process_subscription_event",
            new_callable=AsyncMock,
        ),
        patch(
            "dev_health_ops.api.billing.router._persist_license", new_callable=AsyncMock
        ),
        patch(
            "dev_health_ops.api.billing.router.get_private_key",
            return_value="test_private_key",
        ),
        patch(
            "dev_health_ops.api.billing.router.sign_license",
            return_value="signed_license",
        ),
        patch(
            "dev_health_ops.api.billing.router.get_tier_from_line_items",
            return_value=LicenseTier.ENTERPRISE,
        ),
        patch(
            "dev_health_ops.api.billing.router.send_billing_notification",
        ) as mock_task,
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
        assert resp.json() == {"status": "ok"}
        _assert_durable_billing_dispatch(mock_task)


@pytest.mark.asyncio
async def test_webhook_email_failure_does_not_break_webhook(client):
    from contextlib import asynccontextmanager

    event = _make_stripe_event(
        "invoice.paid",
        {
            "metadata": {"org_id": "00000000-0000-0000-0000-000000000001"},
            "amount_due": 4900,
            "currency": "usd",
            "hosted_invoice_url": "https://invoice.stripe.com/i/test",
        },
    )
    event.id = "evt_test_123"

    mock_db = AsyncMock()
    mock_db.commit = AsyncMock()
    mock_db.rollback = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    mock_inv_svc = MagicMock()
    mock_inv_svc.is_duplicate_event = AsyncMock(return_value=False)
    mock_invoice = MagicMock(
        id="00000000-0000-0000-0000-000000000333",
        stripe_invoice_id="in_test",
        status="paid",
    )
    mock_inv_svc.upsert_invoice = AsyncMock(return_value=mock_invoice)
    mock_inv_svc.upsert_line_items = AsyncMock()
    mock_inv_svc.mark_paid = AsyncMock()

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_postgres_session", mock_session),
        patch("dev_health_ops.api.billing.router.invoice_service", mock_inv_svc),
        patch(
            "dev_health_ops.api.billing.router.send_billing_notification",
        ) as mock_task,
    ):
        # Simulate Celery dispatch failure (e.g. Redis down)
        mock_task.delay.side_effect = RuntimeError("broker unavailable")

        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client

        resp = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )

        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


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


# ---------------------------------------------------------------------------
# FeatureBundle key validation — Layer 1 (write-time)
# ---------------------------------------------------------------------------


def test_validate_bundle_feature_keys_valid():
    """Creating a bundle with known keys succeeds."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    # "git_sync" and "api_access" are both in STANDARD_FEATURES
    validate_bundle_feature_keys(["git_sync", "api_access"])


def test_validate_bundle_feature_keys_accepts_acr_purchased_feature():
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    validate_bundle_feature_keys(["agent_context_runtime"])


def test_validate_bundle_feature_keys_unknown_raises():
    """Creating a bundle with an unknown key raises ValueError naming the key."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    with pytest.raises(ValueError) as exc_info:
        validate_bundle_feature_keys(["git_sync", "totally_fake_feature"])

    assert "totally_fake_feature" in str(exc_info.value)


def test_validate_bundle_feature_keys_empty_succeeds():
    """Empty feature list is valid (no keys to check)."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    validate_bundle_feature_keys([])


def test_validate_bundle_feature_keys_all_standard():
    """All STANDARD_FEATURES keys pass validation."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )
    from dev_health_ops.models.licensing import STANDARD_FEATURES

    all_keys = [key for key, *_rest in STANDARD_FEATURES]
    validate_bundle_feature_keys(all_keys)  # must not raise


# ---------------------------------------------------------------------------
# FeatureBundle key validation — Layer 2 (startup-time)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_bundle_keys_clean_db_passes():
    """Startup check passes when all bundles reference known keys."""
    from unittest.mock import AsyncMock, MagicMock

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("core-bundle", ["git_sync", "basic_analytics"]),
        ("team-bundle", ["investment_view", "api_access"]),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    # Should not raise
    await validate_bundle_keys(mock_session)


@pytest.mark.asyncio
async def test_validate_bundle_keys_stale_raises():
    """Startup check raises RuntimeError when a stale key is found."""
    from unittest.mock import AsyncMock, MagicMock

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("good-bundle", ["git_sync"]),
        ("bad-bundle", ["git_sync", "old_removed_feature"]),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    with pytest.raises(RuntimeError) as exc_info:
        await validate_bundle_keys(mock_session)

    assert (
        "old_removed_feature" in str(exc_info.value)
        or "integrity check failed" in str(exc_info.value).lower()
    )


@pytest.mark.asyncio
async def test_validate_bundle_keys_allow_stale_env_var():
    """ALLOW_STALE_FEATURE_BUNDLES=1 causes stale keys to be logged as warnings
    instead of raising RuntimeError."""
    from unittest.mock import AsyncMock, MagicMock

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("bad-bundle", ["unknown_key_xyz"]),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    with patch.dict("os.environ", {"ALLOW_STALE_FEATURE_BUNDLES": "1"}):
        # Should NOT raise — only warn
        await validate_bundle_keys(mock_session)


@pytest.mark.asyncio
async def test_validate_bundle_keys_empty_bundles_passes():
    """Startup check passes when no bundles exist."""
    from unittest.mock import AsyncMock, MagicMock

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    await validate_bundle_keys(mock_session)


@pytest.mark.asyncio
async def test_validate_bundle_keys_null_features_passes():
    """Bundles with null/empty features list are skipped without error."""
    from unittest.mock import AsyncMock, MagicMock

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("empty-bundle", []),
        ("null-bundle", None),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    await validate_bundle_keys(mock_session)


# ---------------------------------------------------------------------------
# G4 (CHAOS-1207) — Bridge: plan subscription → org feature enablement
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def bridge_db(tmp_path):
    """SQLite in-memory DB with all billing + licensing tables for bridge tests."""
    from datetime import datetime, timezone

    from sqlalchemy import event as sa_event
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from dev_health_ops.models.billing import (
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
    )
    from dev_health_ops.models.git import Base
    from dev_health_ops.models.licensing import OrgLicense
    from dev_health_ops.models.subscriptions import Subscription, SubscriptionEvent
    from dev_health_ops.models.users import Organization

    db_path = tmp_path / "bridge.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )

    @sa_event.listens_for(engine.sync_engine, "connect")
    def _set_fk(dbapi_conn, _record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
        # SQLite doesn't have now(); register it so server_default=sa.text("now()") works.
        dbapi_conn.create_function(
            "now",
            0,
            lambda: datetime.now(timezone.utc).isoformat(sep=" "),
        )

    _tables = tables_of(
        Organization,
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
        Subscription,
        SubscriptionEvent,
        OrgLicense,
    )

    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=_tables))

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_enterprise_plan(session, plan_id, price_id, bundle_id):
    """Insert an enterprise BillingPlan with a FeatureBundle into the DB."""
    import uuid
    from datetime import datetime, timezone

    from dev_health_ops.models.billing import (
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
    )

    now = datetime.now(timezone.utc)
    plan = BillingPlan(
        id=plan_id,
        key="enterprise-monthly",
        name="Enterprise Monthly",
        tier="enterprise",
        created_at=now,
        updated_at=now,
    )
    price = BillingPrice(
        id=price_id,
        plan_id=plan_id,
        interval="monthly",
        amount=49900,
        created_at=now,
        updated_at=now,
    )
    bundle = FeatureBundle(
        id=bundle_id,
        key="enterprise-core",
        name="Enterprise Core",
        features=["sso_saml", "audit_log", "ip_allowlist"],
        created_at=now,
        updated_at=now,
    )
    pfb = PlanFeatureBundle(
        id=uuid.uuid4(),
        plan_id=plan_id,
        bundle_id=bundle_id,
    )
    session.add_all([plan, price, bundle, pfb])
    await session.commit()


def _make_stripe_sub(
    sub_id: str,
    stripe_price_id: str,
    org_id,
    status: str = "active",
    current_period_end: float = 2_000_000_000.0,
    customer: str = "cus_test",
):
    """Build a minimal Stripe subscription SimpleNamespace."""
    from types import SimpleNamespace

    price_ns = SimpleNamespace(id=stripe_price_id)
    item_ns = SimpleNamespace(price=price_ns)
    items_ns = SimpleNamespace(data=[item_ns])
    return SimpleNamespace(
        id=sub_id,
        customer=customer,
        status=status,
        metadata={"org_id": str(org_id)},
        current_period_start=1_700_000_000.0,
        current_period_end=current_period_end,
        cancel_at_period_end=False,
        canceled_at=None,
        trial_start=None,
        trial_end=None,
        items=items_ns,
    )


@pytest.mark.asyncio
async def test_subscription_creates_org_license(bridge_db):
    """Enterprise subscription creates OrgLicense with enterprise tier + plan features."""
    import uuid

    from sqlalchemy import select

    from dev_health_ops.api.billing.subscription_service import SubscriptionService
    from dev_health_ops.models.licensing import OrgLicense

    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    bundle_id = uuid.uuid4()
    stripe_price_id = "price_enterprise_monthly"

    async with bridge_db() as session:
        from sqlalchemy import select as sa_select

        from dev_health_ops.models.billing import BillingPrice
        from dev_health_ops.models.users import Organization

        await _seed_enterprise_plan(session, plan_id, price_id, bundle_id)

        # Update stripe_price_id on the BillingPrice row.
        price_row = (
            await session.execute(
                sa_select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one()
        price_row.stripe_price_id = stripe_price_id
        await session.commit()

        # Insert a minimal Organization row (needed for FK).
        org = Organization(
            id=org_id, slug=f"acme-corp-{org_id.hex[:8]}", name="Acme Corp"
        )
        session.add(org)
        await session.commit()

    stripe_sub = _make_stripe_sub("sub_new_1", stripe_price_id, org_id)

    async with bridge_db() as session:
        svc = SubscriptionService(session)
        await svc.upsert_from_stripe(stripe_sub, org_id)
        await session.commit()

    async with bridge_db() as session:
        lic = (
            await session.execute(select(OrgLicense).where(OrgLicense.org_id == org_id))
        ).scalar_one_or_none()
        assert lic is not None, "OrgLicense must be created after subscription upsert"
        assert lic.tier == "enterprise"
        features = lic.features_override
        assert isinstance(features, dict)
        assert features.get("sso_saml") is True
        assert features.get("audit_log") is True
        assert features.get("ip_allowlist") is True

        # CHAOS-2256 review: Organization.tier must be kept in lockstep so the
        # resolve_org_tier fallback never sees a stale value.
        from dev_health_ops.models.users import Organization

        org_row = (
            await session.execute(select(Organization).where(Organization.id == org_id))
        ).scalar_one()
        assert org_row.tier == "enterprise"


@pytest.mark.asyncio
async def test_subscription_update_does_not_duplicate_license(bridge_db):
    """Upserting an existing subscription updates OrgLicense without duplicating."""
    import uuid

    from sqlalchemy import select

    from dev_health_ops.api.billing.subscription_service import SubscriptionService
    from dev_health_ops.models.licensing import OrgLicense

    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    bundle_id = uuid.uuid4()
    stripe_price_id = "price_ent_upd"
    stripe_sub_id = "sub_upd_1"

    async with bridge_db() as session:
        from sqlalchemy import select as sa_select

        from dev_health_ops.models.billing import BillingPrice
        from dev_health_ops.models.users import Organization

        await _seed_enterprise_plan(session, plan_id, price_id, bundle_id)
        price_row = (
            await session.execute(
                sa_select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one()
        price_row.stripe_price_id = stripe_price_id
        await session.commit()

        org = Organization(
            id=org_id, slug=f"acme-corp-2-{org_id.hex[:8]}", name="Acme Corp 2"
        )
        session.add(org)
        await session.commit()

    # First upsert — creates.
    stripe_sub = _make_stripe_sub(stripe_sub_id, stripe_price_id, org_id)
    async with bridge_db() as session:
        svc = SubscriptionService(session)
        await svc.upsert_from_stripe(stripe_sub, org_id)
        await session.commit()

    # Second upsert with updated period — must update, not duplicate.
    stripe_sub2 = _make_stripe_sub(
        stripe_sub_id, stripe_price_id, org_id, current_period_end=2_100_000_000.0
    )
    async with bridge_db() as session:
        svc = SubscriptionService(session)
        await svc.upsert_from_stripe(stripe_sub2, org_id)
        await session.commit()

    async with bridge_db() as session:
        rows = (
            (
                await session.execute(
                    select(OrgLicense).where(OrgLicense.org_id == org_id)
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1, "Upsert must not duplicate OrgLicense rows"
        assert rows[0].tier == "enterprise"


@pytest.mark.asyncio
async def test_subscription_sync_preserves_manually_managed_license(bridge_db):
    import uuid

    from sqlalchemy import select

    from dev_health_ops.api.billing.subscription_service import SubscriptionService
    from dev_health_ops.models.licensing import OrgLicense
    from dev_health_ops.models.users import Organization

    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    bundle_id = uuid.uuid4()
    stripe_price_id = "price_manual_guard"

    async with bridge_db() as session:
        from sqlalchemy import select as sa_select

        from dev_health_ops.models.billing import BillingPrice

        await _seed_enterprise_plan(session, plan_id, price_id, bundle_id)
        price_row = (
            await session.execute(
                sa_select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one()
        price_row.stripe_price_id = stripe_price_id

        org = Organization(
            id=org_id, slug=f"manual-org-{org_id.hex[:8]}", name="Manual Org"
        )
        org.tier = "enterprise"
        org.managed_by = "manual"
        session.add(org)
        session.add(
            OrgLicense(
                org_id=org_id,
                tier="enterprise",
                license_type="saas",
                managed_by="manual",
                customer_id="cus_manual",
            )
        )
        await session.commit()

    stripe_sub = _make_stripe_sub("sub_manual_guard", stripe_price_id, org_id)

    async with bridge_db() as session:
        svc = SubscriptionService(session)
        await svc.upsert_from_stripe(stripe_sub, org_id)
        await session.commit()

    async with bridge_db() as session:
        org_row = (
            await session.execute(select(Organization).where(Organization.id == org_id))
        ).scalar_one()
        lic = (
            await session.execute(select(OrgLicense).where(OrgLicense.org_id == org_id))
        ).scalar_one()

    assert org_row.tier == "enterprise"
    assert org_row.managed_by == "manual"
    assert lic.tier == "enterprise"
    assert lic.managed_by == "manual"
    assert lic.customer_id == "cus_manual"


@pytest.mark.asyncio
async def test_subscription_cancellation_downgrades_license(bridge_db):
    """Cancelled subscription downgrades OrgLicense to community; row survives."""
    import uuid

    from sqlalchemy import select

    from dev_health_ops.api.billing.subscription_service import SubscriptionService
    from dev_health_ops.models.licensing import OrgLicense

    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    bundle_id = uuid.uuid4()
    stripe_price_id = "price_ent_cancel"
    stripe_sub_id = "sub_cancel_1"

    async with bridge_db() as session:
        from sqlalchemy import select as sa_select

        from dev_health_ops.models.billing import BillingPrice
        from dev_health_ops.models.users import Organization

        await _seed_enterprise_plan(session, plan_id, price_id, bundle_id)
        price_row = (
            await session.execute(
                sa_select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one()
        price_row.stripe_price_id = stripe_price_id
        await session.commit()

        org = Organization(
            id=org_id, slug=f"cancelling-corp-{org_id.hex[:8]}", name="Cancelling Corp"
        )
        session.add(org)
        await session.commit()

    # Active subscription first.
    stripe_sub = _make_stripe_sub(stripe_sub_id, stripe_price_id, org_id)
    async with bridge_db() as session:
        svc = SubscriptionService(session)
        await svc.upsert_from_stripe(stripe_sub, org_id)
        await session.commit()

    # Cancel the subscription.
    stripe_cancelled = _make_stripe_sub(
        stripe_sub_id, stripe_price_id, org_id, status="canceled"
    )
    async with bridge_db() as session:
        svc = SubscriptionService(session)
        await svc.upsert_from_stripe(stripe_cancelled, org_id)
        await session.commit()

    async with bridge_db() as session:
        lic = (
            await session.execute(select(OrgLicense).where(OrgLicense.org_id == org_id))
        ).scalar_one_or_none()
        assert lic is not None, "OrgLicense row must survive cancellation (audit trail)"
        assert lic.tier == "community", (
            "Cancelled subscription must downgrade to community"
        )
        assert lic.is_valid is False, "Cancelled OrgLicense must be marked invalid"
        assert lic.features_override == {}, "No features for community downgrade"


@pytest.mark.asyncio
async def test_bridge_skips_unknown_keys(bridge_db, caplog):
    """Bundle with an unknown feature key logs a warning but does not raise."""
    import logging
    import uuid

    from dev_health_ops.api.billing.subscription_service import SubscriptionService

    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    bundle_id = uuid.uuid4()
    stripe_price_id = "price_unknown_keys"

    async with bridge_db() as session:
        from datetime import datetime, timezone

        from dev_health_ops.models.billing import (
            BillingPlan,
            BillingPrice,
            FeatureBundle,
            PlanFeatureBundle,
        )
        from dev_health_ops.models.users import Organization

        now = datetime.now(timezone.utc)
        plan = BillingPlan(
            id=plan_id,
            key="team-monthly",
            name="Team Monthly",
            tier="team",
            created_at=now,
            updated_at=now,
        )
        price = BillingPrice(
            id=price_id,
            plan_id=plan_id,
            interval="monthly",
            amount=2900,
            stripe_price_id=stripe_price_id,
            created_at=now,
            updated_at=now,
        )
        # Bundle with one valid key and one bogus key.
        bundle = FeatureBundle(
            id=bundle_id,
            key="team-core",
            name="Team Core",
            features=["api_access", "totally_unknown_feature_xyz"],
            created_at=now,
            updated_at=now,
        )
        pfb = PlanFeatureBundle(id=uuid.uuid4(), plan_id=plan_id, bundle_id=bundle_id)
        org = Organization(
            id=org_id, slug=f"bad-bundle-{org_id.hex[:8]}", name="Bad Bundle Corp"
        )
        session.add_all([plan, price, bundle, pfb, org])
        await session.commit()

    stripe_sub = _make_stripe_sub("sub_unk_1", stripe_price_id, org_id)

    with caplog.at_level(
        logging.WARNING, logger="dev_health_ops.api.billing.subscription_service"
    ):
        async with bridge_db() as session:
            svc = SubscriptionService(session)
            # Must not raise.
            await svc.upsert_from_stripe(stripe_sub, org_id)
            await session.commit()

    assert any("unknown feature key" in r.message for r in caplog.records), (
        "A warning must be logged for the unknown feature key"
    )

    from sqlalchemy import select

    from dev_health_ops.models.licensing import OrgLicense

    async with bridge_db() as session:
        lic = (
            await session.execute(select(OrgLicense).where(OrgLicense.org_id == org_id))
        ).scalar_one_or_none()
        assert lic is not None
        # Valid key survived; bogus key was dropped.
        assert "api_access" in (lic.features_override or [])
        assert "totally_unknown_feature_xyz" not in (lic.features_override or [])


@pytest.mark.asyncio
async def test_bridge_failure_rolls_back_subscription(bridge_db):
    """If OrgLicense write fails, the entire transaction (including Subscription) rolls back."""
    import uuid

    from sqlalchemy import select
    from sqlalchemy.exc import SQLAlchemyError

    from dev_health_ops.api.billing.subscription_service import SubscriptionService
    from dev_health_ops.models.subscriptions import Subscription

    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    bundle_id = uuid.uuid4()
    stripe_price_id = "price_atomic_test"
    stripe_sub_id = "sub_atomic_1"

    async with bridge_db() as session:
        from sqlalchemy import select as sa_select

        from dev_health_ops.models.billing import BillingPrice
        from dev_health_ops.models.users import Organization

        await _seed_enterprise_plan(session, plan_id, price_id, bundle_id)
        price_row = (
            await session.execute(
                sa_select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one()
        price_row.stripe_price_id = stripe_price_id
        await session.commit()

        org = Organization(
            id=org_id, slug=f"atomic-corp-{org_id.hex[:8]}", name="Atomic Corp"
        )
        session.add(org)
        await session.commit()

    stripe_sub = _make_stripe_sub(stripe_sub_id, stripe_price_id, org_id)

    # Patch _sync_org_license to raise, simulating a DB write failure.
    with patch.object(
        SubscriptionService,
        "_sync_org_license",
        side_effect=SQLAlchemyError("simulated write failure"),
    ):
        with pytest.raises(SQLAlchemyError):
            async with bridge_db() as session:
                svc = SubscriptionService(session)
                await svc.upsert_from_stripe(stripe_sub, org_id)
                await session.commit()

    # Subscription must not have been committed.
    async with bridge_db() as session:
        sub_row = (
            await session.execute(
                select(Subscription).where(
                    Subscription.stripe_subscription_id == stripe_sub_id
                )
            )
        ).scalar_one_or_none()
        assert sub_row is None, (
            "Subscription must be rolled back when OrgLicense write fails"
        )


# ---------------------------------------------------------------------------
# G7 (CHAOS-1210) — billing_prices ON DELETE CASCADE
#
# SQLite requires PRAGMA foreign_keys=ON to enforce FK constraints.
# We set it via a connection event listener so cascade fires in unit tests.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def billing_cascade_db(tmp_path):
    """SQLite DB with FK enforcement, containing billing + subscription tables."""

    from sqlalchemy import event as sa_event
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from dev_health_ops.models.billing import (
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
    )
    from dev_health_ops.models.git import Base
    from dev_health_ops.models.subscriptions import Subscription, SubscriptionEvent
    from dev_health_ops.models.users import Organization

    db_path = tmp_path / "billing-cascade.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}", connect_args={"check_same_thread": False}
    )

    @sa_event.listens_for(engine.sync_engine, "connect")
    def _set_fk_pragma(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    _tables = tables_of(
        Organization,
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
        Subscription,
        SubscriptionEvent,
    )

    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=_tables))

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_delete_billing_plan_cascades_to_prices(billing_cascade_db):
    """Deleting a BillingPlan removes its BillingPrice rows (G7, CHAOS-1210)."""
    import uuid
    from datetime import datetime, timezone

    from sqlalchemy import select

    from dev_health_ops.models.billing import BillingPlan, BillingPrice

    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    async with billing_cascade_db() as session:
        plan = BillingPlan(
            id=plan_id,
            key="cascade-plan",
            name="Cascade Plan",
            tier="team",
            created_at=now,
            updated_at=now,
        )
        price = BillingPrice(
            id=price_id,
            plan_id=plan_id,
            interval="monthly",
            amount=2900,
            created_at=now,
            updated_at=now,
        )
        session.add_all([plan, price])
        await session.commit()

    async with billing_cascade_db() as session:
        assert (
            await session.execute(
                select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one_or_none() is not None

    async with billing_cascade_db() as session:
        plan_obj = (
            await session.execute(select(BillingPlan).where(BillingPlan.id == plan_id))
        ).scalar_one()
        await session.delete(plan_obj)
        await session.commit()

    async with billing_cascade_db() as session:
        gone = (
            await session.execute(
                select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one_or_none()
        assert gone is None, (
            "billing_prices row must cascade away when its plan is deleted"
        )


def test_subscription_billing_plan_fk_has_no_cascade():
    """Assert at model-metadata level that Subscription.billing_plan_id has no ondelete.

    G7 (CHAOS-1210): billing_prices.plan_id gets CASCADE; subscriptions.billing_plan_id
    intentionally does NOT, so subscription history survives plan deletion.

    NOTE: On PostgreSQL, deleting a plan with active subscriptions that reference
    its prices (via billing_price_id) will raise an IntegrityError unless those
    subscriptions are cleaned up first or billing_prices gets SET NULL — this is
    expected behaviour; subscription rows are historical records and should be
    archived before plan deletion in production.
    """
    from sqlalchemy import inspect

    from dev_health_ops.models.subscriptions import Subscription

    mapper = inspect(Subscription)
    for col in mapper.columns:
        if col.name == "billing_plan_id":
            fk = list(col.foreign_keys)[0]
            assert fk.ondelete is None or fk.ondelete.upper() != "CASCADE", (
                "subscriptions.billing_plan_id must NOT cascade — it is a historical reference"
            )
            return
    raise AssertionError("billing_plan_id column not found on Subscription model")


# ---------------------------------------------------------------------------
# Poison-message guards: non-UUID org_id must never enqueue or retry
# ---------------------------------------------------------------------------


class TestInvalidOrgIdGuards:
    def test_task_drops_invalid_org_id_without_retry(self, caplog):
        """A malformed org_id is permanently bad — the task must drop it
        instead of retry-looping (observed live with Stripe TEST webhook
        fixture ids like 'org-abc')."""
        from dev_health_ops.workers.system_ops import send_billing_notification

        task = cast(_CallableCeleryTask, send_billing_notification)
        task.push_request(id="billing-bad-org", retries=0)
        try:
            with caplog.at_level(
                logging.ERROR, logger="dev_health_ops.workers.system_ops"
            ):
                result = task("subscription_cancelled", "org-abc", tier="team")
        finally:
            task.pop_request()

        assert result == {
            "status": "dropped",
            "reason": "invalid_org_id",
            "org_id": "org-abc",
        }
        assert (
            "Billing notification dropped: invalid organization identifier"
            in caplog.text
        )
        assert "org-abc" not in caplog.text
        assert "subscription_cancelled" not in caplog.text

    def test_task_logs_unsupported_email_type_without_payload_value(self, caplog):
        from dev_health_ops.workers.system_ops import send_billing_notification

        task = cast(_CallableCeleryTask, send_billing_notification)
        task.push_request(id="billing-unknown-type", retries=0)
        try:
            with caplog.at_level(
                logging.ERROR, logger="dev_health_ops.workers.system_ops"
            ):
                result = task("customer-provided-email-type", str(uuid.uuid4()))
        finally:
            task.pop_request()

        assert result == {
            "status": "error",
            "reason": "unknown_email_type: customer-provided-email-type",
        }
        assert "Billing notification dropped: unsupported email type" in caplog.text
        assert "customer-provided-email-type" not in caplog.text

    def test_task_retry_log_excludes_delivery_error_and_identifiers(self, caplog):
        from dev_health_ops.workers.system_ops import send_billing_notification

        task = cast(_CallableCeleryTask, send_billing_notification)
        org_id = str(uuid.uuid4())
        task.push_request(id="billing-delivery-failure", retries=0)
        try:
            with (
                patch(
                    "dev_health_ops.workers.system_ops.run_async",
                    side_effect=RuntimeError("private mail provider response"),
                ),
                patch(
                    "dev_health_ops.api.services.billing_emails.send_subscription_cancelled",
                    return_value=None,
                ),
                caplog.at_level(
                    logging.WARNING, logger="dev_health_ops.workers.system_ops"
                ),
                pytest.raises(RuntimeError, match="private mail provider response"),
            ):
                task("subscription_cancelled", org_id, tier="team")
        finally:
            task.pop_request()

        assert "Billing notification delivery failed (attempt 1/4)" in caplog.text
        assert "private mail provider response" not in caplog.text
        assert org_id not in caplog.text
        assert "subscription_cancelled" not in caplog.text


# ---------------------------------------------------------------------------
# CHAOS-3952: a lost HTTP response after Python already sent the email must
# not resend it on the River retry. The durable row stays loadable across
# retries (nothing marks it consumed), so two identical dispatches from the
# bridge — same durable_notification_id, simulating "Go never saw success and
# retried" — must still send exactly one email.
# ---------------------------------------------------------------------------


class TestBillingNotificationCompletionFence:
    @staticmethod
    def _row(idempotency_key: str) -> dict[str, Any]:
        return {
            "email_type": "invoice_receipt",
            "org_id": str(uuid.uuid4()),
            "attributes": {
                "amount_cents": 500,
                "currency": "usd",
                "invoice_url": "https://x",
            },
            "idempotency_key": idempotency_key,
            "claimed_at": None,
            "completed_at": None,
        }

    @staticmethod
    def _fake_load(row: dict[str, Any]):
        def fake_load(_id: str):
            return (
                row["email_type"],
                row["org_id"],
                row["attributes"],
                row["idempotency_key"],
            )

        return fake_load

    @staticmethod
    def _fake_claim(row: dict[str, Any]):
        from dev_health_ops.workers.system_ops import _ClaimResult

        def fake_claim(_id: str) -> _ClaimResult:
            if row["claimed_at"] is not None:
                return _ClaimResult(
                    claimed=False,
                    claimed_at=row["claimed_at"],
                    completed_at=row["completed_at"],
                )
            row["claimed_at"] = datetime.now(timezone.utc)
            return _ClaimResult(claimed=True)

        return fake_claim

    def test_duplicate_durable_dispatch_sends_exactly_one_email(self):
        from dev_health_ops.workers.system_ops import send_billing_notification

        notification_id = str(uuid.uuid4())
        key = "billing:fence-key"
        task = cast(_CallableCeleryTask, send_billing_notification)
        row = self._row(key)

        def fake_mark_completed(_id: str) -> None:
            row["completed_at"] = datetime.now(timezone.utc)

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                side_effect=self._fake_load(row),
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion",
                side_effect=self._fake_claim(row),
            ),
            patch(
                "dev_health_ops.workers.system_ops._mark_billing_notification_completed",
                side_effect=fake_mark_completed,
            ),
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                new_callable=AsyncMock,
                return_value=None,
            ) as send_invoice_receipt,
        ):
            task.push_request(id="billing-fence-1", retries=0)
            try:
                first = task(
                    durable_notification_id=notification_id, idempotency_key=key
                )
            finally:
                task.pop_request()

            task.push_request(id="billing-fence-2", retries=0)
            try:
                second = task(
                    durable_notification_id=notification_id, idempotency_key=key
                )
            finally:
                task.pop_request()

        assert first["status"] == "sent"
        assert first.get("already_sent") is not True
        assert second["status"] == "sent"
        assert second.get("already_sent") is True
        assert row["completed_at"] is not None
        # The defect (CHAOS-3952): a lost-response retry replays the durable
        # row unchanged, so nothing distinguished the second dispatch from
        # the first and the email went out twice.
        assert send_invoice_receipt.call_count == 1, (
            f"expected exactly one email dispatch across two identical "
            f"durable retries, got {send_invoice_receipt.call_count}"
        )

    def test_idempotency_key_mismatch_is_dropped_without_sending(self):
        from dev_health_ops.workers.system_ops import send_billing_notification

        notification_id = str(uuid.uuid4())
        org_id = str(uuid.uuid4())
        task = cast(_CallableCeleryTask, send_billing_notification)

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                return_value=(
                    "invoice_receipt",
                    org_id,
                    {
                        "amount_cents": 500,
                        "currency": "usd",
                        "invoice_url": "https://x",
                    },
                    "billing:actual-key",
                ),
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion"
            ) as claim,
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                new_callable=AsyncMock,
                return_value=None,
            ) as send_invoice_receipt,
        ):
            task.push_request(id="billing-fence-mismatch", retries=0)
            try:
                result = task(
                    durable_notification_id=notification_id,
                    idempotency_key="billing:wrong-key",
                )
            finally:
                task.pop_request()

        assert result == {
            "status": "dropped",
            "reason": "idempotency_key_mismatch",
        }
        send_invoice_receipt.assert_not_called()
        # A mismatched key is a data-integrity failure, not a dedup
        # question -- must never even attempt a claim.
        claim.assert_not_called()

    def test_stale_claim_is_reported_not_silently_treated_as_sent(self):
        """(c) A claim that is old and never completed must be its own
        visible outcome, not masquerade as `already_sent`."""
        from datetime import timedelta

        from dev_health_ops.workers.system_ops import (
            _STALE_CLAIM_THRESHOLD_SECONDS,
            _ClaimResult,
            send_billing_notification,
        )

        notification_id = str(uuid.uuid4())
        org_id = str(uuid.uuid4())
        task = cast(_CallableCeleryTask, send_billing_notification)
        stale_claimed_at = datetime.now(timezone.utc) - timedelta(
            seconds=_STALE_CLAIM_THRESHOLD_SECONDS + 60
        )

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                return_value=(
                    "invoice_receipt",
                    org_id,
                    {
                        "amount_cents": 500,
                        "currency": "usd",
                        "invoice_url": "https://x",
                    },
                    "billing:stale-key",
                ),
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion",
                return_value=_ClaimResult(
                    claimed=False, claimed_at=stale_claimed_at, completed_at=None
                ),
            ),
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                new_callable=AsyncMock,
                return_value=None,
            ) as send_invoice_receipt,
            patch(
                "dev_health_ops.workers.system_ops.BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL"
            ) as fence_counter,
        ):
            task.push_request(id="billing-stale-claim", retries=0)
            try:
                result = task(
                    durable_notification_id=notification_id,
                    idempotency_key="billing:stale-key",
                )
            finally:
                task.pop_request()

        assert result["status"] != "sent", (
            f"a stale, unresolved claim must not be reported as sent: {result}"
        )
        assert result.get("already_sent") is not True
        assert result["reason"] == "stale_claim"
        send_invoice_receipt.assert_not_called()
        fence_counter.labels.assert_called_once_with(outcome="stale_claim_detected")

    def test_recent_unresolved_claim_is_still_suppressed_as_a_duplicate(self):
        """The mirror of the stale-claim test: a claim only seconds old
        (still within the normal retry/backoff window) is NOT yet reported
        as stale — it is the ordinary duplicate-suppression path."""
        from dev_health_ops.workers.system_ops import (
            _ClaimResult,
            send_billing_notification,
        )

        notification_id = str(uuid.uuid4())
        org_id = str(uuid.uuid4())
        task = cast(_CallableCeleryTask, send_billing_notification)
        recent_claimed_at = datetime.now(timezone.utc)

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                return_value=(
                    "invoice_receipt",
                    org_id,
                    {
                        "amount_cents": 500,
                        "currency": "usd",
                        "invoice_url": "https://x",
                    },
                    "billing:recent-key",
                ),
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion",
                return_value=_ClaimResult(
                    claimed=False, claimed_at=recent_claimed_at, completed_at=None
                ),
            ),
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                new_callable=AsyncMock,
                return_value=None,
            ) as send_invoice_receipt,
        ):
            task.push_request(id="billing-recent-claim", retries=0)
            try:
                result = task(
                    durable_notification_id=notification_id,
                    idempotency_key="billing:recent-key",
                )
            finally:
                task.pop_request()

        assert result["status"] == "sent"
        assert result.get("already_sent") is True
        send_invoice_receipt.assert_not_called()

    def test_post_send_fence_write_failure_does_not_release_the_claim_or_retry(self):
        """Codex round 2, P1+P2 (both executed): the send SUCCEEDED here —
        a broad try/except around both the send and the completion write
        let a transient failure writing completed_at release the claim and
        raise self.retry(), which would re-send an email that already went
        out. The completion write is bookkeeping on top of a fact already
        true, not a gate on it: its failure must be reported as its own
        outcome and must neither release the claim nor retry the task."""
        from dev_health_ops.workers.system_ops import send_billing_notification

        notification_id = str(uuid.uuid4())
        key = "billing:fence-write-failure-key"
        task = cast(_CallableCeleryTask, send_billing_notification)
        row = self._row(key)

        def fake_mark_completed_raises(_id: str) -> None:
            raise RuntimeError("transient database error writing completed_at")

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                side_effect=self._fake_load(row),
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion",
                side_effect=self._fake_claim(row),
            ),
            patch(
                "dev_health_ops.workers.system_ops._mark_billing_notification_completed",
                side_effect=fake_mark_completed_raises,
            ),
            patch(
                "dev_health_ops.workers.system_ops._release_billing_notification_completion_claim"
            ) as release_claim,
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                new_callable=AsyncMock,
                return_value=None,
            ) as send_invoice_receipt,
        ):
            task.push_request(id="billing-fence-write-failure", retries=0)
            try:
                result = task(
                    durable_notification_id=notification_id, idempotency_key=key
                )
            finally:
                task.pop_request()

        # The email genuinely went out -- this call must not raise/retry.
        assert send_invoice_receipt.call_count == 1
        assert result["status"] == "sent"
        assert result.get("already_sent") is not True
        # Releasing here would let a retry send a SECOND email for a
        # notification that already sent successfully.
        release_claim.assert_not_called()
        assert row["claimed_at"] is not None
        assert row["completed_at"] is None

    def test_malformed_durable_attributes_release_claim_and_drop_permanently(self):
        """Codex round 3, P1 (executed): a claim taken BEFORE the durable
        row's attributes are coerced left every un-guarded coercion/
        validation call site able to leave a held claim with no release.
        A malformed stored value (retrying never fixes stored data) must
        release the claim and drop -- not raise/retry, and not silently
        leave the claim held for a later attempt to misreport as sent.

        Also pins lane-4441's peer-review P3 (2026-09-02): a claim taken
        and then permanently dropped must be its own counted outcome, not
        invisible next to every other fence outcome."""
        from dev_health_ops.workers.system_ops import send_billing_notification

        notification_id = str(uuid.uuid4())
        key = "billing:malformed-attrs-key"
        task = cast(_CallableCeleryTask, send_billing_notification)
        row = self._row(key)
        row["attributes"] = {"amount_cents": "not-an-integer"}

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                side_effect=self._fake_load(row),
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion",
                side_effect=self._fake_claim(row),
            ),
            patch(
                "dev_health_ops.workers.system_ops._release_billing_notification_completion_claim",
                side_effect=lambda _id: row.__setitem__("claimed_at", None),
            ) as release_claim,
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                new_callable=AsyncMock,
                return_value=None,
            ) as send_invoice_receipt,
            patch(
                "dev_health_ops.workers.system_ops.BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL"
            ) as fence_counter,
        ):
            task.push_request(id="billing-malformed-attrs", retries=0)
            try:
                result = task(
                    durable_notification_id=notification_id, idempotency_key=key
                )
            finally:
                task.pop_request()

        assert result == {
            "status": "dropped",
            "reason": "malformed_notification_attributes",
        }
        send_invoice_receipt.assert_not_called()
        release_claim.assert_called_once_with(notification_id)
        fence_counter.labels.assert_called_once_with(outcome="permanent_drop")
        assert row["claimed_at"] is None, (
            "a permanent drop must release the claim -- otherwise a later "
            "attempt misreports this never-sent email as already_sent"
        )

    def test_claim_release_write_failure_is_swallowed_not_raised(self):
        """Codex round 3, P1 (executed): the release call sits inside an
        already-caught exception's handler. If a DB failure writing
        claimed_at=NULL were allowed to propagate, it would REPLACE the
        original exception -- skipping self.retry() entirely and leaving
        the caller with no idea cleanup failed. The function must swallow
        its own failure (logged) so the caller's error handling always
        completes."""
        from dev_health_ops.workers.system_ops import (
            _release_billing_notification_completion_claim,
        )

        with patch(
            "dev_health_ops.db.get_postgres_session_sync",
            side_effect=RuntimeError("db unavailable"),
        ):
            # Must not raise.
            _release_billing_notification_completion_claim(str(uuid.uuid4()))

    def test_send_failure_releases_the_claim_so_a_retry_can_still_send(self):
        """Codex round 1, P1 (executed): claim-then-send means an ordinary
        transient send failure must release the claim, or the email is
        silently never sent on any later retry. Model: first attempt's send
        raises (a real transient failure, not a crash) -> the claim it took
        must be released -> a second attempt claims again and sends."""
        from dev_health_ops.workers.system_ops import (
            _ClaimResult,
            send_billing_notification,
        )

        notification_id = str(uuid.uuid4())
        org_id = str(uuid.uuid4())
        task = cast(_CallableCeleryTask, send_billing_notification)

        row: dict[str, Any] = {
            "email_type": "invoice_receipt",
            "org_id": org_id,
            "attributes": {
                "amount_cents": 500,
                "currency": "usd",
                "invoice_url": "https://x",
            },
            "idempotency_key": "billing:retry-key",
            "claimed_at": None,
            "completed_at": None,
        }

        def fake_load(_id: str):
            return (
                row["email_type"],
                row["org_id"],
                row["attributes"],
                row["idempotency_key"],
            )

        def fake_claim(_id: str) -> _ClaimResult:
            if row["claimed_at"] is not None:
                return _ClaimResult(
                    claimed=False,
                    claimed_at=row["claimed_at"],
                    completed_at=row["completed_at"],
                )
            row["claimed_at"] = datetime.now(timezone.utc)
            return _ClaimResult(claimed=True)

        def fake_release(_id: str) -> None:
            row["claimed_at"] = None

        send_calls = {"n": 0}

        async def flaky_send(*args, **kwargs):
            send_calls["n"] += 1
            if send_calls["n"] == 1:
                raise RuntimeError("transient email provider error")
            return None

        def fake_mark_completed(_id: str) -> None:
            row["completed_at"] = datetime.now(timezone.utc)

        with (
            patch(
                "dev_health_ops.workers.system_ops._load_billing_notification",
                side_effect=fake_load,
            ),
            patch(
                "dev_health_ops.workers.system_ops._claim_billing_notification_completion",
                side_effect=fake_claim,
            ),
            patch(
                "dev_health_ops.workers.system_ops._release_billing_notification_completion_claim",
                side_effect=fake_release,
            ) as release_claim,
            patch(
                "dev_health_ops.workers.system_ops._mark_billing_notification_completed",
                side_effect=fake_mark_completed,
            ),
            patch(
                "dev_health_ops.api.services.billing_emails.send_invoice_receipt",
                side_effect=flaky_send,
            ) as send_invoice_receipt,
        ):
            task.push_request(id="billing-retry-1", retries=0)
            try:
                with pytest.raises(Exception):
                    task(
                        durable_notification_id=notification_id,
                        idempotency_key="billing:retry-key",
                    )
            finally:
                task.pop_request()

            release_claim.assert_called_once_with(notification_id)
            assert row["claimed_at"] is None, (
                "the failed attempt's claim must be released, or this "
                "notification can never be sent again"
            )

            task.push_request(id="billing-retry-2", retries=1)
            try:
                second = task(
                    durable_notification_id=notification_id,
                    idempotency_key="billing:retry-key",
                )
            finally:
                task.pop_request()

        assert second["status"] == "sent"
        assert second.get("already_sent") is not True
        assert send_invoice_receipt.call_count == 2
