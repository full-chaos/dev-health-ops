"""Tests for the billing router (Stripe webhooks, checkout, portal, entitlements)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.billing.router import SignatureVerificationError, router
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

        mock_task.delay.assert_called_once()
        args, kwargs = mock_task.delay.call_args
        assert args[0] == "trial_expiring"
        assert args[1] == "00000000-0000-0000-0000-000000000001"
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
    from dev_health_ops.db import postgres_session_dependency

    async def _override_session():
        yield AsyncMock()

    app.dependency_overrides[postgres_session_dependency] = _override_session

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
    mock_invoice = MagicMock(id="inv_test", stripe_invoice_id="in_test", status="paid")
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
        mock_task.delay.assert_called_once_with(
            "invoice_receipt",
            "00000000-0000-0000-0000-000000000001",
            amount_cents=4900,
            currency="usd",
            invoice_url="https://invoice.stripe.com/i/test",
        )


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
    mock_invoice = MagicMock(id="inv_test", stripe_invoice_id="in_test", status="open")
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
        mock_task.delay.assert_called_once_with(
            "payment_failed",
            "00000000-0000-0000-0000-000000000001",
            amount_cents=4900,
            currency="usd",
            attempt_count=3,
        )


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
        mock_task.delay.assert_called_once_with(
            "subscription_cancelled",
            "00000000-0000-0000-0000-000000000001",
            tier="team",
        )


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
        mock_task.delay.assert_called_once_with(
            "subscription_changed",
            "00000000-0000-0000-0000-000000000001",
            old_tier="team",
            new_tier="enterprise",
        )


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
    mock_invoice = MagicMock(id="inv_test", stripe_invoice_id="in_test", status="paid")
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

    _tables = [
        Organization.__table__,
        BillingPlan.__table__,
        BillingPrice.__table__,
        FeatureBundle.__table__,
        PlanFeatureBundle.__table__,
        Subscription.__table__,
        SubscriptionEvent.__table__,
    ]

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
