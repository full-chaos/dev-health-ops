from __future__ import annotations

import uuid
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.billing.router import router as billing_router


def _subscription_service_cls():
    module = importlib.import_module("dev_health_ops.api.billing.subscription_service")
    return getattr(module, "SubscriptionService")


def _subscription_models():
    module = importlib.import_module("dev_health_ops.models.subscriptions")
    return getattr(module, "Subscription"), getattr(module, "SubscriptionEvent")


def _make_stripe_event(event_type: str, event_id: str = "evt_1") -> SimpleNamespace:
    subscription = SimpleNamespace(
        id="sub_123",
        customer="cus_123",
        status="active",
        metadata={"org_id": str(uuid.uuid4())},
        current_period_start=1_700_000_000,
        current_period_end=1_700_086_400,
        cancel_at_period_end=False,
        items=SimpleNamespace(data=[]),
    )
    return SimpleNamespace(
        id=event_id,
        type=event_type,
        data=SimpleNamespace(object=subscription),
    )


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(billing_router)
    return app


@pytest.fixture
def authed_app():
    from dev_health_ops.api.auth.router import get_current_user
    from dev_health_ops.api.services.auth import AuthenticatedUser
    from dev_health_ops.db import postgres_session_dependency

    app = _build_app()
    mock_session = AsyncMock()

    async def _session_override():
        yield mock_session

    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser(
        user_id="user-1",
        email="test@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
    )
    app.dependency_overrides[postgres_session_dependency] = _session_override
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def authed_client(authed_app):
    transport = ASGITransport(app=authed_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def test_subscription_models_instantiation() -> None:
    SubscriptionService = _subscription_service_cls()
    Subscription, SubscriptionEvent = _subscription_models()
    org_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    sub = Subscription(
        org_id=org_id,
        billing_plan_id=plan_id,
        billing_price_id=price_id,
        stripe_subscription_id="sub_123",
        stripe_customer_id="cus_123",
        status="active",
        current_period_start=SubscriptionService._to_dt(1_700_000_000),
        current_period_end=SubscriptionService._to_dt(1_700_086_400),
    )
    event = SubscriptionEvent(
        subscription_id=sub.id,
        stripe_event_id="evt_123",
        event_type="customer.subscription.updated",
        new_status="active",
    )
    assert sub.status == "active"
    assert event.stripe_event_id == "evt_123"


@pytest.mark.asyncio
async def test_subscription_service_idempotency_skips_duplicate_event() -> None:
    SubscriptionService = _subscription_service_cls()
    session = AsyncMock()
    exists_result = MagicMock()
    exists_result.scalar_one_or_none.return_value = uuid.uuid4()
    session.execute.return_value = exists_result

    service = SubscriptionService(session)
    await service.process_event(_make_stripe_event("customer.subscription.updated"))

    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_webhook_handles_new_subscription_events(authed_client) -> None:
    event = _make_stripe_event("customer.subscription.created")

    with (
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router._process_subscription_event"
        ) as mock_process,
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client

        resp = await authed_client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )

        assert resp.status_code == 200
        mock_process.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_subscription_endpoints(authed_client) -> None:
    SubscriptionService = _subscription_service_cls()
    org_id = str(uuid.uuid4())
    subscription = SimpleNamespace(
        id=uuid.uuid4(),
        org_id=org_id,
        billing_plan_id=uuid.uuid4(),
        billing_price_id=uuid.uuid4(),
        stripe_subscription_id="sub_123",
        stripe_customer_id="cus_123",
        status="active",
        current_period_start=SubscriptionService._to_dt(1_700_000_000),
        current_period_end=SubscriptionService._to_dt(1_700_086_400),
        cancel_at_period_end=False,
        canceled_at=None,
        trial_start=None,
        trial_end=None,
        plan={},
        price={},
    )
    history = [
        SimpleNamespace(
            id=uuid.uuid4(),
            stripe_event_id="evt_123",
            event_type="customer.subscription.updated",
            previous_status="trialing",
            new_status="active",
            processed_at=SubscriptionService._to_dt(1_700_010_000),
            payload={},
        )
    ]

    service = MagicMock()
    service.get_for_org = AsyncMock(return_value=subscription)
    service.get_history = AsyncMock(return_value=(history, 1))

    with (
        patch(
            "dev_health_ops.api.billing.subscriptions._service", return_value=service
        ),
        patch(
            "dev_health_ops.api.billing.subscriptions.get_stripe_client"
        ) as mock_stripe_client,
    ):
        mock_stripe = MagicMock()
        mock_stripe.subscriptions.retrieve.return_value = SimpleNamespace(
            items=SimpleNamespace(data=[SimpleNamespace(id="si_123")])
        )
        mock_stripe_client.return_value = mock_stripe

        sub_resp = await authed_client.get("/api/v1/billing/subscriptions")
        assert sub_resp.status_code == 200

        history_resp = await authed_client.get("/api/v1/billing/subscriptions/history")
        assert history_resp.status_code == 200
        assert history_resp.json()["total"] == 1

        change_resp = await authed_client.post(
            "/api/v1/billing/subscriptions/change-plan",
            json={"price_id": "price_new"},
        )
        assert change_resp.status_code == 200

        cancel_resp = await authed_client.post(
            "/api/v1/billing/subscriptions/cancel",
            json={"immediately": False},
        )
        assert cancel_resp.status_code == 200

        reactivate_resp = await authed_client.post(
            "/api/v1/billing/subscriptions/reactivate"
        )
        assert reactivate_resp.status_code == 200
