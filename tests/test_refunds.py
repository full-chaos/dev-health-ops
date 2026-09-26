from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing.refund_service import RefundService
from dev_health_ops.api.billing.router import router
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.refunds import Refund


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def admin_user() -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
        is_superuser=True,
    )


@pytest.fixture
def member_user(admin_user: AuthenticatedUser) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="member@example.com",
        org_id=admin_user.org_id,
        role="member",
    )


@pytest.fixture
def app(admin_user: AuthenticatedUser):
    app = _build_app()
    app.dependency_overrides[get_current_user] = lambda: admin_user
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class _DummySessionCtx:
    def __init__(self, db: object):
        self._db = db

    async def __aenter__(self):
        return self._db

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_refund(org_id: str, invoice_id: str) -> Refund:
    return Refund(
        id=uuid.uuid4(),
        org_id=uuid.UUID(org_id),
        invoice_id=uuid.UUID(invoice_id),
        subscription_id=None,
        stripe_refund_id="re_test_123",
        stripe_charge_id="ch_test_123",
        stripe_payment_intent_id="pi_test_123",
        amount=500,
        currency="usd",
        status="pending",
        reason="requested_by_customer",
        description="requested",
        failure_reason=None,
        initiated_by=None,
        metadata_={},
    )


@pytest.mark.asyncio
async def test_refund_webhook_event_delegates_to_service(client: AsyncClient):
    event = SimpleNamespace(
        type="charge.refund.updated",
        data=SimpleNamespace(object=SimpleNamespace(id="re_test_123")),
    )
    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch("dev_health_ops.api.billing.router.get_postgres_session") as mock_db,
        patch(
            "dev_health_ops.api.billing.router.refund_service.process_webhook",
            new=AsyncMock(),
        ) as mock_process,
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client
        mock_db.return_value = _DummySessionCtx(MagicMock())

        response = await client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "valid"},
        )

    assert response.status_code == 200
    mock_process.assert_awaited_once()


@pytest.mark.asyncio
async def test_service_validates_not_paid_invoice():
    service = RefundService()
    db = MagicMock()
    with (
        patch.object(
            service,
            "_get_invoice",
            new=AsyncMock(
                return_value={
                    "id": str(uuid.uuid4()),
                    "org_id": str(uuid.uuid4()),
                    "status": "open",
                    "amount_paid": 1000,
                    "stripe_charge_id": "ch_123",
                    "currency": "usd",
                    "stripe_payment_intent_id": "pi_123",
                    "subscription_id": None,
                }
            ),
        ),
        patch.object(
            service, "_get_existing_refunds_total", new=AsyncMock(return_value=0)
        ),
        pytest.raises(ValueError, match="Invoice is not paid"),
    ):
        await service.create_refund(
            db=db,
            invoice_id=uuid.uuid4(),
            amount=200,
        )


@pytest.mark.asyncio
async def test_service_validates_refund_amount_bounds():
    service = RefundService()
    db = MagicMock()
    with (
        patch.object(
            service,
            "_get_invoice",
            new=AsyncMock(
                return_value={
                    "id": str(uuid.uuid4()),
                    "org_id": str(uuid.uuid4()),
                    "status": "paid",
                    "amount_paid": 1000,
                    "stripe_charge_id": "ch_123",
                    "currency": "usd",
                    "stripe_payment_intent_id": "pi_123",
                    "subscription_id": None,
                }
            ),
        ),
        patch.object(
            service, "_get_existing_refunds_total", new=AsyncMock(return_value=900)
        ),
        pytest.raises(ValueError, match="Refund amount exceeds refundable balance"),
    ):
        await service.create_refund(
            db=db,
            invoice_id=uuid.uuid4(),
            amount=200,
        )


@pytest.mark.asyncio
async def test_service_validates_already_refunded_invoice():
    service = RefundService()
    db = MagicMock()
    with (
        patch.object(
            service,
            "_get_invoice",
            new=AsyncMock(
                return_value={
                    "id": str(uuid.uuid4()),
                    "org_id": str(uuid.uuid4()),
                    "status": "paid",
                    "amount_paid": 1000,
                    "stripe_charge_id": "ch_123",
                    "currency": "usd",
                    "stripe_payment_intent_id": "pi_123",
                    "subscription_id": None,
                }
            ),
        ),
        patch.object(
            service, "_get_existing_refunds_total", new=AsyncMock(return_value=1000)
        ),
        pytest.raises(ValueError, match="fully refunded"),
    ):
        await service.create_refund(
            db=db,
            invoice_id=uuid.uuid4(),
            amount=100,
        )
