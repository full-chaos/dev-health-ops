from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Column, Table
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing import invoice_routes
from dev_health_ops.api.billing import router as billing_router
from dev_health_ops.api.billing.invoice_service import InvoiceService
from dev_health_ops.models import Base, Organization
from dev_health_ops.models.git import GUID
from dev_health_ops.models.invoices import Invoice


def _make_stripe_event(event_type: str, data_object: dict, event_id: str = "evt_test"):
    obj = SimpleNamespace(**data_object)
    return SimpleNamespace(
        id=event_id,
        type=event_type,
        data=SimpleNamespace(object=obj),
    )


@pytest_asyncio.fixture
async def db_session(tmp_path):
    db_path = tmp_path / "invoice_test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    if "subscriptions" not in Base.metadata.tables:
        Table("subscriptions", Base.metadata, Column("id", GUID(), primary_key=True))

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        yield session

    await engine.dispose()


@pytest_asyncio.fixture
async def seeded_org(db_session: AsyncSession) -> uuid.UUID:
    org_id = uuid.uuid4()
    db_session.add(Organization(id=org_id, slug="acme", name="Acme"))
    await db_session.commit()
    return org_id


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(billing_router)
    return app


@pytest_asyncio.fixture
async def api_client(db_session: AsyncSession, seeded_org: uuid.UUID):
    app = _build_app()

    async def _session_override():
        yield db_session

    from dev_health_ops.api.services.auth import AuthenticatedUser

    app.dependency_overrides[invoice_routes.get_session] = _session_override
    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser(
        user_id="user-1",
        email="test@example.com",
        org_id=str(seeded_org),
        role="admin",
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_invoice_service_upsert_and_list(
    db_session: AsyncSession, seeded_org: uuid.UUID
):
    service = InvoiceService()
    stripe_invoice = SimpleNamespace(
        id="in_123",
        customer="cus_123",
        status="open",
        amount_due=5000,
        amount_paid=0,
        amount_remaining=5000,
        currency="usd",
        period_start=1700000000,
        period_end=1700003600,
        hosted_invoice_url="https://stripe.test/in_123",
        invoice_pdf="https://stripe.test/in_123.pdf",
        payment_intent=None,
        status_transitions=SimpleNamespace(),
        attempt_count=1,
        metadata={"org_id": str(seeded_org)},
        subscription=None,
        lines=SimpleNamespace(data=[]),
    )

    invoice = await service.upsert_invoice(db_session, stripe_invoice)
    await service.upsert_line_items(db_session, invoice.id, stripe_invoice.lines)
    await db_session.commit()

    items, total = await service.list_invoices(
        db=db_session,
        org_id=seeded_org,
        limit=10,
        offset=0,
        status_filter="open",
    )

    assert total == 1
    assert len(items) == 1
    assert items[0].stripe_invoice_id == "in_123"


@pytest.mark.asyncio
async def test_webhook_invoice_event_is_idempotent(api_client):
    event = _make_stripe_event(
        "invoice.updated",
        {
            "id": "in_dup",
            "customer": "cus_1",
            "metadata": {"org_id": str(uuid.uuid4())},
            "lines": SimpleNamespace(data=[]),
        },
        event_id="evt_dup_1",
    )

    with (
        patch("dev_health_ops.api.billing.router.get_stripe_client") as mock_client_fn,
        patch(
            "dev_health_ops.api.billing.router.get_webhook_secret",
            return_value="whsec_test",
        ),
        patch(
            "dev_health_ops.api.billing.router.invoice_service"
        ) as mock_invoice_service,
        patch(
            "dev_health_ops.api.billing.router.get_postgres_session"
        ) as mock_get_session,
    ):
        mock_client = MagicMock()
        mock_client.construct_event.return_value = event
        mock_client_fn.return_value = mock_client

        mock_invoice_service.is_duplicate_event = AsyncMock(return_value=True)
        mock_invoice_service.upsert_invoice = AsyncMock()
        mock_db = AsyncMock()

        @asynccontextmanager
        async def _ctx():
            yield mock_db

        mock_get_session.return_value = _ctx()

        response = await api_client.post(
            "/api/v1/billing/webhooks/stripe",
            content=b"{}",
            headers={"stripe-signature": "sig"},
        )

    assert response.status_code == 200
    mock_invoice_service.upsert_invoice.assert_not_awaited()


@pytest.mark.asyncio
async def test_invoice_list_and_detail_endpoints(
    api_client: AsyncClient,
    db_session: AsyncSession,
    seeded_org: uuid.UUID,
):
    invoice = Invoice(
        org_id=seeded_org,
        stripe_invoice_id="in_list_1",
        stripe_customer_id="cus_list_1",
        status="open",
        amount_due=1200,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(invoice)
    await db_session.commit()

    list_resp = await api_client.get("/api/v1/billing/invoices")
    assert list_resp.status_code == 200
    assert list_resp.json()["total"] == 1

    detail_resp = await api_client.get(f"/api/v1/billing/invoices/{invoice.id}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["stripe_invoice_id"] == "in_list_1"


@pytest.mark.asyncio
async def test_void_invoice_endpoint(
    api_client: AsyncClient, db_session: AsyncSession, seeded_org: uuid.UUID
):
    invoice = Invoice(
        org_id=seeded_org,
        stripe_invoice_id="in_void_1",
        stripe_customer_id="cus_void_1",
        status="open",
        amount_due=2200,
        amount_remaining=2200,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(invoice)
    await db_session.commit()

    with patch(
        "dev_health_ops.api.billing.invoice_routes.get_stripe_client"
    ) as mock_stripe:
        mock_client = MagicMock()
        mock_client.invoices.void_invoice.return_value = {"id": "in_void_1"}
        mock_stripe.return_value = mock_client

        response = await api_client.post(f"/api/v1/billing/invoices/{invoice.id}/void")

    assert response.status_code == 200
    assert response.json()["status"] == "void"


@pytest.mark.asyncio
async def test_void_paid_invoice_rejected(
    api_client: AsyncClient,
    db_session: AsyncSession,
    seeded_org: uuid.UUID,
):
    invoice = Invoice(
        org_id=seeded_org,
        stripe_invoice_id="in_paid_1",
        stripe_customer_id="cus_paid_1",
        status="paid",
        amount_due=3300,
        amount_paid=3300,
        amount_remaining=0,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(invoice)
    await db_session.commit()

    response = await api_client.post(f"/api/v1/billing/invoices/{invoice.id}/void")
    assert response.status_code == 400
