from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing.router import router as billing_router
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency
from dev_health_ops.models.git import Base


def _build_user(
    *, superuser: bool, org_id: str, role: str = "owner"
) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=org_id,
        role=role,
        is_superuser=superuser,
    )


@pytest_asyncio.fixture
async def app_and_sessionmaker():
    app = FastAPI()
    app.include_router(billing_router)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async def _override_db():
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[postgres_session_dependency] = _override_db
    yield app, session_factory

    app.dependency_overrides.clear()
    await engine.dispose()


@pytest_asyncio.fixture
async def client(app_and_sessionmaker):
    app, _ = app_and_sessionmaker
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True)
def _override_invoice_session(monkeypatch):
    from dev_health_ops.api.billing import invoice_routes

    @asynccontextmanager
    async def _fake_postgres_session():
        yield object()

    monkeypatch.setattr(invoice_routes, "get_postgres_session", _fake_postgres_session)


@pytest.mark.asyncio
async def test_superadmin_invoices_list_without_org_id_uses_cross_org(
    client, app_and_sessionmaker, monkeypatch
):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=True,
        org_id="",
    )

    from dev_health_ops.api.billing import invoice_routes

    captured: dict[str, object] = {}

    async def _fake_list_invoices(**kwargs):
        captured.update(kwargs)
        return [], 0

    monkeypatch.setattr(
        invoice_routes.invoice_service, "list_invoices", _fake_list_invoices
    )

    response = await client.get("/api/v1/billing/invoices")

    assert response.status_code == 200
    assert response.json()["items"] == []
    assert captured["org_id"] is None


@pytest.mark.asyncio
async def test_superadmin_invoices_list_with_org_id_query(
    client, app_and_sessionmaker, monkeypatch
):
    app, _ = app_and_sessionmaker
    scoped_org_id = uuid.uuid4()
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=True,
        org_id="",
    )

    from dev_health_ops.api.billing import invoice_routes

    captured: dict[str, object] = {}

    async def _fake_list_invoices(**kwargs):
        captured.update(kwargs)
        return [], 0

    monkeypatch.setattr(
        invoice_routes.invoice_service, "list_invoices", _fake_list_invoices
    )

    response = await client.get(f"/api/v1/billing/invoices?org_id={scoped_org_id}")

    assert response.status_code == 200
    assert captured["org_id"] == scoped_org_id


@pytest.mark.asyncio
async def test_non_superuser_without_org_id_gets_organization_error(
    client, app_and_sessionmaker
):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=False,
        org_id="",
        role="member",
    )

    response = await client.get("/api/v1/billing/invoices")

    assert response.status_code == 400
    assert response.json()["detail"] == "Organization context required"


@pytest.mark.asyncio
async def test_superadmin_subscription_endpoints_allow_cross_org_listing(
    client, app_and_sessionmaker, monkeypatch
):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=True,
        org_id="",
    )

    from dev_health_ops.api.billing import subscriptions

    captured: dict[str, object] = {"orgs": []}

    class _FakeService:
        async def get_for_org(self, org_id):
            captured["orgs"].append(org_id)
            now = datetime.now(timezone.utc)
            return SimpleNamespace(
                id=uuid.uuid4(),
                org_id=uuid.uuid4(),
                stripe_subscription_id="sub_123",
                stripe_customer_id="cus_123",
                status="active",
                current_period_start=now,
                current_period_end=now,
                cancel_at_period_end=False,
                canceled_at=None,
                trial_start=None,
                trial_end=None,
            )

        async def get_history(self, org_id, limit, offset):
            captured["orgs"].append(org_id)
            return [], 0

    async def _fake_load_plan_price(_sub, _db):
        return None, None

    monkeypatch.setattr(subscriptions, "_service", lambda _session: _FakeService())
    monkeypatch.setattr(subscriptions, "_load_plan_price", _fake_load_plan_price)

    sub_response = await client.get("/api/v1/billing/subscriptions")
    history_response = await client.get("/api/v1/billing/subscriptions/history")

    assert sub_response.status_code == 200
    assert history_response.status_code == 200
    assert captured["orgs"] == [None, None]


@pytest.mark.asyncio
async def test_superadmin_subscription_mutations_require_org_id(
    client, app_and_sessionmaker
):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=True,
        org_id="",
    )

    change_response = await client.post(
        "/api/v1/billing/subscriptions/change-plan",
        json={"price_id": "price_123"},
    )
    cancel_response = await client.post(
        "/api/v1/billing/subscriptions/cancel",
        json={"immediately": False},
    )
    reactivate_response = await client.post("/api/v1/billing/subscriptions/reactivate")

    assert change_response.status_code == 400
    assert cancel_response.status_code == 400
    assert reactivate_response.status_code == 400
    assert change_response.json()["detail"] == "org_id required"


@pytest.mark.asyncio
async def test_superadmin_refund_endpoints_allow_cross_org_listing(
    client, app_and_sessionmaker, monkeypatch
):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=True,
        org_id="",
    )

    from dev_health_ops.api.billing import refund_routes

    captured: dict[str, object] = {"orgs": []}

    @asynccontextmanager
    async def _fake_session():
        yield object()

    async def _fake_list_refunds(db, org_id, limit, offset):
        captured["orgs"].append(org_id)
        return [], 0

    async def _fake_get_refund(db, refund_id, org_id):
        captured["orgs"].append(org_id)
        return None

    monkeypatch.setattr(refund_routes, "get_postgres_session", _fake_session)
    monkeypatch.setattr(
        refund_routes.refund_service, "list_refunds", _fake_list_refunds
    )
    monkeypatch.setattr(refund_routes.refund_service, "get_refund", _fake_get_refund)

    list_response = await client.get("/api/v1/billing/refunds")
    get_response = await client.get(f"/api/v1/billing/refunds/{uuid.uuid4()}")

    assert list_response.status_code == 200
    assert get_response.status_code == 404
    assert captured["orgs"] == [None, None]


@pytest.mark.asyncio
async def test_superadmin_create_refund_requires_org_id(
    client, app_and_sessionmaker, monkeypatch
):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(
        superuser=True,
        org_id="",
    )

    from dev_health_ops.api.billing import refund_routes

    @asynccontextmanager
    async def _fake_session():
        yield object()

    monkeypatch.setattr(refund_routes, "get_postgres_session", _fake_session)

    response = await client.post(
        "/api/v1/billing/refunds",
        json={
            "invoice_id": str(uuid.uuid4()),
            "amount": 100,
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "org_id required"
