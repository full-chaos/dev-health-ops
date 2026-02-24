from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user, get_current_user_optional
from dev_health_ops.api.billing.router import router as billing_router
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency
from dev_health_ops.models.billing import BillingPlan
from dev_health_ops.models.git import Base


def _build_user(superuser: bool) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id="user-1",
        email="admin@example.com",
        org_id="org-1",
        role="owner",
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


@pytest.mark.asyncio
async def test_model_crud_with_session_factory(app_and_sessionmaker):
    _, session_factory = app_and_sessionmaker
    async with session_factory() as session:
        plan = BillingPlan(
            key="team",
            name="Team",
            tier="team",
            metadata_={"seed": True},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(plan)
        await session.flush()

        db_plan = await session.get(BillingPlan, plan.id)
        assert db_plan is not None
        assert db_plan.key == "team"

        db_plan.name = "Team Plus"
        await session.flush()
        assert db_plan.name == "Team Plus"

        await session.delete(db_plan)
        await session.flush()
        assert await session.get(BillingPlan, plan.id) is None


@pytest.mark.asyncio
async def test_public_list_and_get_plans(client):
    create_response = await client.post(
        "/api/v1/billing/plans",
        json={
            "key": "team",
            "name": "Team",
            "tier": "team",
            "prices": [{"interval": "monthly", "amount": 4900, "currency": "usd"}],
            "bundle_ids": [],
        },
    )
    assert create_response.status_code == 401


@pytest.mark.asyncio
async def test_superadmin_crud_and_regular_user_access(client, app_and_sessionmaker):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(superuser=True)
    app.dependency_overrides[get_current_user_optional] = lambda: _build_user(
        superuser=True
    )

    create_response = await client.post(
        "/api/v1/billing/plans",
        json={
            "key": "team",
            "name": "Team",
            "tier": "team",
            "display_order": 1,
            "prices": [
                {"interval": "monthly", "amount": 4900, "currency": "usd"},
                {"interval": "yearly", "amount": 47000, "currency": "usd"},
            ],
            "bundle_ids": [],
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    plan_id = created["id"]

    list_response = await client.get("/api/v1/billing/plans")
    assert list_response.status_code == 200
    assert len(list_response.json()) == 1

    update_response = await client.put(
        f"/api/v1/billing/plans/{plan_id}",
        json={"name": "Team Updated", "display_order": 3},
    )
    assert update_response.status_code == 200
    assert update_response.json()["name"] == "Team Updated"

    delete_response = await client.delete(f"/api/v1/billing/plans/{plan_id}")
    assert delete_response.status_code == 200
    assert delete_response.json()["deleted"] is True

    app.dependency_overrides[get_current_user] = lambda: _build_user(superuser=False)
    app.dependency_overrides[get_current_user_optional] = lambda: _build_user(
        superuser=False
    )

    forbidden_list_response = await client.get(
        "/api/v1/billing/plans?include_inactive=true"
    )
    assert forbidden_list_response.status_code == 403

    forbidden_update_response = await client.put(
        f"/api/v1/billing/plans/{plan_id}",
        json={"name": "Should Fail"},
    )
    assert forbidden_update_response.status_code == 403


@pytest.mark.asyncio
async def test_sync_stripe_sets_product_and_price_ids(client, app_and_sessionmaker):
    app, _ = app_and_sessionmaker
    app.dependency_overrides[get_current_user] = lambda: _build_user(superuser=True)
    app.dependency_overrides[get_current_user_optional] = lambda: _build_user(
        superuser=True
    )

    create_response = await client.post(
        "/api/v1/billing/plans",
        json={
            "key": "enterprise",
            "name": "Enterprise",
            "tier": "enterprise",
            "prices": [{"interval": "monthly", "amount": 12900, "currency": "usd"}],
            "bundle_ids": [],
        },
    )
    assert create_response.status_code == 200
    plan_id = create_response.json()["id"]

    mock_client = MagicMock()
    mock_client.products.create.return_value = SimpleNamespace(id="prod_123")
    mock_client.prices.create.return_value = SimpleNamespace(id="price_123")

    with patch(
        "dev_health_ops.api.billing.plans.get_stripe_client", return_value=mock_client
    ):
        sync_response = await client.post(
            f"/api/v1/billing/plans/{plan_id}/sync-stripe"
        )

    assert sync_response.status_code == 200
    synced = sync_response.json()
    assert synced["stripe_product_id"] == "prod_123"
    assert synced["prices"][0]["stripe_price_id"] == "price_123"
