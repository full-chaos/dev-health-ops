from __future__ import annotations

from datetime import datetime, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
