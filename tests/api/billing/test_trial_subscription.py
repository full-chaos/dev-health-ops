from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.billing.subscription_service import SubscriptionService
from dev_health_ops.models.git import Base
from dev_health_ops.models.subscriptions import Subscription
from dev_health_ops.models.users import Organization


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "trial-subscription.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_now(dbapi_conn, _connection_record):
        dbapi_conn.create_function(
            "now",
            0,
            lambda: datetime.now(timezone.utc).isoformat(sep=" "),
        )

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    Organization.__table__,
                    Subscription.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(session: AsyncSession) -> Organization:
    org = Organization(id=uuid.uuid4(), slug="acme", name="Acme", tier="team")
    session.add(org)
    await session.commit()
    return org


def _stripe_subscription_payload() -> SimpleNamespace:
    return SimpleNamespace(
        id="sub_trial_123",
        customer="cus_trial_123",
        status="trialing",
        current_period_start=1_700_000_000,
        current_period_end=1_700_086_400,
        cancel_at_period_end=False,
        canceled_at=None,
        trial_start=1_700_000_000,
        trial_end=1_700_604_800,
        metadata={"org_id": str(uuid.uuid4())},
        items=SimpleNamespace(
            data=[SimpleNamespace(price=SimpleNamespace(id="price_team_123"))]
        ),
    )


async def _has_had_trial(org_id: uuid.UUID, session: AsyncSession) -> bool:
    from dev_health_ops.api.billing import subscription_service

    checker = getattr(subscription_service, "has_had_trial")
    return await checker(org_id, session)


@pytest.mark.asyncio
async def test_upsert_from_stripe_persists_trial_dates(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session)
        stripe_sub = _stripe_subscription_payload()
        org_id = org.id
        resolved_price_id = uuid.uuid4()
        resolved_plan_id = uuid.uuid4()

        service = SubscriptionService(session)
        service._lookup_billing_price = AsyncMock(
            return_value=SimpleNamespace(id=resolved_price_id, plan_id=resolved_plan_id)
        )
        saved = await service.upsert_from_stripe(stripe_sub=stripe_sub, org_id=org_id)

        assert saved.billing_price_id == resolved_price_id
        assert saved.billing_plan_id == resolved_plan_id
        assert saved.trial_start == datetime.fromtimestamp(
            1_700_000_000, tz=timezone.utc
        )
        assert saved.trial_end == datetime.fromtimestamp(1_700_604_800, tz=timezone.utc)


@pytest.mark.asyncio
async def test_has_had_trial_returns_true_when_trial_exists(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session)
        sub = Subscription(
            org_id=org.id,
            billing_plan_id=uuid.uuid4(),
            billing_price_id=uuid.uuid4(),
            stripe_subscription_id="sub_has_trial",
            stripe_customer_id="cus_has_trial",
            status="trialing",
            current_period_start=datetime.now(timezone.utc),
            current_period_end=datetime.now(timezone.utc),
            trial_start=datetime.now(timezone.utc),
            trial_end=datetime.now(timezone.utc),
        )
        session.add(sub)
        await session.commit()

        assert await _has_had_trial(org.id, session) is True


@pytest.mark.asyncio
async def test_has_had_trial_returns_false_when_no_trial(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session)
        sub = Subscription(
            org_id=org.id,
            billing_plan_id=uuid.uuid4(),
            billing_price_id=uuid.uuid4(),
            stripe_subscription_id="sub_no_trial",
            stripe_customer_id="cus_no_trial",
            status="active",
            current_period_start=datetime.now(timezone.utc),
            current_period_end=datetime.now(timezone.utc),
            trial_start=None,
            trial_end=None,
        )
        session.add(sub)
        await session.commit()

        assert await _has_had_trial(org.id, session) is False


@pytest.mark.asyncio
async def test_has_had_trial_returns_false_when_no_subscription(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session)

        assert await _has_had_trial(org.id, session) is False
