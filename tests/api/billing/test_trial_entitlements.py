from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import cast

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.licensing import gating
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.subscriptions import Subscription
from dev_health_ops.models.users import Organization


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "trial-entitlements.db"
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
                    OrgLicense.__table__,
                    Subscription.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(session: AsyncSession, *, tier: str, slug: str) -> Organization:
    org = Organization(id=uuid.uuid4(), slug=slug, name=slug.title(), tier=tier)
    session.add(org)
    await session.commit()
    return org


async def _seed_trial_subscription(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    trial_end: datetime,
) -> None:
    sub = Subscription(
        org_id=org_id,
        billing_plan_id=uuid.uuid4(),
        billing_price_id=uuid.uuid4(),
        stripe_subscription_id=f"sub-{org_id}",
        stripe_customer_id=f"cus-{org_id}",
        status="trialing",
        current_period_start=datetime.now(timezone.utc),
        current_period_end=trial_end,
        trial_start=datetime.now(timezone.utc),
        trial_end=trial_end,
    )
    session.add(sub)
    await session.commit()


async def _get_entitlements(org_id: uuid.UUID, session: AsyncSession) -> dict:
    resolver = getattr(gating, "get_org_entitlements_from_db")
    return await resolver(org_id=org_id, session=session)


@pytest.mark.asyncio
async def test_trialing_org_gets_team_entitlements(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session, tier="team", slug="trial-team")
        await _seed_trial_subscription(
            session,
            org_id=cast(uuid.UUID, org.id),
            trial_end=datetime(2030, 1, 1, tzinfo=timezone.utc),
        )

        entitlements = await _get_entitlements(cast(uuid.UUID, org.id), session)

    assert entitlements["tier"] == "team"
    assert entitlements["features"]["team_dashboard"] is True
    assert entitlements["limits"]["users"] == 20


@pytest.mark.asyncio
async def test_community_org_gets_community_entitlements(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session, tier="community", slug="community-org")
        entitlements = await _get_entitlements(cast(uuid.UUID, org.id), session)

    assert entitlements["tier"] == "community"
    assert entitlements["features"]["team_dashboard"] is False
    assert entitlements["limits"]["users"] == 5


@pytest.mark.asyncio
async def test_entitlements_include_is_trialing_true(session_maker):
    async with session_maker() as session:
        org = await _seed_org(session, tier="team", slug="trialing-flag")
        await _seed_trial_subscription(
            session,
            org_id=cast(uuid.UUID, org.id),
            trial_end=datetime(2030, 1, 1, tzinfo=timezone.utc),
        )

        entitlements = await _get_entitlements(cast(uuid.UUID, org.id), session)

    assert entitlements["is_trialing"] is True


@pytest.mark.asyncio
async def test_entitlements_include_trial_ends_at(session_maker):
    trial_end = datetime(2030, 1, 1, 0, 0, tzinfo=timezone.utc)

    async with session_maker() as session:
        org = await _seed_org(session, tier="team", slug="trial-end-date")
        await _seed_trial_subscription(
            session,
            org_id=cast(uuid.UUID, org.id),
            trial_end=trial_end,
        )

        entitlements = await _get_entitlements(cast(uuid.UUID, org.id), session)

    assert entitlements["trial_ends_at"] == trial_end.replace(tzinfo=None).isoformat()
