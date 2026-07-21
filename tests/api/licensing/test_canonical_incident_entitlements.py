from __future__ import annotations

import importlib
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.gating import get_org_entitlements_from_db
from dev_health_ops.models.billing import BillingPlan, BillingPrice
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.subscriptions import Subscription
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_FEATURE_KEY = "canonical_incident_ingestion"
_router_module = importlib.import_module("dev_health_ops.api.licensing.router")

_TABLES = tables_of(
    Organization,
    OrgLicense,
    FeatureFlag,
    OrgFeatureOverride,
    BillingPlan,
    BillingPrice,
    Subscription,
)


@pytest_asyncio.fixture
async def entitlement_store(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'canonical-entitlements.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection,
                tables=_TABLES,
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    first_org_id = uuid.uuid4()
    second_org_id = uuid.uuid4()

    async with maker() as session:
        session.add_all(
            [
                Organization(
                    id=first_org_id,
                    slug="canonical-entitlements-first",
                    name="Canonical Entitlements First",
                    tier="enterprise",
                ),
                Organization(
                    id=second_org_id,
                    slug="canonical-entitlements-second",
                    name="Canonical Entitlements Second",
                    tier="enterprise",
                ),
                FeatureFlag(
                    key=_FEATURE_KEY,
                    name="Canonical Incident Ingestion",
                    category="integrations",
                    min_tier="community",
                ),
                OrgLicense(
                    org_id=first_org_id,
                    tier="enterprise",
                    features_override={_FEATURE_KEY: True},
                ),
            ]
        )
        await session.commit()

    try:
        yield maker, first_org_id, second_org_id
    finally:
        await engine.dispose()


def _session_patcher(
    maker: async_sessionmaker[AsyncSession],
):
    @asynccontextmanager
    async def _session() -> AsyncIterator[AsyncSession]:
        async with maker() as session:
            yield session

    return _session


@pytest.mark.asyncio
async def test_async_entitlements_deny_canonical_license_override(
    entitlement_store,
) -> None:
    maker, org_id, _ = entitlement_store

    async with maker() as session:
        entitlements = await get_org_entitlements_from_db(org_id, session)

    assert entitlements["features"][_FEATURE_KEY] is False


@pytest.mark.asyncio
async def test_rest_entitlements_add_canonical_feature_without_changing_shape(
    entitlement_store,
    monkeypatch,
) -> None:
    maker, org_id, _ = entitlement_store
    app = FastAPI()
    app.include_router(_router_module.router)
    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="member@example.com",
        org_id=str(org_id),
        role="member",
        is_superuser=False,
    )
    monkeypatch.setattr(
        _router_module,
        "get_postgres_session",
        _session_patcher(maker),
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(f"/api/v1/licensing/entitlements/{org_id}")

    assert response.status_code == 200
    body = response.json()
    assert {"org_id", "tier", "features", "limits"} <= body.keys()
    assert body["features"][_FEATURE_KEY] is False


@pytest.mark.asyncio
async def test_async_entitlements_enable_only_org_with_active_override(
    entitlement_store,
) -> None:
    maker, enabled_org_id, disabled_org_id = entitlement_store
    async with maker() as session:
        feature = (
            await session.execute(
                FeatureFlag.__table__.select().where(FeatureFlag.key == _FEATURE_KEY)
            )
        ).one()
        session.add(
            OrgFeatureOverride(
                org_id=enabled_org_id,
                feature_id=feature.id,
                is_enabled=True,
            )
        )
        await session.commit()

    async with maker() as session:
        enabled = await get_org_entitlements_from_db(enabled_org_id, session)
        disabled = await get_org_entitlements_from_db(disabled_org_id, session)

    assert enabled["features"][_FEATURE_KEY] is True
    assert disabled["features"][_FEATURE_KEY] is False
