from __future__ import annotations

import importlib
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

FEATURE_KEY = "canonical_incident_ingestion"

_admin = importlib.import_module("dev_health_ops.api.admin")
_auth = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    FeatureFlag,
    OrgFeatureOverride,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
    Setting,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
    SyncWatermark,
    BackfillJob,
)


@dataclass(frozen=True, slots=True)
class ApiIdentity:
    org_id: uuid.UUID
    user_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class ApiState:
    session_maker: async_sessionmaker[AsyncSession]
    enabled: ApiIdentity
    disabled: ApiIdentity
    feature_id: uuid.UUID


@asynccontextmanager
async def canonical_api_state_context(tmp_path: Path) -> AsyncIterator[ApiState]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'canonical-sync-gate.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection,
                tables=_TABLES,
            )
        )
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    enabled = ApiIdentity(org_id=uuid.uuid4(), user_id=uuid.uuid4())
    disabled = ApiIdentity(org_id=uuid.uuid4(), user_id=uuid.uuid4())
    feature = FeatureFlag(
        key=FEATURE_KEY,
        name="Canonical Incident Ingestion",
        category="integrations",
        min_tier="community",
        is_enabled=True,
    )
    async with session_maker() as session:
        session.add_all(
            [
                Organization(
                    id=enabled.org_id,
                    slug="canonical-enabled",
                    name="Canonical Enabled",
                    tier="enterprise",
                ),
                Organization(
                    id=disabled.org_id,
                    slug="canonical-disabled",
                    name="Canonical Disabled",
                    tier="enterprise",
                ),
                User(id=enabled.user_id, email="enabled@example.com", is_active=True),
                User(
                    id=disabled.user_id,
                    email="disabled@example.com",
                    is_active=True,
                ),
                feature,
            ]
        )
        await session.flush()
        session.add(
            OrgFeatureOverride(
                org_id=enabled.org_id,
                feature_id=feature.id,
                is_enabled=True,
            )
        )
        await session.commit()
    try:
        yield ApiState(
            session_maker=session_maker,
            enabled=enabled,
            disabled=disabled,
            feature_id=feature.id,
        )
    finally:
        await engine.dispose()


@asynccontextmanager
async def api_client(
    state: ApiState,
    identity: ApiIdentity,
) -> AsyncIterator[AsyncClient]:
    app = FastAPI()
    app.include_router(_admin.router)
    user = AuthenticatedUser(
        user_id=str(identity.user_id),
        email="admin@example.com",
        org_id=str(identity.org_id),
        role="owner",
        is_superuser=False,
    )

    async def session_override() -> AsyncIterator[AsyncSession]:
        async with state.session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[_auth.get_current_user] = lambda: user
    app.dependency_overrides[_admin.get_session] = session_override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()


async def seed_operational_config(state: ApiState, identity: ApiIdentity) -> uuid.UUID:
    async with state.session_maker() as session:
        integration = Integration(
            org_id=str(identity.org_id),
            provider="pagerduty",
            name="PagerDuty",
            config={},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        source = IntegrationSource(
            org_id=str(identity.org_id),
            integration_id=integration.id,
            provider="pagerduty",
            source_type="account",
            external_id="acme",
            name="acme",
            full_name="acme",
            metadata_={},
            is_enabled=True,
        )
        dataset = IntegrationDataset(
            org_id=str(identity.org_id),
            integration_id=integration.id,
            dataset_key="incidents",
            is_enabled=True,
            options={},
        )
        config = SyncConfiguration(
            org_id=str(identity.org_id),
            name="PagerDuty incidents",
            provider="pagerduty",
            sync_targets=["operational"],
            sync_options={},
            is_active=True,
            integration_id=integration.id,
        )
        session.add_all([source, dataset, config])
        await session.commit()
        return config.id


async def seed_repository_config(state: ApiState, identity: ApiIdentity) -> uuid.UUID:
    async with state.session_maker() as session:
        integration = Integration(
            org_id=str(identity.org_id),
            provider="github",
            name="GitHub",
            config={},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        config = SyncConfiguration(
            org_id=str(identity.org_id),
            name="GitHub incident repositories",
            provider="github",
            sync_targets=["operational"],
            sync_options={"owner": "acme"},
            is_active=True,
            integration_id=integration.id,
        )
        session.add(config)
        await session.flush()
        sources = [
            IntegrationSource(
                org_id=str(identity.org_id),
                integration_id=integration.id,
                provider="github",
                source_type="repository",
                external_id=f"acme/{name}",
                name=name,
                full_name=f"acme/{name}",
                metadata_={"planner_managed_sync_config_id": str(config.id)},
                is_enabled=True,
            )
            for name in ("alpha", "beta")
        ]
        session.add_all(
            [
                *sources,
                IntegrationDataset(
                    org_id=str(identity.org_id),
                    integration_id=integration.id,
                    dataset_key="incidents",
                    is_enabled=True,
                    options={},
                ),
            ]
        )
        await session.commit()
        return config.id
