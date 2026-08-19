from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncConfiguration,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Organization
from tests._helpers import seed_sync_dispatch_transport_routes

# Planner fixture instants must sit INSIDE the cold-start window, which is
# `now - initial_sync_depth` and is 30 days here: the fixtures leave
# Integration.config empty, and _get_tier_backfill_days_cap clamps any larger
# per-integration override back to the community cap anyway. An absolute
# instant therefore ages out of the window as the calendar advances. The
# previous pin, 2026-07-20 12:00Z, became unreachable at exactly 2026-08-19
# 12:00Z, and 14 feature-gate tests started failing mid-day with a bare
# NoResultFound on a run that planned no units -- on main as well as on any
# open branch, with no code change to blame. Anchoring to the clock keeps the
# same relationships (dispatch instant, watermark two hours earlier) while
# staying inside the window on every future run.
SYNC_FIXTURE_BEFORE = datetime.now(timezone.utc) - timedelta(days=5)
SYNC_FIXTURE_LAST_SYNC_AT = SYNC_FIXTURE_BEFORE - timedelta(hours=2)

FEATURE_KEY = "canonical_incident_ingestion"


@dataclass(frozen=True, slots=True)
class CanonicalState:
    session: Session
    enabled_org_id: uuid.UUID
    disabled_org_id: uuid.UUID
    feature_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class CanonicalGraph:
    integration: Integration
    source: IntegrationSource
    dataset: IntegrationDataset
    config: SyncConfiguration | None


@contextmanager
def canonical_state_context() -> Iterator[CanonicalState]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    seed_sync_dispatch_transport_routes(session)
    enabled_org_id = uuid.uuid4()
    disabled_org_id = uuid.uuid4()
    feature = FeatureFlag(
        key=FEATURE_KEY,
        name="Canonical Incident Ingestion",
        category="integrations",
        min_tier="community",
        is_enabled=True,
    )
    session.add_all(
        [
            Organization(
                id=enabled_org_id,
                slug="orchestration-enabled",
                name="Orchestration Enabled",
                tier="enterprise",
            ),
            Organization(
                id=disabled_org_id,
                slug="orchestration-disabled",
                name="Orchestration Disabled",
                tier="enterprise",
            ),
            feature,
        ]
    )
    session.flush()
    session.add(
        OrgFeatureOverride(
            org_id=disabled_org_id,
            feature_id=feature.id,
            is_enabled=False,
        )
    )
    session.commit()
    try:
        yield CanonicalState(
            session=session,
            enabled_org_id=enabled_org_id,
            disabled_org_id=disabled_org_id,
            feature_id=feature.id,
        )
    finally:
        session.close()
        engine.dispose()


def create_canonical_graph(
    state: CanonicalState,
    org_id: uuid.UUID,
    *,
    with_config: bool = False,
) -> CanonicalGraph:
    credential = IntegrationCredential(
        provider="pagerduty",
        name="default",
        org_id=str(org_id),
        config={"account_id": "acme", "subdomain": "acme"},
        is_active=True,
    )
    state.session.add(credential)
    state.session.flush()
    integration = Integration(
        org_id=str(org_id),
        provider="pagerduty",
        name="PagerDuty",
        config={},
        is_active=True,
        credential_id=credential.id,
    )
    state.session.add(integration)
    state.session.flush()
    source = IntegrationSource(
        org_id=str(org_id),
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
        org_id=str(org_id),
        integration_id=integration.id,
        dataset_key="incidents",
        is_enabled=True,
        options={},
    )
    config = None
    if with_config:
        config = SyncConfiguration(
            org_id=str(org_id),
            name="PagerDuty incidents",
            provider="pagerduty",
            sync_targets=["operational"],
            sync_options={
                "schedule_cron": "* * * * *",
                "timezone": "UTC",
            },
            is_active=True,
            integration_id=integration.id,
        )
        config.last_sync_at = SYNC_FIXTURE_LAST_SYNC_AT
    state.session.add_all([source, dataset, *([config] if config is not None else [])])
    state.session.commit()
    return CanonicalGraph(
        integration=integration,
        source=source,
        dataset=dataset,
        config=config,
    )


def disable_feature_for_org(
    state: CanonicalState,
    org_id: uuid.UUID,
    *,
    commit: bool = True,
) -> None:
    override = (
        state.session.query(OrgFeatureOverride)
        .filter_by(
            org_id=org_id,
            feature_id=state.feature_id,
        )
        .one_or_none()
    )
    if override is None:
        state.session.add(
            OrgFeatureOverride(
                org_id=org_id,
                feature_id=state.feature_id,
                is_enabled=False,
            )
        )
    else:
        override.is_enabled = False
        override.expires_at = None
    if commit:
        state.session.commit()
    else:
        state.session.flush()
