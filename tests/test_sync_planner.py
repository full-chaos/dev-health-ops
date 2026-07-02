from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.users import Organization
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_STATUS_PENDING,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from dev_health_ops.sync.watermarks import get_watermark, set_watermark

ORG_ID = "planner-org"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _create_integration(session: Session, provider: str = "github") -> Integration:
    integration = Integration(
        org_id=ORG_ID,
        provider=provider,
        name=f"{provider.title()} integration",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _create_source(
    session: Session,
    integration: Integration,
    *,
    external_id: str,
    provider: str | None = None,
    is_enabled: bool = True,
) -> IntegrationSource:
    source_provider = provider or integration.provider
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider=source_provider,
        source_type="repo",
        external_id=external_id,
        name=external_id.rsplit("/", 1)[-1],
        full_name=external_id,
        metadata_={},
        is_enabled=is_enabled,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    session.add(source)
    session.flush()
    return source


def _create_dataset(
    session: Session,
    integration: Integration,
    dataset_key: str,
    *,
    is_enabled: bool = True,
) -> IntegrationDataset:
    dataset = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=is_enabled,
        options={},
    )
    session.add(dataset)
    session.flush()
    return dataset


def _planned_units(session: Session, plan_sync_run_id: str) -> list[SyncRunUnit]:
    return (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == plan_sync_run_id)
        .order_by(SyncRunUnit.provider, SyncRunUnit.dataset_key, SyncRunUnit.source_id)
        .all()
    )


def test_enabled_sources_and_enabled_datasets_fan_out_to_units(db_session):
    integration = _create_integration(db_session)
    sources = [
        _create_source(db_session, integration, external_id="full-chaos/dev-health"),
        _create_source(
            db_session, integration, external_id="full-chaos/dev-health-web"
        ),
    ]
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    units = _planned_units(db_session, plan.sync_run_id)
    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=plan.sync_run_id, kind=OUTBOX_KIND_DISCOVERY)
        .one()
    )
    discovery = (
        db_session.query(SyncRunReferenceDiscovery)
        .filter_by(sync_run_id=plan.sync_run_id)
        .one()
    )

    assert plan.total_units == 4
    assert len(plan.unit_ids) == 4
    assert sync_run is not None
    assert sync_run.status == SyncRunStatus.PLANNED.value
    assert sync_run.total_units == 4
    assert {(str(unit.source_id), unit.dataset_key) for unit in units} == {
        (str(source.id), dataset_key)
        for source in sources
        for dataset_key in ("commits", "prs")
    }
    assert {unit.status for unit in units} == {SyncRunUnitStatus.PLANNED.value}
    assert {unit.mode for unit in units} == {SyncRunMode.INCREMENTAL.value}
    assert outbox.status == OUTBOX_STATUS_PENDING
    assert outbox.claim_token is None
    assert discovery.status == "planned"
    assert discovery.org_id == ORG_ID


def test_planner_stamps_single_credential_per_run(db_session):
    """CHAOS-2755: plan_sync_run stamps credential_id + fingerprint + auth_source
    ONCE on the SyncRun, and neither PlannedUnit nor SyncRunUnit gains a
    credential field (credentials are auth state, never dispatch capacity)."""
    import dataclasses

    from dev_health_ops.credentials.fingerprint import AUTH_SOURCE_ENVIRONMENT
    from dev_health_ops.sync.planner import PlannedUnit

    integration = _create_integration(db_session)  # env auth (credential_id=None)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_source(db_session, integration, external_id="full-chaos/dev-health-web")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert sync_run is not None
    # Stamped exactly once, on the run.
    assert sync_run.auth_source == AUTH_SOURCE_ENVIRONMENT
    assert sync_run.credential_id is None  # environment auth
    assert isinstance(sync_run.credential_fingerprint, str)
    assert len(sync_run.credential_fingerprint) == 64  # sha256 hex digest

    # The run-level columns exist ONLY on sync_runs, never on sync_run_units.
    unit_columns = set(SyncRunUnit.__table__.columns.keys())
    assert "credential_id" not in unit_columns
    assert "credential_fingerprint" not in unit_columns
    assert "auth_source" not in unit_columns

    # PlannedUnit likewise carries no credential field.
    planned_fields = {f.name for f in dataclasses.fields(PlannedUnit)}
    assert not any("credential" in name for name in planned_fields)


def test_unsupported_provider_dataset_pairs_are_skipped(db_session):
    integration = _create_integration(db_session, provider="jira")
    _create_source(db_session, integration, external_id="jira-project", provider="jira")
    _create_dataset(db_session, integration, "commits")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert plan.total_units == 0
    assert plan.unit_ids == ()
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


def test_backfill_creates_one_unit_per_source_dataset_window(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_source(db_session, integration, external_id="full-chaos/dev-health-web")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="manual",
            since=datetime(2026, 6, 1, tzinfo=timezone.utc),
            before=datetime(2026, 6, 14, 23, 59, tzinfo=timezone.utc),
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    units = _planned_units(db_session, plan.sync_run_id)

    assert plan.total_units == 8
    assert sync_run is not None
    assert sync_run.total_units == 8
    assert len(units) == 8
    assert {unit.mode for unit in units} == {SyncRunMode.BACKFILL.value}

    windows = set()
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        windows.add((unit.since_at.date(), unit.before_at.date()))

    assert windows == {
        (datetime(2026, 6, 1).date(), datetime(2026, 6, 7).date()),
        (datetime(2026, 6, 8).date(), datetime(2026, 6, 14).date()),
    }


def test_disabled_source_produces_zero_units(db_session):
    integration = _create_integration(db_session)
    _create_source(
        db_session,
        integration,
        external_id="full-chaos/dev-health",
        is_enabled=False,
    )
    _create_dataset(db_session, integration, "commits")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert plan.total_units == 0
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


def test_disabled_dataset_produces_zero_units(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits", is_enabled=False)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert plan.total_units == 0
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert _planned_units(db_session, plan.sync_run_id) == []


def test_incremental_window_starts_at_dataset_watermark(db_session):
    integration = _create_integration(db_session)
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "prs")
    watermark = datetime(2026, 6, 10, 9, 30, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "prs", watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    unit = _planned_units(db_session, plan.sync_run_id)[0]
    assert unit.since_at is not None
    assert unit.since_at.replace(tzinfo=timezone.utc) == watermark


def test_planner_rejects_cross_org_integration(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")
    with pytest.raises(ValueError):
        plan_sync_run(
            db_session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id="someone-elses-org",
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="manual",
            ),
        )


def test_planned_units_persist_isolated_processor_flags(db_session):
    integration = _create_integration(db_session)
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "prs")
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    unit = _planned_units(db_session, plan.sync_run_id)[0]
    # prs unit must persist explicit flags and must NOT over-fetch unrelated datasets
    assert unit.processor_flags
    assert unit.processor_flags.get("sync_security", False) is False
    assert unit.processor_flags.get("sync_deployments", False) is False
    assert unit.processor_flags.get("sync_incidents", False) is False


# ---------------------------------------------------------------------------
# WS-A tests: cold-start depth + full_resync (CHAOS-2569)
# ---------------------------------------------------------------------------


def test_incremental_cold_start_uses_initial_sync_depth(db_session):
    """No watermark row → window_start == now - depth (±2s).

    Covers both a work-item dataset (prs) and a code dataset (commits).
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")
    _create_dataset(db_session, integration, "prs")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 2
    expected_start = now - timedelta(days=30)
    for unit in units:
        assert unit.since_at is not None
        since = unit.since_at.replace(tzinfo=timezone.utc)
        assert abs((since - expected_start).total_seconds()) < 2


def test_full_resync_uses_configured_depth(db_session):
    """full_resync mode -> window_start == now - depth, not None."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 14}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.FULL_RESYNC.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    unit = units[0]
    assert unit.since_at is not None, "full_resync must not produce a None window_start"
    expected_start = now - timedelta(days=14)
    since = unit.since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_dataset_option_overrides_integration_depth(db_session):
    """Dataset options.initial_sync_depth wins over integration config."""
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    dataset = _create_dataset(db_session, integration, "commits")
    dataset.options = {"initial_sync_depth": 7}
    db_session.flush()

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    expected_start = now - timedelta(days=7)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_existing_watermark_incremental_unchanged(db_session):
    """With a watermark row, since_at == watermark (regression guard)."""
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "commits")
    watermark = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "commits", watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    unit = units[0]
    assert unit.since_at is not None
    assert unit.since_at.replace(tzinfo=timezone.utc) == watermark


# ---------------------------------------------------------------------------
# Finding #1 regression: WatermarkBehavior.NONE datasets keep since_at=None
# ---------------------------------------------------------------------------


def test_none_watermark_behavior_incremental_keeps_since_at_none(db_session):
    """NONE-behavior datasets (repo-metadata only) must keep since_at=None on
    incremental — cold-start depth must NOT be applied.
    """
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    # repo-metadata is the only remaining WatermarkBehavior.NONE dataset
    _create_dataset(db_session, integration, "repo-metadata")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is None, (
        "NONE-behavior dataset (repo-metadata) must keep since_at=None on incremental, "
        "not receive a cold-start depth window"
    )


# ---------------------------------------------------------------------------
# Tier cap: non-UUID org_id, unlimited tier, and real missing-table path
# ---------------------------------------------------------------------------


def test_tier_cap_non_uuid_org_id_returns_community_default(db_session):
    """Non-UUID org_id (e.g. test fixtures) returns community default (30).

    This is a defensive guard in _get_tier_backfill_days_cap for callers that
    pass string org_ids; it does NOT represent the missing-table path.
    """
    from dev_health_ops.sync.planner import (
        _DEFAULT_INITIAL_SYNC_DEPTH_DAYS,
        _get_tier_backfill_days_cap,
    )

    cap = _get_tier_backfill_days_cap(db_session, "not-a-uuid")
    assert cap == _DEFAULT_INITIAL_SYNC_DEPTH_DAYS


def test_tier_cap_unlimited_tier_does_not_cap_depth(db_session, monkeypatch):
    """get_limit returning None (unlimited/enterprise tier) must NOT cap depth.

    An enterprise org with initial_sync_depth=90 must plan 90 days, not 30.
    """
    import uuid as _uuid

    from dev_health_ops.sync.planner import _get_tier_backfill_days_cap

    monkeypatch.setattr(
        "dev_health_ops.api.services.licensing.TierLimitService.get_limit",
        lambda self, org_id, key: None,
    )

    cap = _get_tier_backfill_days_cap(db_session, str(_uuid.uuid4()))
    assert cap is None, "Unlimited tier must return None (no cap), not a default value"


def test_tier_limit_service_returns_empty_on_missing_table():
    """TierLimitService._get_db_tier_limits returns {} when tier_limits is absent.

    When the table is missing the query raises; the service swallows it and
    falls through to hardcoded defaults. It must NOT call session.rollback()
    here — the service is invoked from async callers via run_sync and a sync
    rollback there breaks the greenlet context (MissingGreenlet). The caller's
    session must remain usable.
    """
    from dev_health_ops.api.services.licensing import TierLimitService
    from dev_health_ops.licensing.types import LicenseTier
    from dev_health_ops.models.git import Base as GitBase
    from tests._helpers import tables_of

    # Schema with NO tier_limits table
    engine = create_engine("sqlite:///:memory:")
    GitBase.metadata.create_all(engine, tables=tables_of(Integration))

    with Session(engine) as session:
        svc = TierLimitService(session)
        # Must not raise; must return {} (fall through to hardcoded defaults)
        result = svc._get_db_tier_limits(LicenseTier.COMMUNITY.value)
        assert result == {}, (
            "Missing tier_limits must return empty dict (use hardcoded defaults)"
        )
        # Session must remain usable (no rollback needed on this backend)
        session.execute(__import__("sqlalchemy").text("SELECT 1"))

    engine.dispose()


def test_plan_sync_run_succeeds_when_tier_limits_unavailable(db_session, monkeypatch):
    """plan_sync_run must succeed and flush SyncRun when tier cap lookup fails.

    Monkeypatches _get_db_tier_limits to raise OperationalError (same shape as
    a missing tier_limits table) so we can verify the planner session stays
    usable end-to-end without needing to replicate the full licensing schema.
    Asserts (a) SyncRun flushes (session NOT poisoned) and (b) depth falls
    back to the community hardcoded default (30).
    """
    from datetime import timedelta

    from sqlalchemy.exc import OperationalError

    def _raise_op_error(self, tier):
        raise OperationalError("no such table: tier_limits", None, Exception())

    monkeypatch.setattr(
        "dev_health_ops.api.services.licensing.TierLimitService._get_db_tier_limits",
        _raise_op_error,
    )

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 90}
    db_session.flush()
    _create_source(db_session, integration, external_id="owner/repo")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="test",
        ),
    )

    # (a) SyncRun flushed — session NOT poisoned
    run = db_session.get(SyncRun, plan.sync_run_id)
    assert run is not None, "SyncRun must flush even when tier_limits is unavailable"
    assert run.status == SyncRunStatus.PLANNED.value

    # (b) Depth falls back to community hardcoded default (30), not 90
    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    expected_start = now - timedelta(days=30)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2, (
        f"Expected depth=30 (community default), got since_at={since}"
    )


@pytest.mark.skipif(
    not os.getenv("DEV_HEALTH_POSTGRES_TEST_URI"),
    reason="requires DEV_HEALTH_POSTGRES_TEST_URI",
)
def test_postgres_missing_tier_limits_stays_inside_planner_savepoint():
    """CHAOS-2580: a pre-migration Postgres tier_limits miss must not poison planning.

    PostgreSQL marks the whole transaction failed after a missing-table error.
    The planner owns a SAVEPOINT around tier-limit resolution so the swallowed
    TierLimitService fallback does not prevent later SyncRun/SyncRunUnit/outbox
    flushes in the same transaction.
    """
    from tests._helpers import tables_of

    uri = os.environ["DEV_HEALTH_POSTGRES_TEST_URI"]
    schema = f"chaos_2580_{uuid.uuid4().hex}"
    engine = create_engine(uri)
    connection = engine.connect()
    try:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(text(f'SET search_path TO "{schema}"'))
        connection.commit()
        Base.metadata.create_all(
            connection,
            tables=tables_of(
                Organization,
                OrgLicense,
                Integration,
                IntegrationSource,
                IntegrationDataset,
                SyncRun,
                SyncRunReferenceDiscovery,
                SyncRunUnit,
                SyncDispatchOutbox,
                SyncWatermark,
            ),
        )
        with Session(bind=connection) as session:
            org_id = uuid.uuid4()
            session.add(
                Organization(
                    id=org_id,
                    slug=f"chaos-2580-{org_id.hex[:8]}",
                    name="CHAOS 2580",
                    tier="community",
                )
            )
            session.flush()
            integration = Integration(
                org_id=str(org_id),
                provider="github",
                name="Github integration",
                config={"initial_sync_depth": 90},
                is_active=True,
            )
            session.add(integration)
            session.flush()
            source = IntegrationSource(
                org_id=str(org_id),
                integration_id=integration.id,
                provider="github",
                source_type="repo",
                external_id="owner/repo",
                name="repo",
                full_name="owner/repo",
                metadata_={},
                is_enabled=True,
                discovered_at=datetime.now(timezone.utc),
                last_seen_at=datetime.now(timezone.utc),
            )
            dataset = IntegrationDataset(
                org_id=str(org_id),
                integration_id=integration.id,
                dataset_key="commits",
                is_enabled=True,
                options={},
            )
            session.add_all([source, dataset])
            session.flush()

            plan = plan_sync_run(
                session,
                SyncPlanRequest(
                    integration_id=str(integration.id),
                    org_id=str(org_id),
                    mode=SyncRunMode.INCREMENTAL.value,
                    triggered_by="test",
                ),
            )
            session.flush()

            run = session.get(SyncRun, plan.sync_run_id)
            units = _planned_units(session, plan.sync_run_id)
            outbox = (
                session.query(SyncDispatchOutbox)
                .filter_by(sync_run_id=plan.sync_run_id, kind=OUTBOX_KIND_DISCOVERY)
                .one()
            )
            assert run is not None
            assert run.status == SyncRunStatus.PLANNED.value
            assert len(units) == 1
            assert outbox.status == OUTBOX_STATUS_PENDING
            session.execute(text("SELECT 1"))
    finally:
        connection.rollback()
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        connection.commit()
        connection.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# CHAOS-2570: backfill -> incremental composition (no date gap)
# ---------------------------------------------------------------------------


def test_backfill_then_incremental_has_no_date_gap(db_session):
    """Canonical onboarding flow: a backfill (which never seeds a watermark per
    CHAOS-2514) followed by an incremental must leave NO date gap.

    With no watermark, the incremental cold-starts at ``now - initial_sync_depth``
    (CHAOS-2569), which reaches back past a backfill whose ``before`` is ~now, so
    coverage is continuous across the seam.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    backfill_before = now  # canonical onboarding: backfill up to ~now

    # 1) Backfill plan: units are mode=backfill and NO watermark is seeded.
    backfill_plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="backfill",
            since=now - timedelta(days=14),
            before=backfill_before,
        ),
    )
    backfill_units = _planned_units(db_session, backfill_plan.sync_run_id)
    assert {u.mode for u in backfill_units} == {SyncRunMode.BACKFILL.value}
    assert get_watermark(db_session, ORG_ID, source.external_id, "commits") is None, (
        "backfill must not seed a watermark (CHAOS-2514)"
    )

    # 2) First incremental cold-starts; window_start <= backfill `before` => no gap.
    inc_plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    inc_units = _planned_units(db_session, inc_plan.sync_run_id)
    assert len(inc_units) == 1
    assert inc_units[0].since_at is not None
    since = inc_units[0].since_at.replace(tzinfo=timezone.utc)
    assert since <= backfill_before, (
        "incremental cold-start must reach back to the backfill's `before` "
        "so there is no date gap"
    )


def test_incremental_cold_start_seam_is_depth_bounded(db_session):
    """The no-gap guarantee is depth-bounded: the first incremental cold-start
    window_start is exactly ``now - initial_sync_depth``, so a backfill whose
    ``before`` is at/after that boundary is seamlessly covered. A backfill whose
    ``before`` is OLDER than the boundary is the documented residual edge
    (paused-then-resumed) and is intentionally out of scope here.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    units = _planned_units(db_session, plan.sync_run_id)
    assert units[0].since_at is not None
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    boundary = now - timedelta(days=30)
    assert abs((since - boundary).total_seconds()) < 5
    # A backfill ending at/after the boundary is covered (no gap).
    assert since <= boundary + timedelta(seconds=5)


def test_cold_start_does_not_cover_backfill_before_older_than_depth(db_session):
    """CHAOS-2588 residual (documented boundary): a backfill whose ``before`` is
    OLDER than ``now - initial_sync_depth`` is NOT covered by the incremental
    cold-start window, so a gap ``[before, now - depth]`` remains. This proves the
    no-gap guarantee is BOUNDED (depth-driven, marker-less), not universal -- it
    is the accepted, tracked limit handed off to CHAOS-2588.
    """
    from datetime import timedelta

    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")

    now = datetime.now(timezone.utc)
    backfill_before = now - timedelta(days=90)  # older than depth (30d)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    units = _planned_units(db_session, plan.sync_run_id)
    assert units[0].since_at is not None
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    # Cold-start starts at now-depth, which is AFTER the old backfill `before`,
    # so the residual gap [backfill_before, since] is non-empty.
    assert since > backfill_before


def test_github_work_items_unit_carries_prs_signal_when_prs_enabled(db_session):
    """CHAOS-646: when the PRS dataset is enabled, the planner stamps
    ``sync_prs=True`` on the github work-items unit so the adapter threads
    ``include_pull_requests=True`` into the work-items sync."""
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "work-items")
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    work_items_units = [u for u in units if u.dataset_key == "work-items"]
    assert work_items_units
    for unit in work_items_units:
        assert (unit.processor_flags or {}).get("sync_prs") is True


def test_github_work_items_unit_omits_prs_signal_when_prs_disabled(db_session):
    """CHAOS-646 regression: with the PRS dataset off, the work-items unit must
    carry ``sync_prs=False`` so PRs are NOT ingested as work items."""
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "work-items")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    work_items_units = [u for u in units if u.dataset_key == "work-items"]
    assert work_items_units
    for unit in work_items_units:
        assert (unit.processor_flags or {}).get("sync_prs") is False


# ---------------------------------------------------------------------------
# CHAOS-2707: work-item-labels / work-item-projects are now INCREMENTAL
# ---------------------------------------------------------------------------


def test_work_item_labels_incremental_cold_start_uses_depth(db_session):
    """work-item-labels has WatermarkBehavior.INCREMENTAL (CHAOS-2707).

    With no saved watermark, since_at must equal now - initial_sync_depth,
    not None.
    """
    from datetime import timedelta

    integration = _create_integration(db_session, provider="github")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    _create_dataset(db_session, integration, "work-item-labels")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None, (
        "work-item-labels must use cold-start depth (INCREMENTAL), not since_at=None"
    )
    expected_start = now - timedelta(days=30)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_work_item_projects_incremental_cold_start_uses_depth(db_session):
    """work-item-projects has WatermarkBehavior.INCREMENTAL (CHAOS-2707).

    With no saved watermark, since_at must equal now - initial_sync_depth,
    not None.
    """
    from datetime import timedelta

    integration = _create_integration(db_session, provider="jira")
    integration.config = {"initial_sync_depth": 14}
    db_session.flush()
    _create_source(db_session, integration, external_id="jira-project", provider="jira")
    _create_dataset(db_session, integration, "work-item-projects")

    now = datetime.now(timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None, (
        "work-item-projects must use cold-start depth (INCREMENTAL), not since_at=None"
    )
    expected_start = now - timedelta(days=14)
    since = units[0].since_at.replace(tzinfo=timezone.utc)
    assert abs((since - expected_start).total_seconds()) < 2


def test_work_item_labels_incremental_uses_saved_watermark(db_session):
    """work-item-labels: when a watermark exists, since_at == watermark (CHAOS-2707).

    Proves the saved watermark is honoured, not overridden by cold-start depth.
    """
    integration = _create_integration(db_session, provider="github")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="full-chaos/dev-health"
    )
    _create_dataset(db_session, integration, "work-item-labels")
    watermark = datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "work-item-labels", watermark)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    assert units[0].since_at.replace(tzinfo=timezone.utc) == watermark


def test_work_item_projects_incremental_uses_saved_watermark(db_session):
    """work-item-projects: when a watermark exists, since_at == watermark (CHAOS-2707).

    Proves the saved watermark is honoured, not overridden by cold-start depth.
    """
    integration = _create_integration(db_session, provider="jira")
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    source = _create_source(
        db_session, integration, external_id="jira-project", provider="jira"
    )
    _create_dataset(db_session, integration, "work-item-projects")
    watermark = datetime(2026, 6, 10, 0, 0, tzinfo=timezone.utc)
    set_watermark(
        db_session, ORG_ID, source.external_id, "work-item-projects", watermark
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    assert units[0].since_at.replace(tzinfo=timezone.utc) == watermark


def test_repo_metadata_still_none_watermark_behavior(db_session):
    """repo-metadata remains WatermarkBehavior.NONE after CHAOS-2707.

    Regression guard: removing work-item-labels/projects from _NO_WATERMARK_DATASETS
    must not accidentally change repo-metadata.
    """
    from dev_health_ops.sync.datasets import WatermarkBehavior, get_dataset_spec

    spec = get_dataset_spec("github", "repo-metadata")
    assert spec is not None
    assert spec.watermark_behavior == WatermarkBehavior.NONE


def test_work_item_labels_and_projects_are_incremental_behavior(db_session):
    """Registry-level assertion: both datasets now carry WatermarkBehavior.INCREMENTAL.

    Covers all providers that support these datasets.
    """
    from dev_health_ops.sync.datasets import WatermarkBehavior, get_dataset_spec

    for provider in ("github", "gitlab", "jira", "linear"):
        for dataset_key in ("work-item-labels", "work-item-projects"):
            spec = get_dataset_spec(provider, dataset_key)
            assert spec is not None, f"{provider}/{dataset_key} not in registry"
            assert spec.watermark_behavior == WatermarkBehavior.INCREMENTAL, (
                f"{provider}/{dataset_key} must be INCREMENTAL after CHAOS-2707, "
                f"got {spec.watermark_behavior}"
            )


# ---------------------------------------------------------------------------
# CHAOS-2710: Linear backfill chunk policy
# ---------------------------------------------------------------------------


def test_linear_work_item_backfill_produces_bounded_windows(db_session, monkeypatch):
    """Large Linear work-item backfill is split into windows <= LINEAR_BACKFILL_MAX_WINDOW_DAYS.

    A 90-day range with the default 14-day max must produce 7 chunks, each at most
    14 days wide. Non-Linear providers with the same range keep the 7-day default.
    """
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)

    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    _create_dataset(db_session, integration, "work-items")

    since = datetime(2026, 3, 2, tzinfo=timezone.utc)
    before = datetime(2026, 5, 30, 23, 59, 59, tzinfo=timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 7, f"expected 7 fortnightly chunks, got {len(units)}"
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        window_days = (unit.before_at.date() - unit.since_at.date()).days + 1
        assert window_days <= 14, (
            f"Linear work-item backfill window too wide: {window_days} days"
            f" (since={unit.since_at.date()}, before={unit.before_at.date()})"
        )
    assert {unit.mode for unit in units} == {SyncRunMode.BACKFILL.value}


def test_linear_work_item_backfill_env_override_respected(db_session, monkeypatch):
    """LINEAR_BACKFILL_MAX_WINDOW_DAYS env override is applied to Linear work-item chunks."""
    monkeypatch.setenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", "5")

    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-2", provider="linear"
    )
    _create_dataset(db_session, integration, "work-item-history")

    since = datetime(2026, 5, 1, tzinfo=timezone.utc)
    before = datetime(2026, 5, 20, 23, 59, 59, tzinfo=timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) > 0
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        window_days = (unit.before_at.date() - unit.since_at.date()).days + 1
        assert window_days <= 5, (
            f"Linear work-item-history window too wide with env=5: {window_days} days"
        )


def test_non_linear_backfill_keeps_seven_day_chunks(db_session, monkeypatch):
    """Non-Linear providers are unaffected by the Linear chunk policy.

    A 14-day github backfill must still produce 2 chunks of 7 days each.
    """
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)

    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="owner/repo", provider="github")
    _create_dataset(db_session, integration, "work-items")

    since = datetime(2026, 6, 1, tzinfo=timezone.utc)
    before = datetime(2026, 6, 14, 23, 59, 59, tzinfo=timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 2, (
        f"Expected 2 chunks for 14-day github backfill, got {len(units)}"
    )
    windows = set()
    for u in units:
        assert u.since_at is not None and u.before_at is not None
        windows.add((u.since_at.date(), u.before_at.date()))
    assert windows == {
        (datetime(2026, 6, 1).date(), datetime(2026, 6, 7).date()),
        (datetime(2026, 6, 8).date(), datetime(2026, 6, 14).date()),
    }


def test_linear_backfill_units_never_write_watermarks(db_session, monkeypatch):
    """Regression: Linear backfill units must carry mode=backfill and no watermark.

    Mirrors the invariant in test_sync_units.py::test_run_sync_unit_success_skips_watermark_for_backfill.
    The planner side of the contract: all units produced for a Linear backfill
    carry mode='backfill', which is the gate the worker checks before writing
    watermarks (workers/sync_units.py:401-408). This test asserts the planner
    never emits a non-backfill mode for a backfill request, and that no
    SyncWatermark rows exist after planning (planning never writes watermarks).
    """
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)

    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-3", provider="linear"
    )
    for dataset_key in (
        "work-items",
        "work-item-labels",
        "work-item-projects",
        "work-item-history",
        "work-item-comments",
    ):
        _create_dataset(db_session, integration, dataset_key)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=datetime(2026, 5, 1, tzinfo=timezone.utc),
            before=datetime(2026, 5, 10, 23, 59, 59, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) > 0
    # All units must carry mode=backfill — the worker gate reads this field.
    assert all(u.mode == SyncRunMode.BACKFILL.value for u in units), (
        "All Linear backfill units must carry mode=backfill"
    )
    # Planning must never write watermarks.
    assert db_session.query(SyncWatermark).count() == 0, (
        "plan_sync_run must not write any SyncWatermark rows"
    )


# ---------------------------------------------------------------------------
# CHAOS-2721 (AD-3): work-item-family plan-time collapse
# ---------------------------------------------------------------------------

_FAMILY_DATASETS = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)


def test_work_item_family_collapses_to_single_composite_unit(db_session):
    """Enabling all five work-item-family datasets emits ONE composite unit
    (canonical dataset_key="work-items") with a boolean family_dataset_<key>
    flag per enabled dataset, instead of five units each re-running the full
    crawl (CHAOS-2721)."""
    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1, f"expected ONE composite work-items unit, got {len(units)}"
    unit = units[0]
    assert unit.dataset_key == "work-items"
    flags = unit.processor_flags or {}
    for dataset_key in _FAMILY_DATASETS:
        flag = "family_dataset_" + dataset_key.replace("-", "_")
        assert flags.get(flag) is True, f"{flag} must be set on the composite unit"


def test_work_item_family_collapse_uses_earliest_window_across_datasets(db_session):
    """The composite unit's since_at is the EARLIEST watermark across enabled
    family datasets, so the single crawl covers every dataset (over-fetch is
    safe; CHAOS-2721 / AD-3)."""
    integration = _create_integration(db_session, provider="linear")
    source = _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    _create_dataset(db_session, integration, "work-items")
    _create_dataset(db_session, integration, "work-item-comments")
    # work-items synced more recently than comments -> comments is the laggard.
    newer = datetime(2026, 6, 15, 0, 0, tzinfo=timezone.utc)
    older = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    set_watermark(db_session, ORG_ID, source.external_id, "work-items", newer)
    set_watermark(db_session, ORG_ID, source.external_id, "work-item-comments", older)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 1
    assert units[0].since_at is not None
    assert units[0].since_at.replace(tzinfo=timezone.utc) == older


@pytest.mark.parametrize(
    "provider, external_id",
    [
        ("github", "full-chaos/dev-health"),
        ("gitlab", "group/project"),
        ("jira", "jira-project"),
        ("linear", "linear-team-1"),
    ],
)
def test_work_item_family_collapse_provider_matrix(db_session, provider, external_id):
    """Family collapse is provider-agnostic: all four providers collapse the
    enabled family to a single composite unit (provider x entity contract)."""
    integration = _create_integration(db_session, provider=provider)
    _create_source(db_session, integration, external_id=external_id, provider=provider)
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    work_item_units = [
        u
        for u in _planned_units(db_session, plan.sync_run_id)
        if u.dataset_key in _FAMILY_DATASETS
    ]
    assert len(work_item_units) == 1
    assert work_item_units[0].dataset_key == "work-items"


def test_work_item_family_collapse_backfill_one_composite_per_chunk(
    db_session, monkeypatch
):
    """A Linear backfill enabling all five family datasets produces ONE composite
    unit per chunk (7 for a 90-day/14-day backfill), not 5x7=35 (CHAOS-2721)."""
    monkeypatch.delenv("LINEAR_BACKFILL_MAX_WINDOW_DAYS", raising=False)
    integration = _create_integration(db_session, provider="linear")
    _create_source(
        db_session, integration, external_id="linear-team-1", provider="linear"
    )
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)

    since = datetime(2026, 3, 2, tzinfo=timezone.utc)
    before = datetime(2026, 5, 30, 23, 59, 59, tzinfo=timezone.utc)
    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="test",
            since=since,
            before=before,
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    assert len(units) == 7, f"expected 7 composite chunks, got {len(units)}"
    assert {u.dataset_key for u in units} == {"work-items"}


def test_github_family_composite_carries_prs_signal_alongside_code_unit(db_session):
    """github: the composite work-items unit carries sync_prs while the PRS code
    dataset remains its own unit (collapse only folds the work-item family)."""
    integration = _create_integration(db_session, provider="github")
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    for dataset_key in _FAMILY_DATASETS:
        _create_dataset(db_session, integration, dataset_key)
    _create_dataset(db_session, integration, "prs")

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    units = _planned_units(db_session, plan.sync_run_id)
    work_items = [u for u in units if u.dataset_key == "work-items"]
    prs_units = [u for u in units if u.dataset_key == "prs"]
    assert len(work_items) == 1
    assert (work_items[0].processor_flags or {}).get("sync_prs") is True
    assert len(prs_units) == 1


def test_merge_family_windows_rejects_mismatched_window_counts():
    """The index-aligned merge assumes every enabled family dataset resolves to
    the same number of windows; a mismatch is a planner invariant violation and
    must fail fast rather than silently dropping windows."""
    from dev_health_ops.sync.planner import _merge_family_windows

    a = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    b = datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc)
    c = datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc)
    d = datetime(2026, 6, 4, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="mismatched window counts"):
        _merge_family_windows([((a, b),), ((a, b), (c, d))])
