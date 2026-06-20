from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from dev_health_ops.sync.watermarks import set_watermark

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
    """NONE-behavior datasets (repo-metadata, work-item-labels, etc.) must
    keep since_at=None on incremental — cold-start depth must NOT be applied.
    """
    integration = _create_integration(db_session)
    integration.config = {"initial_sync_depth": 30}
    db_session.flush()
    _create_source(db_session, integration, external_id="full-chaos/dev-health")
    # work-item-labels has WatermarkBehavior.NONE
    _create_dataset(db_session, integration, "work-item-labels")

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
        "NONE-behavior dataset must keep since_at=None on incremental, "
        "not receive a cold-start depth window"
    )


# ---------------------------------------------------------------------------
# Finding #3 regression: tier cap fails closed
# ---------------------------------------------------------------------------


def test_tier_cap_fails_closed_on_non_uuid_org_id(db_session):
    """Non-UUID org_id must fail closed to default depth (30), not unlimited."""
    from dev_health_ops.sync.planner import (
        _DEFAULT_INITIAL_SYNC_DEPTH_DAYS,
        _get_tier_backfill_days_cap,
    )

    cap = _get_tier_backfill_days_cap(db_session, "not-a-uuid")
    assert cap == _DEFAULT_INITIAL_SYNC_DEPTH_DAYS, (
        "Non-UUID org_id must fail closed to default cap, not unlimited"
    )


def test_tier_cap_fails_closed_on_operational_error(db_session, monkeypatch):
    """OperationalError (missing tier_limits table) must fail closed to default depth."""
    import uuid as _uuid

    from sqlalchemy.exc import OperationalError

    from dev_health_ops.sync.planner import (
        _DEFAULT_INITIAL_SYNC_DEPTH_DAYS,
        _get_tier_backfill_days_cap,
    )

    def _raise_op_error(*args, **kwargs):
        raise OperationalError("no such table: tier_limits", None, None)

    monkeypatch.setattr(
        "dev_health_ops.api.services.licensing.TierLimitService.get_limit",
        _raise_op_error,
    )

    cap = _get_tier_backfill_days_cap(db_session, str(_uuid.uuid4()))
    assert cap == _DEFAULT_INITIAL_SYNC_DEPTH_DAYS, (
        "OperationalError must fail closed to default cap, not unlimited"
    )
