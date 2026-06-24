from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Setting,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.sync.config_migration import migrate_configs_to_integrations
from dev_health_ops.sync.trigger_routing import (
    MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
    is_migrated_trigger_routing_enabled,
    mark_sync_run_failed,
    plan_request_for_config,
    planner_request_for_config_if_routed,
    should_route_config_to_planner,
)

ORG_ID = "routing-org"
PLANNER_TAG_KEY = "planner_managed_sync_config_id"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _config(
    session: Session,
    *,
    provider: str = "github",
    sync_targets: list[str] | None = None,
    migrated_integration_id: uuid.UUID | None = None,
    migrated_source_id: uuid.UUID | None = None,
    parent_id: uuid.UUID | None = None,
    planner_managed: bool = False,
) -> SyncConfiguration:
    config = SyncConfiguration(
        org_id=ORG_ID,
        name=f"{provider}-config-{uuid.uuid4()}",
        provider=provider,
        sync_targets=sync_targets if sync_targets is not None else ["git"],
        sync_options={},
        migrated_integration_id=migrated_integration_id,
        migrated_source_id=migrated_source_id,
        parent_id=parent_id,
        planner_managed=planner_managed,
    )
    session.add(config)
    session.flush()
    return config


def _set_flag(session: Session, value: str) -> None:
    session.add(
        Setting(
            org_id=ORG_ID,
            category=SettingCategory.SYNC.value,
            key=MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
            value=value,
        )
    )
    session.flush()


def _integration(
    session: Session, integration_id: uuid.UUID, *, provider: str = "github"
) -> Integration:
    integration = Integration(
        id=integration_id,
        org_id=ORG_ID,
        provider=provider,
        name=f"{provider}-integration-{integration_id}",
        config={},
    )
    session.add(integration)
    session.flush()
    return integration


def _integration_source(
    session: Session,
    integration_id: uuid.UUID,
    *,
    provider: str = "github",
    full_name: str,
    metadata: dict[str, str] | None = None,
    is_enabled: bool = True,
) -> IntegrationSource:
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration_id,
        provider=provider,
        source_type="repository",
        external_id=full_name,
        name=full_name.rsplit("/", 1)[-1],
        full_name=full_name,
        metadata_=metadata or {},
        is_enabled=is_enabled,
    )
    session.add(source)
    session.flush()
    return source


def _source(session: Session, integration_id: uuid.UUID) -> IntegrationSource:
    _integration(session, integration_id)
    return _integration_source(
        session, integration_id, full_name="full-chaos/dev-health"
    )


# --- flag reader -----------------------------------------------------------


def test_flag_missing_is_disabled(db_session):
    assert is_migrated_trigger_routing_enabled(db_session, ORG_ID) is False


@pytest.mark.parametrize("value", ["true", "1", "yes", "on", "TRUE", "On"])
def test_flag_truthy_values_enable(db_session, value):
    _set_flag(db_session, value)
    assert is_migrated_trigger_routing_enabled(db_session, ORG_ID) is True


@pytest.mark.parametrize("value", ["false", "0", "no", "off", ""])
def test_flag_falsey_values_disable(db_session, value):
    _set_flag(db_session, value)
    assert is_migrated_trigger_routing_enabled(db_session, ORG_ID) is False


def test_flag_is_scoped_per_org(db_session):
    _set_flag(db_session, "true")
    assert is_migrated_trigger_routing_enabled(db_session, "other-org") is False


# --- plan request builder --------------------------------------------------


def test_unmigrated_config_returns_none(db_session):
    config = _config(db_session)
    assert plan_request_for_config(config, triggered_by="manual") is None


def test_parent_config_plans_whole_integration(db_session):
    integration_id = uuid.uuid4()
    config = _config(
        db_session,
        sync_targets=["git", "prs"],
        migrated_integration_id=integration_id,
    )
    req = plan_request_for_config(config, triggered_by="schedule")

    assert req is not None
    assert req.integration_id == str(integration_id)
    assert req.org_id == ORG_ID
    assert req.mode == "incremental"
    assert req.triggered_by == "schedule"
    # Parent => whole integration: no source/dataset scoping.
    assert req.source_ids is None
    assert req.dataset_keys is None


def test_child_config_scopes_to_source_and_datasets(db_session):
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    config = _config(
        db_session,
        provider="github",
        sync_targets=["prs"],
        migrated_integration_id=integration_id,
        migrated_source_id=source_id,
    )
    req = plan_request_for_config(config, triggered_by="manual")

    assert req is not None
    assert req.integration_id == str(integration_id)
    assert req.source_ids == (str(source_id),)
    # "prs" legacy target maps to the prs dataset key family.
    assert req.dataset_keys is not None
    assert "prs" in req.dataset_keys


def test_child_config_without_targets_falls_back_to_all_datasets(db_session):
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    config = _config(
        db_session,
        sync_targets=[],
        migrated_integration_id=integration_id,
        migrated_source_id=source_id,
    )
    req = plan_request_for_config(config, triggered_by="manual")

    assert req is not None
    assert req.source_ids == (str(source_id),)
    # No mappable targets => all enabled datasets (None), never an empty run.
    assert req.dataset_keys is None


def test_mode_override(db_session):
    config = _config(
        db_session,
        migrated_integration_id=uuid.uuid4(),
    )
    req = plan_request_for_config(config, triggered_by="manual", mode="backfill")
    assert req is not None
    assert req.mode == "backfill"


def test_planner_managed_parent_routes_without_flag(db_session):
    integration_id = uuid.uuid4()
    config = _config(
        db_session, migrated_integration_id=integration_id, planner_managed=True
    )
    _source(db_session, integration_id)

    req = planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual"
    )

    assert req is not None
    assert req.integration_id == str(integration_id)


@pytest.mark.parametrize("provider", ["github", "gitlab"])
def test_planner_managed_parent_scopes_to_tagged_enabled_sources(db_session, provider):
    integration_id = uuid.uuid4()
    _integration(db_session, integration_id, provider=provider)
    config = _config(
        db_session,
        provider=provider,
        migrated_integration_id=integration_id,
        planner_managed=True,
    )
    tag = {PLANNER_TAG_KEY: str(config.id)}
    selected = [
        _integration_source(
            db_session,
            integration_id,
            provider=provider,
            full_name=f"acme/{provider}-selected-a",
            metadata=tag,
        ),
        _integration_source(
            db_session,
            integration_id,
            provider=provider,
            full_name=f"acme/{provider}-selected-b",
            metadata=tag,
        ),
        _integration_source(
            db_session,
            integration_id,
            provider=provider,
            full_name=f"acme/{provider}-selected-c",
            metadata=tag,
        ),
    ]
    _integration_source(
        db_session,
        integration_id,
        provider=provider,
        full_name=f"acme/{provider}-untagged",
    )
    _integration_source(
        db_session,
        integration_id,
        provider=provider,
        full_name=f"acme/{provider}-disabled",
        metadata=tag,
        is_enabled=False,
    )

    req = planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual"
    )

    assert req is not None
    assert req.source_ids is not None
    assert set(req.source_ids) == {str(source.id) for source in selected}
    assert len(req.source_ids) == 3


def test_planner_managed_child_keeps_single_source_scope(db_session):
    integration_id = uuid.uuid4()
    _integration(db_session, integration_id)
    source = _integration_source(
        db_session, integration_id, full_name="full-chaos/dev-health"
    )
    child = _config(
        db_session,
        migrated_integration_id=integration_id,
        migrated_source_id=source.id,
        planner_managed=True,
    )

    req = planner_request_for_config_if_routed(db_session, child, triggered_by="manual")

    assert req is not None
    assert req.source_ids == (str(source.id),)


def test_planner_managed_parent_with_zero_tagged_enabled_sources_syncs_nothing(
    db_session,
):
    integration_id = uuid.uuid4()
    _integration(db_session, integration_id)
    config = _config(
        db_session,
        migrated_integration_id=integration_id,
        planner_managed=True,
    )
    _integration_source(db_session, integration_id, full_name="full-chaos/untagged")
    _integration_source(
        db_session,
        integration_id,
        full_name="full-chaos/disabled",
        metadata={PLANNER_TAG_KEY: str(config.id)},
        is_enabled=False,
    )

    req = planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual"
    )

    assert req is not None
    assert req.source_ids == ()


def test_flag_routed_parent_without_planner_marker_keeps_all_enabled_semantics(
    db_session,
):
    integration_id = uuid.uuid4()
    _integration(db_session, integration_id)
    config = _config(db_session, migrated_integration_id=integration_id)
    _integration_source(
        db_session,
        integration_id,
        full_name="full-chaos/tagged-but-legacy",
        metadata={PLANNER_TAG_KEY: str(config.id)},
    )
    _set_flag(db_session, "true")

    req = planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual"
    )

    assert req is not None
    assert req.source_ids is None


def test_migrated_single_with_source_needs_flag_without_planner_marker(db_session):
    integration_id = uuid.uuid4()
    config = _config(db_session, migrated_integration_id=integration_id)
    _source(db_session, integration_id)

    assert (
        planner_request_for_config_if_routed(db_session, config, triggered_by="manual")
        is None
    )
    _set_flag(db_session, "true")
    assert planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual"
    )


def test_migrated_parent_with_children_needs_flag(db_session):
    integration_id = uuid.uuid4()
    parent = _config(db_session, migrated_integration_id=integration_id)
    _config(
        db_session,
        migrated_integration_id=integration_id,
        migrated_source_id=uuid.uuid4(),
        parent_id=parent.id,
    )

    assert (
        planner_request_for_config_if_routed(db_session, parent, triggered_by="manual")
        is None
    )
    _set_flag(db_session, "true")
    assert planner_request_for_config_if_routed(
        db_session, parent, triggered_by="manual"
    )


def test_migrated_parent_with_sources_stays_legacy_for_all_triggers(db_session):
    _set_flag(db_session, "false")

    parent = SyncConfiguration(
        org_id=ORG_ID,
        name="legacy parent",
        provider="github",
        sync_targets=["git"],
        sync_options={"owner": "full-chaos"},
        is_active=True,
    )
    db_session.add(parent)
    db_session.flush()
    child = SyncConfiguration(
        org_id=ORG_ID,
        name="full-chaos/dev-health",
        provider="github",
        sync_targets=["git"],
        sync_options={"owner": "full-chaos", "repo": "dev-health"},
        parent_id=parent.id,
        is_active=True,
    )
    db_session.add(child)
    db_session.flush()

    migrate_configs_to_integrations(db_session)
    db_session.flush()

    assert parent.planner_managed is False
    assert child.planner_managed is False
    assert parent.migrated_integration_id is not None
    assert child.migrated_source_id is not None
    assert should_route_config_to_planner(db_session, parent) is False
    assert (
        planner_request_for_config_if_routed(db_session, parent, triggered_by="manual")
        is None
    )
    assert (
        planner_request_for_config_if_routed(
            db_session, parent, triggered_by="schedule"
        )
        is None
    )
    assert (
        planner_request_for_config_if_routed(
            db_session, parent, triggered_by="backfill", mode="backfill"
        )
        is None
    )


def test_operational_error_rolls_back_and_disables(monkeypatch, db_session):
    """A failed flag read (e.g. missing settings table on PG) must roll back
    the aborted transaction and return False so the caller's legacy path runs
    on a clean session."""
    from sqlalchemy.exc import OperationalError

    def _boom(*_args, **_kwargs):
        raise OperationalError("SELECT settings", {}, Exception("no such table"))

    rolled_back = {"count": 0}
    real_rollback = db_session.rollback

    def _spy_rollback(*args, **kwargs):
        rolled_back["count"] += 1
        return real_rollback(*args, **kwargs)

    monkeypatch.setattr(db_session, "query", _boom)
    monkeypatch.setattr(db_session, "rollback", _spy_rollback)

    assert is_migrated_trigger_routing_enabled(db_session, ORG_ID) is False
    assert rolled_back["count"] == 1


# --- mark_sync_run_failed (conditional compare-and-set) --------------------


def _make_sync_run(db_session, status):
    import uuid as _uuid

    from dev_health_ops.models import SyncRun

    run = SyncRun(
        org_id=ORG_ID,
        integration_id=_uuid.uuid4(),
        triggered_by="manual",
        mode="incremental",
        status=status,
        total_units=0,
    )
    db_session.add(run)
    db_session.flush()
    return run


def test_mark_sync_run_failed_marks_planned_run_and_units(db_session):
    import uuid as _uuid

    from dev_health_ops.models import (
        SyncRunStatus,
        SyncRunUnit,
        SyncRunUnitStatus,
    )

    run = _make_sync_run(db_session, SyncRunStatus.PLANNED.value)
    unit = SyncRunUnit(
        org_id=ORG_ID,
        sync_run_id=run.id,
        integration_id=run.integration_id,
        source_id=_uuid.uuid4(),
        provider="github",
        dataset_key="git",
        cost_class="light",
        mode="incremental",
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
    )
    db_session.add(unit)
    db_session.commit()

    mark_sync_run_failed(db_session, str(run.id), "dispatch enqueue failed")

    db_session.refresh(run)
    db_session.refresh(unit)
    assert run.status == SyncRunStatus.FAILED.value
    assert run.error == "dispatch enqueue failed"
    assert run.completed_at is not None
    # The still-PLANNED unit is failed so a late dispatcher claims nothing.
    assert unit.status == SyncRunUnitStatus.FAILED.value


def test_mark_sync_run_failed_stamps_canonical_config(db_session):
    from dev_health_ops.models import SyncRunStatus

    run = _make_sync_run(db_session, SyncRunStatus.PLANNED.value)
    config = SyncConfiguration(
        org_id=ORG_ID,
        name="canonical",
        provider="github",
        sync_targets=["git"],
        migrated_integration_id=run.integration_id,
    )
    db_session.add(config)
    db_session.commit()

    mark_sync_run_failed(db_session, str(run.id), "dispatch enqueue failed")

    db_session.refresh(config)
    assert config.last_sync_at is not None
    assert config.last_sync_success is False
    assert config.last_sync_error == "dispatch enqueue failed"
    assert config.last_sync_stats == {
        "error": "dispatch enqueue failed",
        "phase": "dispatch_enqueue",
    }


def test_mark_sync_run_failed_stamps_oldest_duplicate_parent_config(db_session):
    from dev_health_ops.models import SyncRunStatus

    run = _make_sync_run(db_session, SyncRunStatus.PLANNED.value)
    older = SyncConfiguration(
        org_id=ORG_ID,
        name="canonical-older",
        provider="github",
        sync_targets=["git"],
        migrated_integration_id=run.integration_id,
    )
    newer = SyncConfiguration(
        org_id=ORG_ID,
        name="canonical-newer",
        provider="github",
        sync_targets=["git"],
        migrated_integration_id=run.integration_id,
    )
    older.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer.created_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    db_session.add_all([newer, older])
    db_session.commit()

    mark_sync_run_failed(db_session, str(run.id), "dispatch enqueue failed")

    db_session.refresh(older)
    db_session.refresh(newer)
    assert older.last_sync_at is not None
    assert older.last_sync_success is False
    assert older.last_sync_error == "dispatch enqueue failed"
    assert newer.last_sync_at is None
    assert newer.last_sync_success is None
    assert newer.last_sync_error is None


def test_mark_sync_run_failed_noop_when_run_already_advanced(db_session):
    from dev_health_ops.models import SyncRunStatus

    # An ambiguous enqueue failure where the dispatcher already advanced the run
    # must NOT be overwritten back to FAILED.
    run = _make_sync_run(db_session, SyncRunStatus.DISPATCHING.value)
    db_session.commit()

    mark_sync_run_failed(db_session, str(run.id), "should not overwrite")

    db_session.refresh(run)
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.error != "should not overwrite"


def test_mark_sync_run_failed_missing_run_is_noop(db_session):
    import uuid as _uuid

    # No row for this id -> best-effort no-op, no exception.
    mark_sync_run_failed(db_session, str(_uuid.uuid4()), "x")
