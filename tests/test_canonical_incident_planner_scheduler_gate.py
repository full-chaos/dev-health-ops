from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from dev_health_ops.models import (
    JobRun,
    JobStatus,
    ScheduledJob,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunUnit,
)
from dev_health_ops.sync import planner
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from tests.canonical_incident_orchestration_support import (
    CanonicalState,
    canonical_state_context,
    create_canonical_graph,
    remove_feature_override,
)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def _request(
    state: CanonicalState, integration_id: str, org_id: str
) -> SyncPlanRequest:
    return SyncPlanRequest(
        integration_id=integration_id,
        org_id=org_id,
        mode=SyncRunMode.INCREMENTAL.value,
        triggered_by="test",
        before=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )


def test_planner_creates_canonical_work_when_feature_enabled(
    canonical_state: CanonicalState,
) -> None:
    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.enabled_org_id)

    # When
    plan = plan_sync_run(
        state.session,
        _request(state, str(graph.integration.id), str(state.enabled_org_id)),
    )

    # Then
    assert plan.total_units == 11
    assert state.session.query(SyncRun).count() == 1


def test_planner_denies_canonical_work_before_persistence(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.disabled_org_id)
    monkeypatch.setattr(
        planner,
        "_resolve_credential_stamp",
        lambda *_args: pytest.fail("feature-denied plan hydrated credentials"),
    )

    # When
    with pytest.raises(RuntimeError, match="feature_disabled"):
        plan_sync_run(
            state.session,
            _request(state, str(graph.integration.id), str(state.disabled_org_id)),
        )

    # Then
    assert state.session.query(SyncRun).count() == 0
    assert state.session.query(SyncRunUnit).count() == 0
    assert state.session.query(SyncDispatchOutbox).count() == 0


def test_scheduler_skips_disabled_canonical_config_before_marker_or_work(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_scheduler

    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.disabled_org_id, with_config=True)
    assert graph.config is not None
    job = ScheduledJob(
        name=f"sync-config-{graph.config.id}",
        job_type="sync",
        schedule_cron="* * * * *",
        org_id=str(state.disabled_org_id),
        provider="pagerduty",
        sync_config_id=graph.config.id,
        tz="UTC",
        status=JobStatus.ACTIVE.value,
    )
    state.session.add(job)
    state.session.commit()
    dispatch = MagicMock()
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_units.dispatch_sync_run",
        dispatch,
    )

    # When
    dispatched = sync_scheduler._maybe_dispatch_config(
        state.session,
        graph.config,
        datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )

    # Then
    state.session.refresh(job)
    assert dispatched is False
    assert job.next_run_at is None
    assert state.session.query(SyncRun).count() == 0
    assert state.session.query(JobRun).count() == 0
    dispatch.apply_async.assert_not_called()


def test_scheduler_rechecks_feature_immediately_before_enqueue(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.sync import execution_trigger
    from dev_health_ops.workers import sync_scheduler

    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.enabled_org_id, with_config=True)
    assert graph.config is not None
    job = ScheduledJob(
        name=f"sync-config-{graph.config.id}",
        job_type="sync",
        schedule_cron="* * * * *",
        org_id=str(state.enabled_org_id),
        provider="pagerduty",
        sync_config_id=graph.config.id,
        tz="UTC",
        status=JobStatus.ACTIVE.value,
    )
    state.session.add(job)
    state.session.commit()
    dispatch = MagicMock()
    real_trigger = execution_trigger.create_sync_execution_trigger

    def create_then_disable(*args, **kwargs):
        trigger = real_trigger(*args, **kwargs)
        remove_feature_override(state, state.enabled_org_id)
        return trigger

    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(
        execution_trigger,
        "create_sync_execution_trigger",
        create_then_disable,
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_units.dispatch_sync_run",
        dispatch,
    )

    # When
    result = sync_scheduler._maybe_dispatch_config(
        state.session,
        graph.config,
        datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )

    # Then
    assert result is False
    dispatch.apply_async.assert_not_called()
    assert state.session.query(SyncRun).count() == 1


def test_scheduler_skips_typed_pagerduty_disable_without_enqueuing(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.sync import execution_trigger
    from dev_health_ops.sync.execution_trigger import SyncExecutionTriggerResult
    from dev_health_ops.workers import sync_scheduler

    state = canonical_state
    graph = create_canonical_graph(state, state.enabled_org_id, with_config=True)
    assert graph.config is not None
    job = ScheduledJob(
        name=f"sync-config-{graph.config.id}",
        job_type="sync",
        schedule_cron="* * * * *",
        org_id=str(state.enabled_org_id),
        provider="pagerduty",
        sync_config_id=graph.config.id,
        tz="UTC",
        status=JobStatus.ACTIVE.value,
    )
    state.session.add(job)
    state.session.commit()
    dispatch = MagicMock()
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(
        execution_trigger,
        "create_sync_execution_trigger",
        lambda *_args, **_kwargs: SyncExecutionTriggerResult(
            sync_run_id="sync-run-1",
            job_run_id="job-run-1",
            total_units=0,
            dispatch_required=False,
            terminal_reason="PagerDuty account identity needs repair",
        ),
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_units.dispatch_sync_run",
        dispatch,
    )

    result = sync_scheduler._maybe_dispatch_config(
        state.session,
        graph.config,
        datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert result is False
    dispatch.apply_async.assert_not_called()
