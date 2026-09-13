from __future__ import annotations

import json
import logging
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    BackfillJob,
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncComputeCheckpoint,
    SyncComputeCheckpointStatus,
    SyncComputeType,
    SyncConfiguration,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    WorkerJobOutbox,
    WorkerJobRoute,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_PENDING,
)
from dev_health_ops.sync.error_sanitize import REDACTION_MARKER
from dev_health_ops.workers.post_sync_dispatch import build_post_sync_dispatch_payload
from tests._helpers import seed_sync_dispatch_transport_routes


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_sync_dispatch_transport_routes(session)
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db

    session.commit()
    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _file_backed_engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'sync-unit-race.db'}")
    Base.metadata.create_all(engine)
    return engine


def _seed_run(
    session,
    *,
    mode=SyncRunMode.INCREMENTAL.value,
    provider="github",
    source_type="repo",
    external_id="full-chaos/dev-health",
    name="dev-health",
    full_name="full-chaos/dev-health",
    dataset_key="commits",
    processor_flags=None,
):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider=provider,
        name="demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider=provider,
        source_type=source_type,
        external_id=external_id,
        name=name,
        full_name=full_name,
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=True,
        options={},
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, dataset, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode=mode,
        since_at=None,
        before_at=datetime.now(timezone.utc),
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags=processor_flags or {"sync_git": True},
    )
    session.add(unit)
    session.add(
        SyncRunReferenceDiscovery(
            org_id=org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    session.flush()
    return run, unit


def _seed_zero_unit_run(
    session,
    *,
    provider="linear",
    planner_error=None,
    planner_result=None,
):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider=provider,
        name=f"{provider}-demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
        error=planner_error,
        result=planner_result,
    )
    session.add(run)
    session.flush()
    session.add(
        SyncRunReferenceDiscovery(
            org_id=org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    session.flush()
    return run


def _patch_worker_enqueues(monkeypatch):
    """Capture the run-level Celery enqueues dispatch still makes.

    CHAOS-4054 step 4: a provider unit is never published to Celery any more,
    so there is no per-unit signature left to fake here. What a dispatch pass
    stages for a unit is a durable ``sync.provider_unit`` outbox row, which the
    tests read straight from ``WorkerJobOutbox``; the redispatch countdown and
    the finalize hand-off are the only Celery publishes that remain.
    """

    from dev_health_ops.workers import sync_units

    dispatch_calls = []
    finalize_calls = []

    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, **kwargs: dispatch_calls.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalize_calls.append((args, queue)),
    )
    return dispatch_calls, finalize_calls


def _outbox_unit_keys(session):
    """Every provider-unit dedupe key staged in the durable outbox."""

    return {
        row.dedupe_key
        for row in session.query(WorkerJobOutbox).all()
        if row.job_kind == "sync.provider_unit"
    }


def test_finalize_aggregates_partial_failed(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical-partial",
        provider="github",
        sync_targets=["git", "prs"],
        integration_id=run.integration_id,
    )
    db_session.add(config)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    failed = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.FAILED.value,
        attempts=1,
    )
    run.total_units = 2
    db_session.add(failed)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(config)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_units == 1
    assert run.failed_units == 1
    assert config.last_sync_success is False
    assert config.last_sync_error == "Sync run completed with failed units"


def test_finalize_writes_ready_compute_checkpoints_for_successful_work_graph_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    unit.since_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    unit.before_at = datetime(2026, 6, 2, tzinfo=timezone.utc)
    failed_prs = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.FAILED.value,
        attempts=1,
    )
    deployments = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="deployments",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.SUCCESS.value,
        attempts=1,
    )
    run.total_units = 3
    db_session.add_all([failed_prs, deployments])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))
    second = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "finalized"
    assert second["status"] == "already_dispatched"
    checkpoints = db_session.query(SyncComputeCheckpoint).all()
    assert len(checkpoints) == 1
    checkpoint = checkpoints[0]
    assert checkpoint.sync_run_unit_id == unit.id
    assert checkpoint.compute_type == SyncComputeType.WORK_GRAPH.value
    assert checkpoint.status == SyncComputeCheckpointStatus.READY.value
    assert checkpoint.window_start == unit.since_at
    assert checkpoint.window_end == unit.before_at
    assert checkpoint.completed_at is None
    assert checkpoint.checkpoint_metadata == {
        "cost_class": "medium",
        "mode": SyncRunMode.INCREMENTAL.value,
        "legacy_targets": ["git"],
    }


def test_finalize_does_not_checkpoint_until_all_units_terminal(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    running = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
    )
    run.total_units = 2
    db_session.add(running)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "pending"
    assert db_session.query(SyncComputeCheckpoint).count() == 0


def test_finalize_continues_when_compute_checkpointing_fails(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    def fail_checkpoint(*_args, **_kwargs):
        raise SQLAlchemyError("checkpoint unavailable")

    monkeypatch.setattr(
        sync_units, "_checkpoint_successful_compute_inputs", fail_checkpoint
    )

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert db_session.query(SyncRunPostDispatch).count() == 1
    assert (
        db_session.query(SyncDispatchOutbox)
        .filter(SyncDispatchOutbox.kind == OUTBOX_KIND_POST_SYNC)
        .count()
        == 1
    )


def test_build_post_sync_dispatch_payload_matches_finalize_window_fields(db_session):
    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    unit.since_at = datetime(2026, 6, 1, 10, 30, tzinfo=timezone.utc)
    unit.before_at = datetime(2026, 6, 3, 22, 15, tzinfo=timezone.utc)
    db_session.flush()

    payload = build_post_sync_dispatch_payload(db_session, run.id)

    assert payload is not None
    assert payload.provider == "github"
    assert payload.sync_targets == ["git"]
    assert payload.org_id == run.org_id
    assert payload.from_date == "2026-06-01"
    assert payload.to_date == "2026-06-03"
    assert payload.work_graph_from_date == "2026-06-01T00:00:00+00:00"
    assert payload.work_graph_to_date == "2026-06-04T00:00:00+00:00"


def test_build_post_sync_dispatch_payload_returns_none_without_success(db_session):
    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.FAILED.value
    db_session.flush()

    assert build_post_sync_dispatch_payload(db_session, run.id) is None


@pytest.mark.parametrize(
    ("sync_options", "expected"),
    [
        ({}, False),
        (
            {
                "auto_import_teams": False,
                "auto_import_projects": False,
                "auto_import_members": False,
            },
            False,
        ),
        ({"auto_import_teams": True}, True),
        # CHAOS-4323: the dispatch gate is an OR across all three -- selecting
        # ONLY projects (teams/members left off) must still fire the gate so
        # the post-sync team-autoimport task dispatches at all; the task
        # itself then honours each flag independently once it runs.
        ({"auto_import_projects": True}, True),
        ({"auto_import_members": True}, True),
    ],
)
def test_build_post_sync_dispatch_payload_auto_import_teams_is_any_of_three(
    db_session, sync_options, expected
):
    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical",
        provider="github",
        sync_targets=["git"],
        sync_options=sync_options,
        integration_id=run.integration_id,
    )
    db_session.add(config)
    db_session.flush()

    payload = build_post_sync_dispatch_payload(db_session, run.id)

    assert payload is not None
    assert payload.auto_import_teams is expected


def test_finalize_zero_unit_run_does_not_report_success(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(db_session)
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.completed_units == 0
    assert run.failed_units == 0
    assert run.error == "No sync units planned"
    assert run.result == {
        "completed_units": 0,
        "failed_units": 0,
        "reason": "no_sync_units_planned",
    }
    assert db_session.query(SyncRunPostDispatch).count() == 1
    post_sync_outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_POST_SYNC)
        .one()
    )
    assert post_sync_outbox.status == OUTBOX_STATUS_PENDING


class _RecordingCounter:
    """Minimal stand-in for the dual Prometheus/OTel counter."""

    def __init__(self):
        self.increments = []

    def labels(self, **values):
        counter = self

        class _Bound:
            def inc(self, amount=1):
                counter.increments.append((values, amount))

        return _Bound()


def test_finalize_zero_unit_run_preserves_the_planner_recorded_cause(
    db_session, monkeypatch
):
    """CHAOS-4159: finalize must not overwrite a diagnosis the planner made.

    A zero-unit run still finalizes FAILED -- that trade is ratified by
    ``test_fully_caught_up_plan_finalizes_failed_not_silently_successful`` and
    is NOT what this test changes. What it pins is that the run must not
    finalize ANONYMOUSLY. Finalize has no units to read a cause off, so it
    cannot know why the plan was empty; the planner can, and the terminalizing
    planner paths already write ``error`` plus ``result.error_category`` onto
    the run before dispatch.

    Before this fix the ``total_units == 0`` branch overwrote both
    unconditionally, so a PagerDuty integration with no credential, a disabled
    sync target and a genuinely empty plan all ended up with the identical
    ``"No sync units planned"``. An operator reading ``sync_runs.error`` then
    cannot tell "attach a credential" from "this is expected" -- which is
    precisely how a local stack accumulated hundreds of indistinguishable red
    rows.
    """
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(
        db_session,
        provider="pagerduty",
        planner_error="PagerDuty credential is unavailable",
        planner_result={"error_category": "pagerduty_credential_unavailable"},
    )
    _patch_db_session(monkeypatch, db_session)

    sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert run.status == SyncRunStatus.FAILED.value, (
        "the zero-unit FAILED path is ratified and must stay -- this fix "
        "changes the label, never the status"
    )
    assert run.error == "PagerDuty credential is unavailable", (
        "the planner's diagnosis must survive finalize, not be replaced by "
        "the generic zero-unit label"
    )
    assert run.result == {
        "completed_units": 0,
        "failed_units": 0,
        "error_category": "pagerduty_credential_unavailable",
        "reason": "pagerduty_credential_unavailable",
    }


def test_finalize_zero_unit_run_prefers_an_explicit_planner_reason(
    db_session, monkeypatch
):
    """An explicit ``reason`` outranks ``error_category`` as the label.

    ``error_category`` is a coarse bucket that other consumers already read;
    ``reason`` is the planner saying exactly what it decided. When both are
    present the specific one is the classification, and the bucket is carried
    through unchanged for those other consumers.
    """
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(
        db_session,
        provider="pagerduty",
        planner_error="PagerDuty sync target must be operational",
        planner_result={
            "error_category": "pagerduty_sync_disabled",
            "reason": "pagerduty_targets_malformed",
        },
    )
    _patch_db_session(monkeypatch, db_session)

    sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert run.result["reason"] == "pagerduty_targets_malformed"
    assert run.result["error_category"] == "pagerduty_sync_disabled"


def test_finalize_zero_unit_run_ignores_a_blank_planner_reason(db_session, monkeypatch):
    """A blank recorded reason is absence, not a classification.

    An empty label is worse than the honest generic one: it reads as a
    classification that ran and found nothing, so an operator stops looking.
    """
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(
        db_session,
        provider="github",
        planner_result={"reason": "   ", "error_category": ""},
    )
    _patch_db_session(monkeypatch, db_session)

    sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert run.error == "No sync units planned"
    assert run.result["reason"] == "no_sync_units_planned"
    assert "error_category" not in run.result


@pytest.mark.parametrize("blank_error", ("", "   "))
def test_finalize_zero_unit_run_treats_a_blank_planner_error_as_absent(
    db_session, monkeypatch, blank_error
):
    """A blank recorded error is absence, exactly as a blank reason is.

    ``_zero_unit_reason`` already treats a blank ``reason`` as absent, on the
    argument that an empty label reads as a classification that ran and found
    nothing (see the sibling test above). ``run.error`` is that same argument
    one column over, and the preserve-the-cause branch did not carry it: only
    ``run.error is None`` took the generic literal, so a blank-but-not-NULL
    error took the preserving branch and survived as an EMPTY cause.

    That is the worst of the three outcomes. ``sync_runs.error`` is served raw
    to operators through the admin job-history endpoint, so the run shows as
    FAILED with no reason at all -- strictly less useful than the honest
    generic label it replaced, because it reads as "the system had nothing to
    say" rather than "the reason was not captured".

    No current producer writes a blank error: the three writers that can put a
    cause on a zero-unit run all write non-empty text (``sync/planner.py``
    writes ``error=reason``, ``sync/feature_denial.py`` writes ``str(error)``,
    and the dispatch-denied path in this module writes ``decision.reason or
    "sync dispatch denied"``). So this closes the branch's own asymmetry
    rather than a defect reachable today -- which is why it is pinned by a
    test instead of left for the next reader to notice.
    """
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(
        db_session,
        provider="github",
        planner_error=blank_error,
    )
    _patch_db_session(monkeypatch, db_session)

    sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert run.status == SyncRunStatus.FAILED.value
    assert run.error == "No sync units planned", (
        "a blank error is absence, not a preserved cause -- preserving it "
        "leaves an operator staring at a FAILED run with no reason at all"
    )
    assert run.result["reason"] == "no_sync_units_planned"


def test_finalize_zero_unit_run_counts_by_provider_and_reason(db_session, monkeypatch):
    """Standing telemetry order: the new classification must be observable.

    Counted from the once-only branch. A re-finalization of an
    already-terminalized run must NOT count again, or retry pressure reads as
    more zero-unit runs than actually happened.
    """
    from dev_health_ops.workers import sync_units

    counter = _RecordingCounter()
    monkeypatch.setattr(sync_units, "ZERO_UNIT_FINALIZATIONS_TOTAL", counter)

    run = _seed_zero_unit_run(
        db_session,
        provider="PagerDuty",
        planner_error="PagerDuty credential is unavailable",
        planner_result={"error_category": "pagerduty_credential_unavailable"},
    )
    _patch_db_session(monkeypatch, db_session)

    sync_units.finalize_sync_run(str(run.id))
    assert counter.increments == [
        (
            {
                "provider": "pagerduty",
                "reason": "pagerduty_credential_unavailable",
            },
            1,
        )
    ]

    result = sync_units.finalize_sync_run(str(run.id))
    assert result["status"] == "already_dispatched"
    assert len(counter.increments) == 1, "a re-finalization must not be counted again"


def test_finalize_zero_unit_run_sanitizes_the_cause_it_preserves(
    db_session, monkeypatch
):
    """Preserving the planner's cause must not un-do a scrub.

    The generic literal this branch used to write unconditionally was, by
    accident, also a sanitizer: whatever was in ``sync_runs.error`` was thrown
    away before anyone could read it. ``sync_runs.error`` is served raw to
    operators through the admin job-history endpoint, and the pre-existing
    ``run_error`` sanitization only guards the COPY into
    ``SyncConfiguration.last_sync_error`` -- not the column itself. So a
    credential-shaped fragment left on the run by an older release, or by any
    future planner path that stringifies an exception, would now reach an
    operator's screen.
    """
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(
        db_session,
        provider="github",
        planner_error="plan failed: Authorization: Bearer ghp_notarealtokenvalue",
        planner_result={"error_category": "planner_error"},
    )
    _patch_db_session(monkeypatch, db_session)

    sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert "ghp_notarealtokenvalue" not in (run.error or "")
    assert REDACTION_MARKER in (run.error or "")
    assert run.result["reason"] == "planner_error"


def test_finalize_zero_unit_run_counts_only_after_the_transaction_commits(
    db_session, monkeypatch
):
    """A rolled-back finalization must not leave an increment behind.

    ``nested.commit()`` only releases a savepoint. If the work after it raises,
    the outer transaction rolls back and nothing was durably finalized -- but a
    counter already incremented stays incremented. Over a retry sequence that
    publishes N+1 for one real finalization, which is exactly the direction
    that moves an alert threshold.
    """
    from dev_health_ops.workers import sync_units

    counter = _RecordingCounter()
    monkeypatch.setattr(sync_units, "ZERO_UNIT_FINALIZATIONS_TOTAL", counter)

    run = _seed_zero_unit_run(db_session, provider="linear")
    _patch_db_session(monkeypatch, db_session)

    boom = RuntimeError("post-dispatch payload build failed")
    monkeypatch.setattr(
        sync_units,
        "build_post_sync_dispatch_payload",
        lambda *args, **kwargs: (_ for _ in ()).throw(boom),
    )
    with pytest.raises(RuntimeError):
        sync_units.finalize_sync_run(str(run.id))

    assert counter.increments == [], (
        "a finalization whose transaction did not commit must not be counted"
    )


def test_finalize_sync_run_only_syncs_nonterminal_job_run_observers(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    scheduled = ScheduledJob(
        org_id=run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=uuid.uuid4(),
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    running_observer = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.RUNNING.value,
    )
    running_observer.result = {"sync_run_id": str(run.id)}
    terminal_observer = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.FAILED.value,
    )
    terminal_observer.error = "already terminal"
    terminal_observer.result = {"sync_run_id": str(run.id), "sentinel": "preserved"}
    db_session.add_all([running_observer, terminal_observer])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(running_observer)
    db_session.refresh(terminal_observer)
    assert result["status"] == "finalized"
    assert running_observer.status == JobRunStatus.SUCCESS.value
    assert running_observer.completed_at == run.completed_at
    assert terminal_observer.status == JobRunStatus.FAILED.value
    assert terminal_observer.error == "already terminal"
    assert terminal_observer.result == {
        "sync_run_id": str(run.id),
        "sentinel": "preserved",
    }


def test_finalize_sync_run_sanitizes_copied_run_error_into_observer_columns(
    db_session, monkeypatch
):
    """CHAOS-2766 codex review finding, round 2: finalize_sync_run and
    sync_observers_for_terminal_sync_run both copy SyncRun.error VERBATIM
    into other durable columns (SyncConfiguration.last_sync_error,
    JobRun.error, BackfillJob.error_message) via a plain variable
    assignment -- not str(exc)/an f-string -- so the CHAOS-2766 AST guard
    (test_error_sanitize_guard.py) cannot see this propagation. A row
    written before sanitize_error_text existed (or written by any future
    site this repo's guard doesn't cover) could carry raw credential text in
    SyncRun.error; this asserts that text is re-sanitized at every copy
    site, not just at the original write."""
    from dev_health_ops.sync.error_sanitize import REDACTION_MARKER
    from dev_health_ops.workers import sync_units

    fixture_value = "ghp_" + "FAKE1234567890abcdefghijklmnopqrst"
    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.FAILED.value
    # Simulates a row carrying raw credential text at rest -- set directly on
    # the column rather than through a sanitizing write path, exactly what a
    # pre-CHAOS-2766 row (or any future unsanitized write) would look like.
    run.error = (
        "403 rate limited -- Authorization: Bearer "
        + fixture_value
        + " (redis://:"
        + fixture_value
        + "@redis-broker.internal:6379/0)"
    )
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical-copied-error-sanitize",
        provider="github",
        sync_targets=["git"],
        integration_id=run.integration_id,
    )
    db_session.add(config)
    db_session.flush()
    scheduled = ScheduledJob(
        org_id=run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=config.id,
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    pending_job_run = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.PENDING.value,
    )
    pending_job_run.result = {"sync_run_id": str(run.id)}
    backfill_job = BackfillJob(
        org_id=str(run.org_id),
        sync_config_id=config.id,
        celery_task_id=f"sync_run:{run.id}",
        status="pending",
        since_date=date(2026, 1, 1),
        before_date=date(2026, 1, 8),
        total_chunks=1,
    )
    db_session.add_all([pending_job_run, backfill_job])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(config)
    db_session.refresh(pending_job_run)
    db_session.refresh(backfill_job)
    assert result["status"] == "finalized"

    for persisted_text in (
        config.last_sync_error,
        pending_job_run.error,
        backfill_job.error_message,
    ):
        assert persisted_text is not None
        assert fixture_value not in persisted_text
        assert "Bearer" not in persisted_text
        assert REDACTION_MARKER in persisted_text


def test_dispatch_sync_run_redispatches_only_planned_units(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    recent_dispatching = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
    )
    db_session.add(recent_dispatching)
    db_session.flush()
    recent_dispatching.updated_at = datetime.now(timezone.utc)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(recent_dispatching)
    assert result == {"status": "dispatched", "queued_units": 1}
    # Only the PLANNED unit is staged: the freshly DISPATCHING sibling is not
    # re-claimed, so it never gets a second outbox row.
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{planned.id}"}
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert recent_dispatching.status == SyncRunUnitStatus.DISPATCHING.value


def test_dispatch_sync_run_routes_only_the_matrix_routable_unit_to_river(
    db_session, monkeypatch
):
    """Only the pair the capability matrix routes reaches the outbox.

    The sibling unit names a pair the checked-in matrix does not carry at all.
    River is the only runtime left, so there is no second writer for it to
    fall through to: it is terminalized as ``feature_disabled`` in the same
    pass that stages the routable unit.
    """

    from dev_health_ops.workers import sync_units

    run, launchdarkly = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        dataset_key="feature-flags",
    )
    unroutable_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=launchdarkly.integration_id,
        source_id=launchdarkly.source_id,
        provider="synthetic",
        dataset_key="matrix-incomplete",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={},
    )
    run.total_units = 2
    db_session.add(unroutable_unit)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    rows = db_session.query(WorkerJobOutbox).all()
    db_session.refresh(unroutable_unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert unroutable_unit.status == SyncRunUnitStatus.FAILED.value
    assert unroutable_unit.error == "feature_disabled"
    assert len(rows) == 1
    assert rows[0].job_kind == "sync.provider_unit"
    assert rows[0].dedupe_key == f"sync.provider_unit:{launchdarkly.id}"
    assert rows[0].args["payload"] == {"unit_id": str(launchdarkly.id)}
    assert rows[0].args["domain"] == {
        "type": "sync_run_unit",
        "id": str(launchdarkly.id),
    }
    assert set(rows[0].args) == {
        "contract_version",
        "organization_id",
        "correlation_id",
        "idempotency_key",
        "domain",
        "payload",
    }


# NOTE: ``test_dispatch_sync_run_durable_celery_route_overrides_plannable_capability``
# formerly here asserted that a durable ``sync.provider_unit`` route of
# ``celery`` beat the capability matrix and kept a fully plannable pair on the
# Celery writer. CHAOS-4054 step 4 deleted the Celery dispatch plane and with
# it that read: dispatch no longer resolves the durable route at all, so
# "durable route overrides capability" is not a behaviour that can be asserted
# or violated any more. Its only other claim -- that launchdarkly/feature-flags
# is a route-ready, plannable pair -- is asserted positively by
# ``test_dispatch_sync_run_routes_only_the_matrix_routable_unit_to_river`` and
# ``test_dispatch_plannable_aggregate_route_has_only_the_river_writer``.


_GITHUB_WORK_ITEM_FAMILY_FLAGS = {
    "family_dataset_work_items": True,
    "family_dataset_work_item_labels": True,
    "family_dataset_work_item_projects": True,
    "family_dataset_work_item_history": True,
    "family_dataset_work_item_comments": True,
}


def _plan_caught_up_github_work_item_family(session):
    """Build the ordinary caught-up-sibling planner shape for routing tests."""

    from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
    from dev_health_ops.sync.watermarks import set_watermark

    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="github-caught-up-family",
        config={"initial_sync_depth": 30},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
    )
    session.add_all(
        [
            source,
            IntegrationDataset(
                org_id=org_id,
                integration_id=integration.id,
                dataset_key="work-items",
                is_enabled=True,
                options={},
            ),
            IntegrationDataset(
                org_id=org_id,
                integration_id=integration.id,
                dataset_key="work-item-labels",
                is_enabled=True,
                options={},
            ),
        ]
    )
    session.flush()
    anchor = datetime.now(timezone.utc)
    requested_before = anchor - timedelta(days=5)
    set_watermark(
        session, org_id, source.external_id, "work-items", anchor - timedelta(days=9)
    )
    set_watermark(
        session,
        org_id,
        source.external_id,
        "work-item-labels",
        anchor - timedelta(days=2),
    )
    plan = plan_sync_run(
        session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=org_id,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=requested_before,
        ),
    )
    unit = session.get(SyncRunUnit, uuid.UUID(plan.unit_ids[0]))
    assert unit is not None
    assert unit.dataset_key == "work-items"
    flags = unit.processor_flags or {}
    assert {name: flags.get(name) for name in _GITHUB_WORK_ITEM_FAMILY_FLAGS} == (
        _GITHUB_WORK_ITEM_FAMILY_FLAGS
    )
    discovery = (
        session.query(SyncRunReferenceDiscovery)
        .filter_by(sync_run_id=uuid.UUID(plan.sync_run_id))
        .one()
    )
    discovery.status = "success"
    discovery.attempts = 1
    discovery.completed_at = datetime.now(timezone.utc)
    session.flush()
    return session.get(SyncRun, uuid.UUID(plan.sync_run_id)), unit


def test_dispatch_planned_caught_up_github_family_keeps_one_writer(
    db_session, monkeypatch
):
    """A real caught-up planner unit stages exactly one writer.

    The cutover-order parametrize this test used to carry (forward to River
    before Python was retired, rollback to Celery after Go was disabled) is
    gone with the Celery plane: River is the only runtime, so the single
    canonical family claim has exactly one destination -- one outbox row, and
    nothing else.
    """

    from dev_health_ops.workers import sync_units

    run, unit = _plan_caught_up_github_work_item_family(db_session)
    assert run is not None
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "dispatched",
        "queued_units": 1,
    }
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{unit.id}"}


def test_dispatch_sync_run_github_work_items_claim_stages_one_river_writer(
    db_session, monkeypatch
):
    """The one valid canonical family claim stages exactly one River writer."""

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="github",
        dataset_key="work-items",
        processor_flags=_GITHUB_WORK_ITEM_FAMILY_FLAGS,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "dispatched",
        "queued_units": 1,
    }
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{unit.id}"}


# NOTE: ``test_dispatch_sync_run_github_work_items_rollback_disables_go_before_python``
# formerly here asserted that flipping the durable ``sync.provider_unit`` route
# back to ``celery`` was the rollback barrier that took the valid canonical
# work-items claim away from Go and handed it to the Python writer. CHAOS-4054
# step 4 deleted the Python writer's dispatch path, so there is no rollback
# barrier left to assert: the claim's only destination is the outbox, which
# ``test_dispatch_sync_run_github_work_items_claim_stages_one_river_writer``
# above already proves.


def test_dispatch_sync_run_github_work_item_direct_alias_never_stages_a_writer(
    db_session, monkeypatch
):
    """A malformed alias claim is refused before any staging happens."""

    from dev_health_ops.jobs.routes import WorkerJobRouteError
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="github",
        dataset_key="work-item-comments",
        processor_flags=_GITHUB_WORK_ITEM_FAMILY_FLAGS,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with pytest.raises(WorkerJobRouteError, match="canonical"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert db_session.query(WorkerJobOutbox).count() == 0


@pytest.mark.parametrize(
    "processor_flags",
    (
        pytest.param(
            {
                "family_dataset_work_items": True,
                "family_dataset_work_item_labels": True,
                "family_dataset_work_item_projects": True,
                "family_dataset_work_item_history": True,
            },
            id="missing-family-flag",
        ),
        pytest.param(
            {
                **_GITHUB_WORK_ITEM_FAMILY_FLAGS,
                "family_dataset_unrecognized": True,
            },
            id="unknown-family-flag",
        ),
    ),
)
def test_dispatch_sync_run_github_work_items_rejects_partial_canonical_claim(
    db_session, monkeypatch, processor_flags: dict[str, bool]
):
    """A stale non-exact family claim is refused before it stages anything."""

    from dev_health_ops.jobs.routes import WorkerJobRouteError
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="github",
        dataset_key="work-items",
        processor_flags=processor_flags,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with pytest.raises(WorkerJobRouteError, match="complete canonical"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert db_session.query(WorkerJobOutbox).count() == 0


@pytest.mark.parametrize("provider", ("gitlab", "jira", "linear"))
@pytest.mark.parametrize(
    ("dataset_key", "processor_flags"),
    (
        pytest.param(
            "work-item-comments",
            _GITHUB_WORK_ITEM_FAMILY_FLAGS,
            id="direct-alias",
        ),
        pytest.param(
            "work-items",
            {
                "family_dataset_work_items": True,
                "family_dataset_work_item_labels": True,
                "family_dataset_work_item_projects": True,
                "family_dataset_work_item_history": True,
            },
            id="missing-flag",
        ),
        pytest.param(
            "work-items",
            {
                **_GITHUB_WORK_ITEM_FAMILY_FLAGS,
                "family_dataset_work_item_comments": False,
            },
            id="false-flag",
        ),
        pytest.param(
            "work-items",
            {**_GITHUB_WORK_ITEM_FAMILY_FLAGS, "family_dataset_unknown": True},
            id="unknown-flag",
        ),
    ),
)
def test_dispatch_enabled_atomic_work_item_family_rejects_before_staging(
    db_session,
    monkeypatch,
    provider: str,
    dataset_key: str,
    processor_flags: dict[str, bool],
) -> None:
    """An enabled Go family rejects stale ownership before anything is staged."""

    from dev_health_ops.jobs.routes import WorkerJobRouteError
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider=provider,
        dataset_key=dataset_key,
        processor_flags=processor_flags,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with pytest.raises(WorkerJobRouteError, match="complete canonical family"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert db_session.query(WorkerJobOutbox).count() == 0


# NOTE: ``test_dispatch_default_off_work_item_family_keeps_legacy_claim_admissible``
# formerly here asserted the OPPOSITE of the test just above: that an
# incomplete work-item-family claim was still admissible for gitlab/jira/
# linear when their route switch was off ("default-off admission"). CHAOS-4054
# deleted that leniency along with the switch plane -- every production
# caller now passes ``strict_atomic=True`` unconditionally
# (``provider_family_contract.validate_provider_family_claim``), so an
# incomplete claim is rejected for every provider, with no "default-off"
# carve-out left. That is exactly what
# ``test_dispatch_enabled_atomic_work_item_family_rejects_before_staging``
# above already proves for gitlab/jira/linear (its ``provider`` parametrize
# covers all three), so the deleted test's assertion would now be a
# contradiction, not a fixup.


@pytest.mark.parametrize(
    ("dataset_key", "processor_flags"),
    (
        ("incidents", {"sync_incidents": True}),
        ("incident-alerts", {}),
        ("incident-log-entries", {}),
        ("incident-notes", {}),
    ),
)
def test_dispatch_pagerduty_incident_family_preserves_independent_d16_claims(
    db_session,
    monkeypatch,
    dataset_key: str,
    processor_flags: dict[str, bool],
) -> None:
    """PagerDuty's incident quartet stays INDEPENDENT (D16): each of the four
    datasets is its own admissible claim, never atomic-family-collapsed --
    ``validate_provider_family_claim`` never rejects them regardless of the
    (now-strict-by-default) atomic admission rule. All four are also always
    route-ready and plannable, so each admitted claim stages exactly one
    River outbox row of its own.
    """

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="pagerduty",
        dataset_key=dataset_key,
        processor_flags=processor_flags,
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(
        sync_units,
        "require_canonical_incident_feature_for_update_sync",
        lambda *_args: None,
    )
    _patch_worker_enqueues(monkeypatch)

    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "dispatched",
        "queued_units": 1,
    }
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{unit.id}"}


_AGGREGATE_ROUTE_DISPATCH_CASES = (
    pytest.param("gitlab", "deployments", {}, id="gitlab-deployments"),
    pytest.param(
        "gitlab",
        "work-items",
        _GITHUB_WORK_ITEM_FAMILY_FLAGS,
        id="gitlab-work-items",
    ),
    pytest.param(
        "jira",
        "work-items",
        _GITHUB_WORK_ITEM_FAMILY_FLAGS,
        id="jira-work-items",
    ),
    pytest.param(
        "linear",
        "work-items",
        _GITHUB_WORK_ITEM_FAMILY_FLAGS,
        id="linear-work-items",
    ),
    pytest.param("pagerduty", "services", {}, id="pagerduty-services"),
    pytest.param(
        "pagerduty",
        "incidents",
        {"sync_incidents": True},
        id="pagerduty-incidents",
    ),
    pytest.param("pagerduty", "incident-alerts", {}, id="pagerduty-incident-alerts"),
    pytest.param(
        "pagerduty",
        "incident-log-entries",
        {},
        id="pagerduty-incident-log-entries",
    ),
    pytest.param("pagerduty", "incident-notes", {}, id="pagerduty-incident-notes"),
)


@pytest.mark.parametrize(
    ("provider", "dataset_key", "processor_flags"),
    _AGGREGATE_ROUTE_DISPATCH_CASES,
)
def test_dispatch_plannable_aggregate_route_has_only_the_river_writer(
    db_session,
    monkeypatch,
    provider: str,
    dataset_key: str,
    processor_flags: dict[str, bool],
) -> None:
    """CHAOS-4054: each pair here is always route-ready and plannable, with
    no switch left to flip -- capability is always on in the binary. That
    alone is enough to send the unit to River and nowhere else.

    The old switch-off counterpart of this test
    (``test_dispatch_default_off_aggregate_route_has_only_the_celery_writer``)
    has no successor: these are all canonical/independently-plannable
    identities (never aliases), and step 4 deleted the Celery dispatch plane
    outright, so "this pair stays on the Celery writer" is not a state the
    dispatcher can produce for any pair -- see
    ``.remember/chaos-4054-context.md``. The one remaining way a claimed pair
    can fail to reach River is that the matrix does not route it at all, which
    ``test_dispatch_sync_run_non_plannable_alias_pair_never_stages_a_writer`` covers.
    """

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider=provider,
        dataset_key=dataset_key,
        processor_flags=processor_flags,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    if provider == "pagerduty":
        monkeypatch.setattr(
            sync_units,
            "require_canonical_incident_feature_for_update_sync",
            lambda *_args: None,
        )

    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "dispatched",
        "queued_units": 1,
    }
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{unit.id}"}


def test_dispatch_sync_run_non_plannable_alias_pair_never_stages_a_writer(
    db_session, monkeypatch
):
    """An alias identity is route-ready but never plannable, so no runtime
    owns it as a unit of its own. CHAOS-4078 gave the PR-social family
    (prs/pr-reviews/pr-comments) its own fold-family claim validation, and a
    unit persisted directly under a non-canonical dataset_key IS malformed
    per ``validate_provider_family_claim`` (going forward the planner only
    ever mints units under the canonical "prs"/"cicd" identity, folding
    alias-only selections onto it).

    Unlike the work-item family's ATOMIC_CANONICAL equivalent
    (``test_dispatch_sync_run_github_work_item_direct_alias_never_stages_a_writer``,
    which DOES raise), a FOLD_CONTRIBUTING direct-alias claim does not abort
    this run's dispatch: the capability matrix never marks an alias
    plannable, so ``routes_to_river`` fails it closed regardless, and
    CHAOS-3990 pins graceful per-unit termination for a stale or
    pre-fold-legacy alias unit reaching dispatch over crashing the whole run
    (see ``tests/test_disabled_alias_dispatch_strand.py``'s DISABLED_ALIASES
    sweep, which exercises this exact provider/dataset shape). This test
    keeps its CHAOS-4078 name and "never stages a writer" claim -- true under
    either raise or terminate -- while asserting the terminate shape.
    """

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="github",
        source_type="repo",
        dataset_key="pr-comments",
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "noop",
        "queued_units": 0,
    }

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == "feature_disabled"
    assert unit.result["reason"] == (
        "no worker can execute github/pr-comments: the provider capability "
        "matrix does not mark it route-ready and plannable, so no shipped "
        "writer owns it"
    )
    assert db_session.query(WorkerJobOutbox).count() == 0


# These tests were deleted mid-review and then restored, which is worth
# recording. CHAOS-4054 step 4 deletes the Celery DISPATCH plane; it does not
# delete the durable route CONTROL plane, which CHAOS-4082 still owns. The
# first cut of this change conflated the two, dropped
# ``resolve_worker_job_route`` from ``dispatch_sync_run``, and deleted these
# tests as having no subject left. An adversarial review caught the
# consequence: with the read gone the producer stages outbox rows the Go relay
# RELEASES for a Celery route, wedging units in ``dispatching`` behind a
# DispatchGuard slot. The read -- and its FOR SHARE lock, which is what keeps
# an operator rollback's quiescence claim honest -- is back, so these tests
# have a subject again.
#
# What changed is the ``celery`` case. It used to mean "dispatch through the
# Python writer"; it now means "fail closed", because step 4 removed the
# runtime that made it a destination.
@pytest.mark.parametrize("state", ("missing", "paused", "drifted", "rolled-back"))
def test_dispatch_sync_run_route_faults_fail_closed(db_session, monkeypatch, state):
    """A faulty or non-River durable route must refuse, never stage work.

    Staging anyway is strictly worse than refusing: the relay resolves the same
    row every step and releases the claim for a Celery route, so the unit sits
    in ``dispatching`` behind an outbox row nothing will deliver, holding a
    concurrency slot with no lease to expire and no sweep willing to reclaim it
    (the Go sweep's own route fence declines for exactly the same reason).
    """

    from dev_health_ops.jobs.routes import WorkerJobRouteError
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session, provider="github", dataset_key="commits")
    route_row = db_session.query(WorkerJobRoute).filter(
        WorkerJobRoute.job_kind == "sync.provider_unit"
    )
    if state == "missing":
        route_row.delete()
    elif state == "paused":
        route_row.update({WorkerJobRoute.paused: True})
    elif state == "drifted":
        route_row.update({WorkerJobRoute.transport: "shadow"})
    else:
        # The declared rollback route. Legal for the row, and no longer a
        # dispatch destination for provider units.
        route_row.update({WorkerJobRoute.transport: "celery"})
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with pytest.raises(WorkerJobRouteError):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    # The negative control that makes this test about wedging rather than
    # about raising: nothing was claimed and nothing was staged, so no
    # DispatchGuard slot is held and the next pass can still make progress.
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert _outbox_unit_keys(db_session) == set()


def test_dispatch_sync_run_provider_outbox_claim_rolls_back_and_dedupes(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        dataset_key="feature-flags",
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    real_enqueue = sync_units.enqueue_worker_job

    def die_after_staging(*args, **kwargs):
        real_enqueue(*args, **kwargs)
        raise RuntimeError("simulated producer death")

    monkeypatch.setattr(sync_units, "enqueue_worker_job", die_after_staging)
    with pytest.raises(RuntimeError, match="simulated producer death"):
        sync_units.dispatch_sync_run(str(run.id))
    db_session.expire_all()
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert db_session.query(WorkerJobOutbox).count() == 0

    monkeypatch.setattr(sync_units, "enqueue_worker_job", real_enqueue)
    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "dispatched",
        "queued_units": 1,
    }
    unit.updated_at = datetime.now(timezone.utc) - timedelta(hours=1)
    db_session.commit()
    assert sync_units.dispatch_sync_run(str(run.id)) == {
        "status": "dispatched",
        "queued_units": 1,
    }
    assert db_session.query(WorkerJobOutbox).count() == 1


# ---------------------------------------------------------------------------
# CHAOS-3131: matrix-driven per-pair routing coherence.
#
# Readiness must generalize past the single hardcoded
# launchdarkly/feature-flags pair, and one SyncRun can legitimately contain
# both pairs the matrix routes and pairs it does not, decided independently in
# the same dispatch pass -- with run-level status, unit counts, and
# finalization staying coherent across both outcomes.
#
# NOTE: ``test_dispatch_sync_run_plain_river_route_also_reaches_outbox``
# formerly opened this block. It asserted that promoting the durable
# ``sync.provider_unit`` route from ``river_canary`` to plain ``river`` did
# not silently revert every unit to Celery dispatch, and it worked by patching
# ``sync_units.resolve_worker_job_route``. CHAOS-4054 step 4 deleted that call
# from ``dispatch_sync_run`` entirely, so the two durable route values it
# distinguished no longer have distinct behaviour -- and no route value can
# revert a unit to Celery, because the Celery branch is gone. Nothing replaces
# it; that the routable pair reaches the outbox is asserted by every test in
# this block.
# ---------------------------------------------------------------------------


def test_dispatch_sync_run_routes_every_matrix_ready_pair_independently(
    db_session, monkeypatch
):
    """Proves the mechanism generalizes past one route-ready pair: with TWO
    real route-ready+plannable pairs (launchdarkly/feature-flags,
    github/commits) in the SAME run as a pair the checked-in matrix does not
    recognise at all (``synthetic/matrix-incomplete``), a three-unit run
    stages both ready pairs in the outbox and terminalizes the unrecognised
    one, in the same dispatch pass. Each pair is decided on its own; a pair
    the matrix does not route no longer has a second runtime to fall through
    to, so it is failed ``feature_disabled`` rather than published into a
    queue no consumer serves.
    """
    from dev_health_ops.workers import sync_units

    run, launchdarkly_unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        dataset_key="feature-flags",
    )
    github_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=launchdarkly_unit.integration_id,
        source_id=launchdarkly_unit.source_id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True, "sync_commits": True},
    )
    unrouted_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=launchdarkly_unit.integration_id,
        source_id=launchdarkly_unit.source_id,
        provider="synthetic",
        dataset_key="matrix-incomplete",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={},
    )
    run.total_units = 3
    db_session.add_all([github_unit, unrouted_unit])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unrouted_unit)
    assert result == {"status": "dispatched", "queued_units": 2}
    assert _outbox_unit_keys(db_session) == {
        f"sync.provider_unit:{launchdarkly_unit.id}",
        f"sync.provider_unit:{github_unit.id}",
    }
    assert unrouted_unit.status == SyncRunUnitStatus.FAILED.value
    assert unrouted_unit.error == "feature_disabled"


def test_finalize_aggregates_success_across_units_from_either_writer(
    db_session, monkeypatch
):
    """finalize_sync_run reads only SyncRunUnit.status; it must never need to
    know which writer produced a terminal state. One unit's status is set the
    way the Python ``run_sync_unit`` task would leave it; the other's is set
    the way the Go River worker's UnitRepository.Complete would leave it (same
    table, same enum, same result-JSON shape). Finalizing proves the two
    producers stay coherent at the run-level aggregation boundary.
    """
    from dev_health_ops.workers import sync_units

    run, python_unit = _seed_run(db_session, provider="github", dataset_key="commits")
    river_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=python_unit.integration_id,
        source_id=python_unit.source_id,
        provider="launchdarkly",
        dataset_key="feature-flags",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.SUCCESS.value,
        attempts=1,
        result={"go_provider_route": {"effects_written": 4, "effects_skipped": 0}},
    )
    python_unit.status = SyncRunUnitStatus.SUCCESS.value
    run.total_units = 2
    db_session.add(river_unit)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert run.completed_units == 2
    assert run.failed_units == 0


def test_finalize_aggregates_partial_failure_across_units_from_either_writer(
    db_session, monkeypatch
):
    """Same coherence proof as the all-success case, but for the partial
    failure branch: the River-origin unit fails while the Python-origin unit
    succeeds, and the run must still land PARTIAL_FAILED with correct
    per-status counts -- proving finalize's aggregation does not implicitly
    assume every unit in a run was terminalized by the same writer.
    """
    from dev_health_ops.workers import sync_units

    run, python_unit = _seed_run(db_session, provider="github", dataset_key="commits")
    river_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=python_unit.integration_id,
        source_id=python_unit.source_id,
        provider="launchdarkly",
        dataset_key="feature-flags",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.FAILED.value,
        attempts=1,
        error="sync provider canary capability is unavailable",
        result={"error_category": "provider_unit_exhausted"},
    )
    python_unit.status = SyncRunUnitStatus.SUCCESS.value
    run.total_units = 2
    db_session.add(river_unit)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_units == 1
    assert run.failed_units == 1


def test_dispatch_sync_run_denial_fails_planned_units_and_spares_in_flight(
    db_session, monkeypatch
):
    """DispatchGuard denial ('cancel') must fail unresolved PLANNED units and
    leave any unit already in flight alone. Simulates a unit already RUNNING
    (as the Go worker's Claim would leave it: lease_owner + heartbeat set)
    alongside a PLANNED sibling that has not dispatched yet.
    """
    from dev_health_ops.sync.guard import GuardDecision
    from dev_health_ops.workers import sync_units

    run, river_running = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        dataset_key="feature-flags",
    )
    now = datetime.now(timezone.utc)
    river_running.status = SyncRunUnitStatus.RUNNING.value
    river_running.attempts = 1
    river_running.lease_owner = "go-river-worker"
    river_running.lease_expires_at = now + timedelta(minutes=5)
    river_running.last_heartbeat_at = now
    still_planned = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=river_running.integration_id,
        source_id=river_running.source_id,
        provider="github",
        dataset_key="blame",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True, "sync_commits": True},
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 2
    db_session.add(still_planned)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    reason = "sync run cancelled by operator"
    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(
            False, reason, (str(river_running.id), str(still_planned.id))
        ),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(river_running)
    db_session.refresh(still_planned)
    assert result == {
        "status": "denied_active",
        "reason": reason,
        "failed_planned_units": 1,
        "failed_stale_dispatching_units": 0,
    }
    assert still_planned.status == SyncRunUnitStatus.FAILED.value
    # The in-flight River unit is untouched: cancellation must never
    # terminalize a unit a live consumer might still be executing.
    assert river_running.status == SyncRunUnitStatus.RUNNING.value
    # Denial short-circuits before the claim, so nothing is staged.
    assert db_session.query(WorkerJobOutbox).count() == 0


def test_dispatch_sync_run_concurrency_cap_defers_before_routing(
    db_session, monkeypatch
):
    """A concurrency/budget partial-cap defers a unit before the per-pair
    routing decision is ever made: the capped unit is left PLANNED and never
    reaches the matrix check, while its uncapped sibling in the same pass is
    staged normally. Once the cap clears, the very next dispatch pass must
    still route the deferred unit correctly, proving budget caps and routing
    are independent concerns applied in the right order.
    """
    from dev_health_ops.sync.guard import GuardDecision
    from dev_health_ops.workers import sync_units

    run, capped_unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        dataset_key="feature-flags",
    )
    uncapped_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=capped_unit.integration_id,
        source_id=capped_unit.source_id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True, "sync_commits": True},
    )
    run.total_units = 2
    db_session.add(uncapped_unit)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(
            True, "concurrency cap", (str(capped_unit.id),), True
        ),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(capped_unit)
    db_session.refresh(uncapped_unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert capped_unit.status == SyncRunUnitStatus.PLANNED.value
    assert uncapped_unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{uncapped_unit.id}"}

    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(True, None, (), False),
    )
    result_after_cap_clears = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(capped_unit)
    assert result_after_cap_clears["status"] == "dispatched"
    assert capped_unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert _outbox_unit_keys(db_session) == {
        f"sync.provider_unit:{uncapped_unit.id}",
        f"sync.provider_unit:{capped_unit.id}",
    }


def test_dispatch_sync_run_reclaims_stale_units_and_redecides_each_pair(
    db_session, monkeypatch
):
    """A stale DISPATCHING unit means an earlier dispatch staged it but no
    consumer ever picked it up (broker restart, or the producer died before a
    River claim landed -- worker loss). Reclaim must work on a stale unit
    exactly as it does on a never-dispatched one, and the routing decision
    must be re-made fresh per pair on the reclaiming pass rather than reusing
    whatever was decided (and lost) the first time -- here the reclaimed unit
    is re-staged for River while its unroutable sibling (a pair the matrix
    does not know at all) is terminalized in the same pass.

    CHAOS-4078 note: this used to pair the reclaimed unit with a raw
    ``github/pr-comments`` alias-identity sibling. Every alias identity is
    now family-governed (PR-social/TestOps joined the work-item family under
    CHAOS-4078's fold-family claim validation), so a raw alias-keyed unit is a
    MALFORMED claim that raises ``WorkerJobRouteError`` before either pair in
    the run is decided (see
    ``test_dispatch_sync_run_non_plannable_alias_pair_never_stages_a_writer``)
    rather than gracefully terminalizing beside a healthy sibling. This test's
    actual subject -- reclaim + fresh per-pair redecision -- needs a
    genuinely unroutable-but-well-formed pair instead, so it uses an unknown
    provider/dataset the capability matrix has never heard of.
    """
    from dev_health_ops.workers import sync_units

    run, river_unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        dataset_key="feature-flags",
    )
    unroutable_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=river_unit.integration_id,
        source_id=river_unit.source_id,
        provider="acme",
        dataset_key="widgets",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={},
    )
    stale = datetime.now(timezone.utc) - timedelta(minutes=30)
    river_unit.status = SyncRunUnitStatus.DISPATCHING.value
    river_unit.updated_at = stale
    run.total_units = 2
    db_session.add(unroutable_unit)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(river_unit)
    db_session.refresh(unroutable_unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert river_unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{river_unit.id}"}
    assert unroutable_unit.status == SyncRunUnitStatus.FAILED.value
    assert unroutable_unit.error == "feature_disabled"


# NOTE: ``test_local_all_reclaims_stale_complete_writer_alias_to_river_without_celery``
# and ``test_local_all_routes_complete_writer_identity_set_only_to_river``
# formerly here exercised the ``DEV_HEALTH_ENV=local`` / ``GO_PROVIDER_ROUTES=all``
# local-only convenience preset, which made pr-reviews/pr-comments/tests
# aliases ALSO route straight to River in a developer's local shell.
# CHAOS-4054 deleted that whole preset outright, along with every
# ``_LOCAL_ALL_*`` machinery (see ``.remember/chaos-4054-context.md``): an
# alias identity is route-ready but never plannable in the checked-in matrix,
# full stop, with no environment override of any kind -- local or otherwise.
# "every persisted complete-writer identity routes to River in one Go-only
# pass" is therefore a structurally impossible claim to make about an alias
# any more, so these tests have no successor. What happens to an alias
# identity instead -- it is unroutable, so dispatch terminalizes it -- is
# covered by
# ``test_dispatch_sync_run_non_plannable_alias_pair_never_stages_a_writer``, by the
# reclaim test just above, and by the DISABLED_ALIASES sweep in
# ``tests/test_disabled_alias_dispatch_strand.py``.


def test_dispatch_sync_run_continues_accepted_run_after_planner_config_pause(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="paused-planner",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        integration_id=run.integration_id,
        is_active=False,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    db_session.refresh(config)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.error is None
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.error is None
    assert config.last_sync_success is None
    assert config.last_sync_error is None


def test_paused_config_with_stale_dispatching_reclaims_accepted_work(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, stale_dispatching = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    stale_dispatching.status = SyncRunUnitStatus.DISPATCHING.value
    stale_dispatching.updated_at = now - timedelta(minutes=30)
    running = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-live",
        lease_expires_at=now + timedelta(minutes=5),
        last_heartbeat_at=now,
        processor_flags={"sync_prs": True},
    )
    planned = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        # A pair the capability matrix routes: this test is about reclaiming
        # accepted work under a paused config, not about routability, so the
        # third unit must not divert into the unroutable terminalize branch.
        dataset_key="deployments",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_deployments": True},
    )
    config = SyncConfiguration(
        org_id=run.org_id,
        name="paused-with-stale-dispatching",
        provider="github",
        sync_targets=["git", "prs", "deployments"],
        sync_options={},
        integration_id=run.integration_id,
        is_active=False,
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 3
    db_session.add_all([running, planned, config])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls = _patch_worker_enqueues(monkeypatch)

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(stale_dispatching)
    db_session.refresh(running)
    db_session.refresh(planned)
    assert dispatch_result == {"status": "dispatched", "queued_units": 2}
    assert stale_dispatching.status == SyncRunUnitStatus.DISPATCHING.value
    assert stale_dispatching.error is None
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert running.lease_owner == "worker-live"
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert dispatch_calls == []
    assert finalize_calls == []
    assert _outbox_unit_keys(db_session) == {
        f"sync.provider_unit:{stale_dispatching.id}",
        f"sync.provider_unit:{planned.id}",
    }

    running.status = SyncRunUnitStatus.FAILED.value
    running.error = "sync unit lease expired"
    running.result = {"error_category": "worker_lost"}
    running.lease_owner = None
    running.lease_expires_at = None
    running.updated_at = datetime.now(timezone.utc)
    stale_dispatching.status = SyncRunUnitStatus.FAILED.value
    stale_dispatching.error = "provider auth failed"
    stale_dispatching.result = {"error_category": "auth"}
    planned.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()

    finalize_result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert finalize_result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.failed_units == 2


def test_total_cap_hard_deny_with_stale_dispatching_does_not_redispatch(
    db_session, monkeypatch
):
    from dev_health_ops.sync.guard import GuardDecision
    from dev_health_ops.workers import sync_units

    run, stale_dispatching = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    stale_dispatching.status = SyncRunUnitStatus.DISPATCHING.value
    stale_dispatching.updated_at = now - timedelta(minutes=30)
    running = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-live",
        lease_expires_at=now + timedelta(minutes=5),
        last_heartbeat_at=now,
        processor_flags={"sync_prs": True},
    )
    planned = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_issues": True},
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 3
    db_session.add_all([running, planned])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls = _patch_worker_enqueues(monkeypatch)
    reason = "sync run unit cap exceeded: 3/1"
    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(
            False,
            reason,
            (str(stale_dispatching.id), str(running.id), str(planned.id)),
        ),
    )

    def fail_queue(*_args, **_kwargs):
        raise AssertionError("total-cap hard-deny must not stage a provider unit")

    monkeypatch.setattr(sync_units, "enqueue_worker_job", fail_queue)

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(stale_dispatching)
    db_session.refresh(running)
    db_session.refresh(planned)
    assert dispatch_result == {
        "status": "denied_active",
        "reason": reason,
        "failed_planned_units": 1,
        "failed_stale_dispatching_units": 1,
    }
    assert stale_dispatching.status == SyncRunUnitStatus.FAILED.value
    assert stale_dispatching.error == reason
    assert stale_dispatching.result == {"error_category": "dispatch_denied"}
    assert planned.status == SyncRunUnitStatus.FAILED.value
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert dispatch_calls == []
    assert finalize_calls == [((str(run.id),), "sync")]
    assert db_session.query(WorkerJobOutbox).count() == 0


def test_total_cap_hard_deny_terminalizes_linked_job_run(db_session, monkeypatch):
    from dev_health_ops.sync.guard import GuardDecision
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    retrying = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RETRYING.value,
        attempts=1,
        available_at=now + timedelta(minutes=10),
        processor_flags={"sync_issues": True},
    )
    run.total_units = 2
    db_session.add(retrying)
    db_session.flush()
    scheduled = ScheduledJob(
        org_id=run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=uuid.uuid4(),
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    job_run = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.PENDING.value,
    )
    job_run.result = {"sync_run_id": str(run.id)}
    db_session.add(job_run)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    reason = "sync run unit cap exceeded: 1/0"
    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(False, reason, (str(unit.id),)),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(job_run)
    db_session.refresh(unit)
    db_session.refresh(retrying)
    assert result == {
        "status": "denied",
        "reason": reason,
        "failed_planned_units": 2,
    }
    assert run.status == SyncRunStatus.FAILED.value
    # The stranded units cascade to FAILED with the deny reason — a hard-
    # denied run can never legally redispatch them, and the reconciler
    # skips terminal runs, so leaving them PLANNED/RETRYING strands them
    # forever (and blocks any later finalize on the RETRYING one).
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == reason
    assert retrying.status == SyncRunUnitStatus.FAILED.value
    assert retrying.error == reason
    assert run.failed_units == 2
    assert job_run.status == JobRunStatus.FAILED.value
    assert job_run.error == reason
    assert job_run.completed_at is not None
    assert job_run.result["failed_units"] == 2


def test_dispatch_sync_run_continues_accepted_run_after_child_config_pause(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    parent_config = SyncConfiguration(
        org_id=run.org_id,
        name="active-parent",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        integration_id=run.integration_id,
        is_active=True,
    )
    db_session.add(parent_config)
    db_session.flush()
    child_config = SyncConfiguration(
        org_id=run.org_id,
        parent_id=parent_config.id,
        name="paused-child",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        integration_id=run.integration_id,
        source_id=unit.source_id,
        is_active=False,
    )
    db_session.add(child_config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    db_session.refresh(parent_config)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.error is None
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.error is None
    assert parent_config.last_sync_success is None
    assert parent_config.last_sync_error is None


def test_dispatch_sync_run_logs_budget_guard_would_allow(
    db_session, monkeypatch, caplog
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_sync_run.budget_guard_dry_run"
    ]
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert records
    record = records[0]
    assert record.decision == "would_allow"
    assert record.bucket["provider"] == "github"
    assert record.bucket["dimension"] == "rest_core"
    assert record.confidence == "medium"
    assert record.suggested_available_at is None


def test_dispatch_sync_run_logs_budget_guard_would_defer_without_deferring(
    db_session, monkeypatch, caplog
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS",
        json.dumps({"github:rest_core": 1}),
    )
    monkeypatch.setenv("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", "120")

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_sync_run.budget_guard_dry_run"
    ]
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.available_at is None
    assert records
    record = records[0]
    assert record.decision == "would_defer"
    assert record.budget_limit == 1
    assert record.projected_units == 2
    assert record.suggested_available_at is not None


def test_dispatch_sync_run_logs_linear_budget_guard_route_family_dry_run(
    db_session, monkeypatch, caplog
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="linear",
        source_type="team",
        external_id="TEAM",
        name="TEAM",
        full_name="TEAM",
        dataset_key="work-items",
        # CHAOS-4054: every atomic family is validated strictly now (no more
        # switch-gated leniency), so this claim must carry the complete
        # canonical family to reach the budget-guard logging this test
        # actually exercises -- unrelated to which flags are set.
        processor_flags=_GITHUB_WORK_ITEM_FAMILY_FLAGS,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS",
        json.dumps({"linear:graphql_cost:issues": 1, "linear:graphql_cost": 100}),
    )
    monkeypatch.setenv("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", "120")

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_sync_run.budget_guard_dry_run"
    ]
    issue_record = next(record for record in records if record.route_family == "issues")
    team_record = next(record for record in records if record.route_family == "teams")
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.available_at is None
    assert issue_record.bucket["provider"] == "linear"
    assert issue_record.bucket["dimension"] == "graphql_cost"
    assert issue_record.decision == "would_defer"
    assert issue_record.budget_limit == 1
    assert issue_record.projected_units == 5
    assert issue_record.suggested_available_at is not None
    assert team_record.decision == "would_allow"
    assert team_record.budget_limit == 100


def test_dispatch_sync_run_enforces_budget_deferral(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls = _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 1}))
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "120")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    assert result["status"] == "deferred"
    assert result["queued_units"] == 0
    assert run.status == SyncRunStatus.PLANNED.value
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.available_at is not None
    available_at = _aware(unit.available_at)
    assert available_at > datetime.now(timezone.utc) + timedelta(seconds=90)
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferred"
    assert unit.result["budget_guard"][0]["decision"] == "deferred"
    assert dispatch_calls == []
    assert finalize_calls == []
    assert db_session.query(WorkerJobOutbox).count() == 0


def test_dispatch_sync_run_enforces_launchdarkly_budget_deferral(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        external_id="project:default",
        name="default",
        full_name="LaunchDarkly/default",
        dataset_key="feature-flags",
        processor_flags={"sync_feature_flags": True},
    )
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls = _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS",
        json.dumps(
            {"launchdarkly:rest_core": 999, "launchdarkly:rest_core:audit_log": 1}
        ),
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "120")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    assert result["status"] == "deferred"
    assert result["queued_units"] == 0
    assert run.status == SyncRunStatus.PLANNED.value
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.available_at is not None
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferred"
    assert any(
        entry["decision"] == "deferred" and entry["route_family"] == "audit_log"
        for entry in unit.result["budget_guard"]
    )
    assert dispatch_calls == []
    assert finalize_calls == []
    assert db_session.query(WorkerJobOutbox).count() == 0


def test_dispatch_sync_run_budget_reservation_blocks_second_unit(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    second = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=first.integration_id,
        source_id=first.source_id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True},
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 2}))
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(first)
    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    statuses = {first.status, second.status}
    assert statuses == {
        SyncRunUnitStatus.DISPATCHING.value,
        SyncRunUnitStatus.RETRYING.value,
    }
    deferred = first if first.status == SyncRunUnitStatus.RETRYING.value else second
    assert deferred.available_at is not None
    assert deferred.result is not None
    assert deferred.result["error_category"] == "budget_deferred"


def test_dispatch_sync_run_budget_reservation_expires(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    stale_reserved = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        # A pair the capability matrix routes, estimated in the same
        # ``github:rest_core`` bucket the limit below caps: the subject here
        # is the expiry of this unit's stale budget reservation, so it must
        # not divert into the unroutable terminalize branch instead.
        dataset_key="repo-metadata",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
        processor_flags={"sync_repo_metadata": True},
    )
    run.total_units = 2
    db_session.add(stale_reserved)
    db_session.flush()
    stale_reserved.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 2}))

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(stale_reserved)
    assert result == {"status": "dispatched", "queued_units": 2}
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert stale_reserved.status == SyncRunUnitStatus.DISPATCHING.value


def test_dispatch_sync_run_budget_release_after_terminal_unit(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    completed = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.SUCCESS.value,
        attempts=1,
        result={"ok": True},
        processor_flags={"sync_issues": True},
    )
    run.total_units = 2
    db_session.add(completed)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 2}))

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(completed)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert completed.status == SyncRunUnitStatus.SUCCESS.value


def test_dispatch_sync_run_github_budget_route_family_isolates_contents_blob(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, candidate = _seed_run(db_session)
    candidate.dataset_key = "blame"
    candidate.processor_flags = {"sync_blame": True}
    active_files = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=candidate.integration_id,
        source_id=candidate.source_id,
        provider="github",
        dataset_key="files",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
        processor_flags={"sync_files": True},
    )
    run.total_units = 2
    db_session.add(active_files)
    db_session.flush()
    active_files.updated_at = datetime.now(timezone.utc)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS",
        json.dumps({"github:contents_blob:blame": 8, "github:contents_blob": 1}),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(candidate)
    db_session.refresh(active_files)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert candidate.status == SyncRunUnitStatus.DISPATCHING.value
    assert active_files.status == SyncRunUnitStatus.DISPATCHING.value


def test_dispatch_sync_run_does_not_terminalize_when_unit_enqueue_fails(
    db_session, monkeypatch
):
    """A failed enqueue is a producer fault, never a verdict on the work.

    The enqueue that can fail is now the durable outbox staging rather than a
    Celery publish, so the claim and the failed enqueue share one transaction
    and roll back together: the unit goes back to PLANNED for the next pass.
    What must NOT happen either way is terminalization -- neither the run nor
    the unit may be stamped failed because the producer could not enqueue.
    """

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    def enqueue_dies(*args, **kwargs):
        raise RuntimeError("outbox write failed")

    monkeypatch.setattr(sync_units, "enqueue_worker_job", enqueue_dies)

    with pytest.raises(RuntimeError, match="outbox write failed"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.expire_all()
    assert run.status == SyncRunStatus.PLANNED.value
    assert run.completed_at is None
    assert run.error is None
    assert run.result is None
    assert run.failed_units == 0
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert unit.error is None
    assert db_session.query(WorkerJobOutbox).count() == 0


def test_dispatch_sync_run_redispatches_stale_dispatching_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    unit.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result["queued_units"] == 1
    assert _outbox_unit_keys(db_session) == {f"sync.provider_unit:{unit.id}"}


def test_dispatch_sync_run_does_not_reclaim_stale_running_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.updated_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result == {
        "status": "waiting_inflight",
        "queued_units": 0,
        "in_flight_units": 1,
    }
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RUNNING.value


def test_dispatch_sync_run_does_not_reclaim_fresh_running_units(
    db_session, monkeypatch
):
    # A unit that is legitimately still running (fresh updated_at) must NOT be
    # reclaimed, or we would double-execute it.
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.updated_at = datetime.now(timezone.utc)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result == {
        "status": "waiting_inflight",
        "queued_units": 0,
        "in_flight_units": 1,
    }
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RUNNING.value


def test_fail_stale_dispatching_does_not_overwrite_concurrent_claim(tmp_path):
    """Write-time CAS: a stale DISPATCHING row a delayed run_sync_unit has
    concurrently claimed to RUNNING (with a live lease) must NOT be clobbered to
    FAILED by the dispatch-denial stale-fail path.
    """
    from sqlalchemy import update as sa_update

    from dev_health_ops.workers.sync_units import (
        _fail_stale_dispatching_units,
        _stale_dispatch_seconds,
    )

    engine = _file_backed_engine(tmp_path)
    try:
        stale_age = timedelta(seconds=_stale_dispatch_seconds() + 600)
        seeded_at = datetime.now(timezone.utc) - stale_age
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            run_id = run.id
            unit_id = unit.id
            failed_unit = SyncRunUnit(
                org_id=unit.org_id,
                sync_run_id=run.id,
                integration_id=unit.integration_id,
                source_id=unit.source_id,
                provider=unit.provider,
                dataset_key="pull_requests",
                cost_class=unit.cost_class,
                mode=unit.mode,
                since_at=unit.since_at,
                before_at=unit.before_at,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags=unit.processor_flags,
                updated_at=seeded_at,
            )
            run.total_units = 2
            seed_session.add(failed_unit)
            # Explicit updated_at in the SET clause suppresses the column onupdate,
            # so the row is durably STALE DISPATCHING.
            seed_session.execute(
                sa_update(SyncRunUnit)
                .where(SyncRunUnit.id == unit_id)
                .values(
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    updated_at=seeded_at,
                )
                .execution_options(synchronize_session=False)
            )
            seed_session.commit()
            failed_unit_id = failed_unit.id

        # A delayed run_sync_unit atomically claims the SAME stale row
        # DISPATCHING -> RUNNING with a live lease, in an independent session.
        lease_owner = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc)
        lease_expires_at = claimed_at + timedelta(seconds=3600)
        with Session(engine) as claim_session:
            claimed_count = (
                claim_session.query(SyncRunUnit)
                .filter(
                    SyncRunUnit.id == unit_id,
                    SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
                )
                .update(
                    {
                        SyncRunUnit.status: SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.error: None,
                        SyncRunUnit.lease_owner: lease_owner,
                        SyncRunUnit.lease_expires_at: lease_expires_at,
                        SyncRunUnit.last_heartbeat_at: claimed_at,
                        SyncRunUnit.updated_at: claimed_at,
                    },
                    synchronize_session=False,
                )
            )
            assert claimed_count == 1
            claim_session.commit()

        # The dispatch-denial path now runs the stale-fail helper.  The write-time
        # status='dispatching' predicate excludes the now-RUNNING row.
        with Session(engine) as fail_session:
            failed = _fail_stale_dispatching_units(
                fail_session, run_id, "sync dispatch denied"
            )
            fail_session.commit()

        assert failed == 1
        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            stale_unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == failed_unit_id)
                .one()
            )
            assert unit.status == SyncRunUnitStatus.RUNNING.value
            assert unit.lease_owner == lease_owner
            assert unit.lease_expires_at is not None
            assert _aware(unit.lease_expires_at) > datetime.now(timezone.utc)
            assert unit.error is None
            assert unit.result is None
            assert stale_unit.status == SyncRunUnitStatus.FAILED.value
            assert stale_unit.error == "sync dispatch denied"
            assert stale_unit.result == {"error_category": "dispatch_denied"}
    finally:
        engine.dispose()


def test_fail_stale_dispatching_fails_genuinely_stale_unit(tmp_path):
    """Control: a genuinely-stale DISPATCHING unit with no concurrent claim IS
    failed by the write-time CAS.
    """
    from sqlalchemy import update as sa_update

    from dev_health_ops.workers.sync_units import (
        _fail_stale_dispatching_units,
        _stale_dispatch_seconds,
    )

    engine = _file_backed_engine(tmp_path)
    try:
        stale_age = timedelta(seconds=_stale_dispatch_seconds() + 600)
        seeded_at = datetime.now(timezone.utc) - stale_age
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            run_id = run.id
            unit_id = unit.id
            seed_session.execute(
                sa_update(SyncRunUnit)
                .where(SyncRunUnit.id == unit_id)
                .values(
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    updated_at=seeded_at,
                )
                .execution_options(synchronize_session=False)
            )
            seed_session.commit()

        with Session(engine) as fail_session:
            failed = _fail_stale_dispatching_units(
                fail_session, run_id, "sync dispatch denied"
            )
            fail_session.commit()

        assert failed == 1
        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync dispatch denied"
            assert unit.result == {"error_category": "dispatch_denied"}
    finally:
        engine.dispose()


def test_claim_units_does_not_reclaim_concurrently_claimed_running(tmp_path):
    """Write-time CAS in _claim_units stale-reclaim: a stale DISPATCHING row a
    delayed run_sync_unit has concurrently claimed to RUNNING (with a live
    lease) must NOT be reclaimed/requeued. A genuinely-stale DISPATCHING unit
    with no concurrent claim IS reclaimed (returned, updated_at refreshed).
    """
    from sqlalchemy import update as sa_update

    from dev_health_ops.workers.sync_units import (
        _claim_units,
        _stale_dispatch_seconds,
    )

    engine = _file_backed_engine(tmp_path)
    try:
        stale_age = timedelta(seconds=_stale_dispatch_seconds() + 600)
        seeded_at = datetime.now(timezone.utc) - stale_age
        with Session(engine) as seed_session:
            run, reclaimable = _seed_run(seed_session)
            run_id = run.id
            reclaimable_id = reclaimable.id
            # Second unit: stale DISPATCHING that a delayed run_sync_unit will
            # concurrently claim to RUNNING below.
            concurrent = SyncRunUnit(
                org_id=reclaimable.org_id,
                sync_run_id=run.id,
                integration_id=reclaimable.integration_id,
                source_id=reclaimable.source_id,
                provider=reclaimable.provider,
                dataset_key="pull_requests",
                cost_class=reclaimable.cost_class,
                mode=reclaimable.mode,
                since_at=reclaimable.since_at,
                before_at=reclaimable.before_at,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags=reclaimable.processor_flags,
                updated_at=seeded_at,
            )
            run.total_units = 2
            seed_session.add(concurrent)
            # Explicit updated_at in the SET clause suppresses the column
            # onupdate, so both rows are durably STALE DISPATCHING.
            seed_session.execute(
                sa_update(SyncRunUnit)
                .where(SyncRunUnit.id == reclaimable_id)
                .values(
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    updated_at=seeded_at,
                )
                .execution_options(synchronize_session=False)
            )
            seed_session.commit()
            concurrent_id = concurrent.id

        # A delayed run_sync_unit atomically claims the SAME stale row
        # DISPATCHING -> RUNNING with a live lease, in an independent session.
        lease_owner = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc)
        lease_expires_at = claimed_at + timedelta(seconds=3600)
        with Session(engine) as claim_session:
            claimed_count = (
                claim_session.query(SyncRunUnit)
                .filter(
                    SyncRunUnit.id == concurrent_id,
                    SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
                )
                .update(
                    {
                        SyncRunUnit.status: SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.error: None,
                        SyncRunUnit.lease_owner: lease_owner,
                        SyncRunUnit.lease_expires_at: lease_expires_at,
                        SyncRunUnit.last_heartbeat_at: claimed_at,
                        SyncRunUnit.updated_at: claimed_at,
                    },
                    synchronize_session=False,
                )
            )
            assert claimed_count == 1
            claim_session.commit()

        # Now run the stale-reclaim path. The write-time status='dispatching'
        # AND updated_at<=stale_dispatch predicates exclude the now-RUNNING row.
        with Session(engine) as claim_units_session:
            claimed = _claim_units(claim_units_session, run_id)
            claimed_ids = {unit.id for unit in claimed}
            claim_units_session.commit()

        # The concurrently-claimed RUNNING unit was NOT reclaimed/requeued.
        assert concurrent_id not in claimed_ids
        # The genuinely-stale DISPATCHING unit WAS reclaimed.
        assert reclaimable_id in claimed_ids

        with Session(engine) as assert_session:
            running_unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == concurrent_id)
                .one()
            )
            reclaimed_unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == reclaimable_id)
                .one()
            )
            # RUNNING row untouched: lease intact, never reclaimed.
            assert running_unit.status == SyncRunUnitStatus.RUNNING.value
            assert running_unit.lease_owner == lease_owner
            assert running_unit.lease_expires_at is not None
            assert _aware(running_unit.lease_expires_at) > datetime.now(timezone.utc)
            # Reclaimed row stays DISPATCHING with a freshly-refreshed updated_at.
            assert reclaimed_unit.status == SyncRunUnitStatus.DISPATCHING.value
            assert _aware(reclaimed_unit.updated_at) > seeded_at
    finally:
        engine.dispose()


def test_retrying_unit_not_claimed_before_available_at_and_claimed_after(
    tmp_path, monkeypatch
):
    """RETRYING units with a future available_at are skipped; past ones are claimed."""
    from dev_health_ops.workers.sync_units import _claim_units

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            now = datetime.now(timezone.utc)
            unit.status = SyncRunUnitStatus.RETRYING.value
            unit.available_at = now + timedelta(hours=1)  # future — must NOT be claimed
            unit.rate_limit_deferrals = 1
            run_id = run.id
            unit_id = unit.id
            seed_session.commit()

        # --- available_at in the FUTURE: unit must stay RETRYING ---
        with Session(engine) as check_session:
            claimed = _claim_units(check_session, run_id)
            claimed_ids_future = {u.id for u in claimed}
            check_session.commit()

        assert unit_id not in claimed_ids_future, (
            "RETRYING unit with future available_at must not be claimed"
        )

        with Session(engine) as assert_session:
            refreshed = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert refreshed.status == SyncRunUnitStatus.RETRYING.value

        # --- Move available_at into the PAST: unit must now be claimed ---
        with Session(engine) as update_session:
            u = (
                update_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            u.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            update_session.commit()

        with Session(engine) as claim_session:
            claimed_after = _claim_units(claim_session, run_id)
            claimed_ids = {u.id for u in claimed_after}
            claim_session.commit()

        assert unit_id in claimed_ids, (
            "RETRYING unit with past available_at must be claimed to DISPATCHING"
        )

        with Session(engine) as assert_session2:
            refreshed2 = (
                assert_session2.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert refreshed2.status == SyncRunUnitStatus.DISPATCHING.value
    finally:
        engine.dispose()


def test_finalize_checkpoint_carries_family_dataset_audit_metadata(
    db_session, monkeypatch
):
    """CHAOS-2721: the compute checkpoint for a collapsed family unit records the
    enabled family datasets so per-dataset provenance survives the collapse."""
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="linear",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
        dataset_key="work-items",
        processor_flags={
            "family_dataset_work_items": True,
            "family_dataset_work_item_history": True,
        },
    )
    unit.status = SyncRunUnitStatus.SUCCESS.value
    unit.since_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    unit.before_at = datetime(2026, 6, 2, tzinfo=timezone.utc)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "finalized"
    checkpoint = db_session.query(SyncComputeCheckpoint).one()
    assert checkpoint.dataset_key == "work-items"
    assert checkpoint.checkpoint_metadata["legacy_targets"] == ["work-items"]
    assert checkpoint.checkpoint_metadata["family_datasets"] == [
        "work-items",
        "work-item-history",
    ]


def test_heavy_cap_clamp_is_logged_when_overlap_ge_cap(caplog):
    """The clamp must be VISIBLE — a silent widen hides the misconfiguration."""
    import os

    from dev_health_ops.sync.planner import (
        _effective_heavy_max_window_days,
        reset_heavy_cap_clamp_warnings,
    )

    previous = {
        key: os.environ.get(key)
        for key in ("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", "SYNC_WATERMARK_OVERLAP")
    }
    os.environ["SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS"] = "7"
    os.environ["SYNC_WATERMARK_OVERLAP"] = str(9 * 86_400)
    # The warning is emitted once per (cap, overlap) per process; clear the
    # cache so this test does not depend on whether another test warmed it.
    reset_heavy_cap_clamp_warnings()
    try:
        with caplog.at_level(logging.WARNING, logger="dev_health_ops.sync.planner"):
            effective = _effective_heavy_max_window_days()
            repeats = [_effective_heavy_max_window_days() for _ in range(5)]
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    # floor(9 days) + 1 == 10, strictly greater than the 9-day overlap.
    assert effective == 10
    clamped = [
        record
        for record in caplog.records
        if "heavy_window_cap_clamped_below_watermark_overlap" in record.getMessage()
    ]
    assert clamped, "clamping the cap must emit a warning naming both values"
    record = clamped[0]
    assert record.configured_cap_days == 7
    assert record.watermark_overlap_seconds == 9 * 86_400
    assert record.effective_cap_days == 10
    # Deduped: the helper runs once per (source x heavy dataset), so repeating it
    # must NOT re-warn — hundreds of identical lines per plan would bury the
    # signal on a large org. The clamp itself still applies every time.
    assert repeats == [10] * 5
    assert len(clamped) == 1, (
        f"expected exactly one clamp warning for 6 calls, got {len(clamped)}"
    )


def test_heavy_cap_is_not_clamped_when_overlap_is_below_cap():
    """No clamp, and no widening, in the normal configuration."""
    import os

    from dev_health_ops.sync.planner import _effective_heavy_max_window_days

    previous = {
        key: os.environ.get(key)
        for key in ("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", "SYNC_WATERMARK_OVERLAP")
    }
    os.environ["SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS"] = "7"
    os.environ["SYNC_WATERMARK_OVERLAP"] = "3600"
    try:
        assert _effective_heavy_max_window_days() == 7
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_fully_caught_up_plan_finalizes_failed_not_silently_successful(
    db_session, monkeypatch
):
    """Pin the end-to-end outcome of CHAOS-3412's empty-window suppression.

    Suppressing empty/inverted windows means a request whose upper bound is
    ALREADY covered for every enabled dataset plans zero units, and a zero-unit
    run finalizes as FAILED with "No sync units planned" (pre-existing behavior,
    see test_finalize_zero_unit_run_does_not_report_success).

    That is a deliberate trade, not an oversight: the alternative is the old
    behavior, where an inverted window produced a unit that fetched nothing and
    finalized SUCCESS — a false coverage claim. A loud, honest failure beats a
    quiet, wrong success. Asserted here so the outcome is known rather than
    discovered in production. Scheduled runs pass no explicit ``before`` and so
    can never reach this path.
    """
    from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
    from dev_health_ops.sync.watermarks import set_watermark
    from dev_health_ops.workers import sync_units

    _patch_db_session(monkeypatch, db_session)
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="demo",
        config={"initial_sync_depth": 30},
        is_active=True,
    )
    db_session.add(integration)
    db_session.flush()
    db_session.add(
        IntegrationSource(
            org_id=org_id,
            integration_id=integration.id,
            provider="github",
            source_type="repo",
            external_id="full-chaos/dev-health",
            name="dev-health",
            full_name="full-chaos/dev-health",
            metadata_={},
            is_enabled=True,
        )
    )
    db_session.add(
        IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key="commits",
            is_enabled=True,
            options={},
        )
    )
    db_session.flush()

    anchor = datetime.now(timezone.utc)
    set_watermark(
        db_session,
        org_id,
        "full-chaos/dev-health",
        "commits",
        anchor - timedelta(days=2),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=org_id,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
            before=anchor - timedelta(days=10),  # already covered
        ),
    )
    assert plan.total_units == 0

    sync_units.finalize_sync_run(plan.sync_run_id)

    run = db_session.get(SyncRun, plan.sync_run_id)
    assert run is not None
    assert run.status == SyncRunStatus.FAILED.value, (
        "an already-covered request must NOT finalize as a success that "
        "implies coverage was refreshed"
    )
    assert run.error == "No sync units planned"


def test_current_watermark_still_plans_a_unit_for_a_scheduled_shaped_request(
    db_session, monkeypatch
):
    """CHAOS-4159 premise pin: "watermark is current" does NOT plan zero units.

    CHAOS-4159 was filed on the theory that a quiet integration -- one whose
    watermark is already at "now" -- resolves to an empty window, plans zero
    units and is therefore wrongly finalized FAILED. That theory is wrong, and
    this test is here so nobody re-derives it from a red dashboard.

    ``get_watermark_with_overlap`` subtracts ``SYNC_WATERMARK_OVERLAP`` from
    the stored value, so a watermark sitting exactly at ``now`` still resolves
    a window start STRICTLY BEFORE ``now``. A scheduled request passes no
    explicit ``before``, so the window end is ``now``. ``end <= start`` --
    the only condition under which ``_watermark_stamping_window`` suppresses
    the window -- is therefore unreachable on the scheduled path, exactly as
    ``test_fully_caught_up_plan_finalizes_failed_not_silently_successful``
    already notes.

    Zero-unit runs in the wild come from a plan with no admissible work
    (no credential, a disabled target, no enabled dataset), not from a caught-up
    watermark. That is why CHAOS-4159 landed as "preserve the planner's cause"
    rather than "finalize a no-op as SUCCESS": there is no no-op to finalize.
    """
    from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
    from dev_health_ops.sync.watermarks import set_watermark

    _patch_db_session(monkeypatch, db_session)
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="demo",
        config={"initial_sync_depth": 30},
        is_active=True,
    )
    db_session.add(integration)
    db_session.flush()
    db_session.add(
        IntegrationSource(
            org_id=org_id,
            integration_id=integration.id,
            provider="github",
            source_type="repo",
            external_id="full-chaos/dev-health",
            name="dev-health",
            full_name="full-chaos/dev-health",
            metadata_={},
            is_enabled=True,
        )
    )
    db_session.add(
        IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key="commits",
            is_enabled=True,
            options={},
        )
    )
    db_session.flush()

    set_watermark(
        db_session,
        org_id,
        "full-chaos/dev-health",
        "commits",
        datetime.now(timezone.utc),
    )

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=org_id,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="schedule",
        ),
    )

    assert plan.total_units == 1, (
        "a fully caught-up integration on the SCHEDULED path still plans "
        "work; if this ever becomes 0, the premise behind CHAOS-4159 has "
        "become true and the no-op-success question must be reopened"
    )
