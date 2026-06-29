from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
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
    SyncConfiguration,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.sync.cancellation import cancel_sync_run
from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_DISPATCH
from dev_health_ops.workers.sync_units import sync_observers_for_terminal_sync_run


def test_cancel_sync_run_clears_leases_outbox_and_activity_rows():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            run, unit, job_run, backfill_job = _seed_running_backfill(session)
            session.add(
                SyncDispatchOutbox(
                    org_id=run.org_id,
                    sync_run_id=run.id,
                    kind=OUTBOX_KIND_DISPATCH,
                    status="pending",
                    available_at=datetime.now(timezone.utc),
                )
            )
            session.add(
                SyncWatermark(
                    org_id=run.org_id,
                    repo_id="linear-team",
                    source_id="linear-team",
                    target="work-items",
                    dataset_key="work_items",
                    last_synced_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                )
            )
            session.flush()

            result = cancel_sync_run(session, run.id, org_id=run.org_id)
            session.commit()

            assert result is not None
            assert result.status == "cancelled"
            assert result.cancelled_units == 1
            assert result.cleared_outbox_rows == 1
            session.refresh(run)
            session.refresh(unit)
            session.refresh(job_run)
            session.refresh(backfill_job)
            assert run.status == SyncRunStatus.FAILED.value
            assert run.result is not None
            assert run.result["cancelled"] is True
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.lease_owner is None
            assert unit.lease_expires_at is None
            assert unit.result is not None
            assert unit.result["error_category"] == "cancelled"
            assert job_run.status == JobRunStatus.CANCELLED.value
            assert backfill_job.status == "cancelled"
            assert session.query(SyncDispatchOutbox).count() == 0
            watermark = session.query(SyncWatermark).one()
            assert watermark.last_synced_at == datetime(2024, 1, 1)
    finally:
        engine.dispose()


def test_terminal_observer_preserves_cancelled_job_status():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            run, _unit, job_run, backfill_job = _seed_running_backfill(session)
            run.status = SyncRunStatus.FAILED.value
            run.completed_at = datetime.now(timezone.utc)
            run.error = "cancelled by operator"
            run.result = {"cancelled": True}

            sync_observers_for_terminal_sync_run(session, run)
            session.commit()

            session.refresh(job_run)
            session.refresh(backfill_job)
            assert job_run.status == JobRunStatus.CANCELLED.value
            assert backfill_job.status == "cancelled"
    finally:
        engine.dispose()


def _seed_running_backfill(session: Session):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="linear",
        name="linear",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="linear",
        source_type="team",
        external_id="linear-team",
        name="Linear Team",
        full_name="Linear Team",
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key="work_items",
        is_enabled=True,
        options={},
    )
    config = SyncConfiguration(
        name="linear",
        provider="linear",
        org_id=org_id,
        sync_targets=["work-items"],
        integration_id=integration.id,
    )
    session.add_all([source, dataset, config])
    session.flush()
    scheduled_job = ScheduledJob(
        name=f"sync-config-{config.id}",
        job_type="sync",
        schedule_cron="0 * * * *",
        org_id=org_id,
        provider="linear",
        job_config={"sync_config_id": str(config.id)},
        sync_config_id=config.id,
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="backfill",
        mode=SyncRunMode.BACKFILL.value,
        status=SyncRunStatus.RUNNING.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
        started_at=datetime.now(timezone.utc),
    )
    session.add_all([scheduled_job, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="linear",
        dataset_key="work_items",
        cost_class="medium",
        mode=SyncRunMode.BACKFILL.value,
        since_at=None,
        before_at=datetime.now(timezone.utc),
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-1",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        processor_flags={"sync_work_items": True},
    )
    job_run = JobRun(
        job_id=scheduled_job.id,
        triggered_by="backfill",
        status=JobRunStatus.RUNNING.value,
    )
    job_run.started_at = datetime.now(timezone.utc)
    job_run.result = {"sync_run_id": str(run.id)}
    backfill_job = BackfillJob(
        org_id=org_id,
        sync_config_id=config.id,
        celery_task_id=f"celery-task|sync_run:{run.id}",
        status="running",
        since_date=date(2024, 1, 1),
        before_date=date(2024, 1, 31),
    )
    session.add_all([unit, job_run, backfill_job])
    session.flush()
    return run, unit, job_run, backfill_job
