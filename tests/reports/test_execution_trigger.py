from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.reports import (
    ReportRun,
    ReportRunStatus,
    SavedReport,
    ScheduledReportOccurrence,
)
from dev_health_ops.models.settings import JobStatus, ScheduledJob
from dev_health_ops.models.worker_job_outbox import WorkerJobOutbox
from dev_health_ops.reports.execution_trigger import (
    SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION,
    cancel_report_execution,
    create_on_demand_report_execution,
    create_scheduled_report_execution,
    retry_report_execution,
    scheduled_report_occurrence_identity,
)
from dev_health_ops.reports.export import persist_report_run, start_report_run
from dev_health_ops.reports.notifications import (
    claim_report_notification,
    complete_report_notification,
    release_report_notification,
)


@pytest.fixture
def engine(tmp_path):
    value = create_engine(f"sqlite:///{tmp_path / 'reports.db'}")
    Base.metadata.create_all(value)
    try:
        yield value
    finally:
        value.dispose()


def _seed(session: Session) -> tuple[SavedReport, ScheduledJob]:
    report = SavedReport(name="Weekly", org_id="org-a")
    session.add(report)
    session.flush()
    job = ScheduledJob(
        name="report-weekly",
        job_type="report",
        schedule_cron="0 * * * *",
        org_id="org-a",
        status=JobStatus.ACTIVE.value,
        job_config={"report_id": str(report.id)},
    )
    session.add(job)
    session.flush()
    report.schedule_id = job.id
    session.flush()
    return report, job


def test_on_demand_run_and_deferred_handoff_rollback_together(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            report_id = report.id

    with Session(engine) as session:
        transaction = session.begin()
        create_on_demand_report_execution(session, report_id, "org-a")
        transaction.rollback()

    with Session(engine) as session:
        assert session.scalar(select(ReportRun)) is None
        assert session.scalar(select(WorkerJobOutbox)) is None


def test_schedule_occurrence_reuses_one_run_and_one_durable_handoff(engine):
    scheduled_for = datetime(2026, 7, 23, 12, tzinfo=UTC)
    with Session(engine) as session:
        with session.begin():
            report, job = _seed(session)
            report_id, job_id = report.id, job.id

    with Session(engine) as session:
        with session.begin():
            first = create_scheduled_report_execution(
                session,
                session.get(SavedReport, report_id),  # type: ignore[arg-type]
                session.get(ScheduledJob, job_id),  # type: ignore[arg-type]
                "org-a",
                scheduled_for=scheduled_for,
            )
    with Session(engine) as session:
        with session.begin():
            second = create_scheduled_report_execution(
                session,
                session.get(SavedReport, report_id),  # type: ignore[arg-type]
                session.get(ScheduledJob, job_id),  # type: ignore[arg-type]
                "org-a",
                scheduled_for=scheduled_for,
            )

    assert first.created is True
    assert second.created is False
    assert second.dispatch_required is True
    assert second.run_id == first.run_id
    with Session(engine) as session:
        occurrence = session.scalar(select(ScheduledReportOccurrence))
        assert occurrence is not None
        assert (
            occurrence.identity_version == SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION
        )
        assert occurrence.occurrence_id == scheduled_report_occurrence_identity(
            report_id, scheduled_for
        )
        assert len(session.scalars(select(ReportRun)).all()) == 1
        assert len(session.scalars(select(WorkerJobOutbox)).all()) == 1


def test_retry_preserves_artifact_and_notification_identity(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")

    with Session(engine) as session:
        with session.begin():
            assert start_report_run(session, trigger.run_id)
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.status = ReportRunStatus.FAILED.value
            retry = retry_report_execution(session, trigger.run_id)
            assert retry.run_id == trigger.run_id
            assert retry.created is False
            assert start_report_run(session, trigger.run_id)
            assert persist_report_run(
                session, trigger.run_id, trigger.report_id, "# canonical", []
            )
            claimed = claim_report_notification(session, trigger.run_id)
            assert claimed is not None
            assert complete_report_notification(session, trigger.run_id, claimed[2])

    with Session(engine) as session:
        with session.begin():
            assert not persist_report_run(
                session, trigger.run_id, trigger.report_id, "# canonical", []
            )
            assert claim_report_notification(session, trigger.run_id) is None
            assert len(session.scalars(select(WorkerJobOutbox)).all()) == 1


def test_notification_claim_recovers_after_crash_and_fences_stale_worker(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            assert start_report_run(session, trigger.run_id)
            # SQLite returns timezone-naive values from this fixture; duration
            # accounting is unrelated to notification lease recovery.
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.started_at = None
            assert persist_report_run(
                session, trigger.run_id, trigger.report_id, "# canonical", []
            )
            first = claim_report_notification(session, trigger.run_id)
            assert first is not None
            assert claim_report_notification(session, trigger.run_id) is None

            # Simulate a worker death after pending -> delivering. The next
            # attempt may reclaim only after the durable lease expires.
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.notification_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            reclaimed = claim_report_notification(session, trigger.run_id)
            assert reclaimed is not None
            assert reclaimed[2] != first[2]
            assert not complete_report_notification(session, trigger.run_id, first[2])
            assert not release_report_notification(session, trigger.run_id, first[2])
            assert complete_report_notification(session, trigger.run_id, reclaimed[2])
            assert claim_report_notification(session, trigger.run_id) is None


def test_canceled_run_cannot_be_rendered_or_retried(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            assert cancel_report_execution(session, trigger.run_id)
            assert not start_report_run(session, trigger.run_id)
            assert not persist_report_run(
                session, trigger.run_id, trigger.report_id, "# ignored", []
            )

    with Session(engine) as session:
        run = session.get(ReportRun, trigger.run_id)
        assert run is not None
        assert run.status == ReportRunStatus.CANCELED.value
