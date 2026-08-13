from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

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
from dev_health_ops.reports.export import (
    MAX_REPORT_RUN_EXECUTION_RECLAIMS,
    REPORT_RUN_RECLAIM_EXHAUSTED_CODE,
    ReportRunLeaseActive,
    ReportRunReclaimExhausted,
    fail_report_run,
    persist_report_run,
    renew_report_run,
    start_report_run,
)
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
            stored_report = session.get(SavedReport, report_id)
            stored_job = session.get(ScheduledJob, job_id)
            assert stored_report is not None
            assert stored_job is not None
            first = create_scheduled_report_execution(
                session,
                stored_report,
                stored_job,
                "org-a",
                scheduled_for=scheduled_for,
            )
    with Session(engine) as session:
        with session.begin():
            stored_report = session.get(SavedReport, report_id)
            stored_job = session.get(ScheduledJob, job_id)
            assert stored_report is not None
            assert stored_job is not None
            second = create_scheduled_report_execution(
                session,
                stored_report,
                stored_job,
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
            first_claim = start_report_run(session, trigger.run_id)
            assert first_claim is not None
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.status = ReportRunStatus.FAILED.value
            retry = retry_report_execution(session, trigger.run_id)
            assert retry.run_id == trigger.run_id
            assert retry.created is False
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            assert run.execution_reclaim_count == 0
            retry_claim = start_report_run(session, trigger.run_id)
            assert retry_claim is not None
            assert persist_report_run(
                session,
                trigger.run_id,
                trigger.report_id,
                "# canonical",
                [],
                retry_claim.token,
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
            claim = start_report_run(session, trigger.run_id)
            assert claim is not None
            # SQLite returns timezone-naive values from this fixture; duration
            # accounting is unrelated to notification lease recovery.
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.started_at = None
            assert persist_report_run(
                session,
                trigger.run_id,
                trigger.report_id,
                "# canonical",
                [],
                claim.token,
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


def test_python_run_claim_recovers_crash_and_fences_stale_worker(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            first = start_report_run(session, trigger.run_id)
            assert first is not None
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.execution_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)

            reclaimed = start_report_run(session, trigger.run_id)
            assert reclaimed is not None
            assert reclaimed.reclaimed
            assert reclaimed.token != first.token
            assert not persist_report_run(
                session,
                trigger.run_id,
                trigger.report_id,
                "# stale",
                [],
                first.token,
            )
            assert renew_report_run(session, trigger.run_id, reclaimed.token)
            assert persist_report_run(
                session,
                trigger.run_id,
                trigger.report_id,
                "# recovered",
                [],
                reclaimed.token,
            )


def test_python_live_run_lease_is_not_acknowledged_as_success(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            assert start_report_run(session, trigger.run_id) is not None

            with pytest.raises(ReportRunLeaseActive) as active:
                start_report_run(session, trigger.run_id)
            assert active.value.retry_after_seconds > 0


def test_python_task_redelivers_worker_loss_after_live_lease(engine, monkeypatch):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            assert start_report_run(session, trigger.run_id) is not None

    @contextmanager
    def session_scope():
        with Session(engine) as session:
            yield session

    class RetryRequested(RuntimeError):
        pass

    retry: dict[str, object] = {}

    def request_retry(*, exc, countdown):
        retry.update(exc=exc, countdown=countdown)
        raise RetryRequested

    from dev_health_ops.workers.report_task import execute_saved_report

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        session_scope,
    )
    monkeypatch.setattr(execute_saved_report, "retry", request_retry)
    with pytest.raises(RetryRequested):
        execute_saved_report(trigger.report_id, trigger.run_id)

    assert isinstance(retry["exc"], ReportRunLeaseActive)
    assert 1 <= retry["countdown"] <= 300  # type: ignore[operator]
    assert execute_saved_report.acks_late is True
    assert execute_saved_report.reject_on_worker_lost is True


def test_python_task_counts_durable_expired_lease_outcomes(engine, monkeypatch):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            recovered = create_on_demand_report_execution(session, report.id, "org-a")
            assert start_report_run(session, recovered.run_id) is not None
            recovered_run = session.get(ReportRun, recovered.run_id)
            assert recovered_run is not None
            recovered_run.execution_lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )

            exhausted = create_on_demand_report_execution(session, report.id, "org-a")
            assert start_report_run(session, exhausted.run_id) is not None
            exhausted_run = session.get(ReportRun, exhausted.run_id)
            assert exhausted_run is not None
            exhausted_run.execution_reclaim_count = MAX_REPORT_RUN_EXECUTION_RECLAIMS
            exhausted_run.execution_lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )

    @contextmanager
    def session_scope():
        with Session(engine) as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    async def recovered_result(*_args):
        return SimpleNamespace(rendered_markdown="# recovered", provenance=[])

    from dev_health_ops.metrics.prometheus import REPORT_RUN_LEASE_EXPIRED_TOTAL
    from dev_health_ops.workers.report_task import execute_saved_report

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        session_scope,
    )
    monkeypatch.setattr(
        "dev_health_ops.db.require_clickhouse_uri", lambda: "clickhouse://fake/db"
    )
    monkeypatch.setattr("dev_health_ops.db.reset_async_engines", lambda: None)
    monkeypatch.setattr(
        "dev_health_ops.reports.engine.execute_report", recovered_result
    )
    retrying = REPORT_RUN_LEASE_EXPIRED_TOTAL.labels(result="retrying")
    failed = REPORT_RUN_LEASE_EXPIRED_TOTAL.labels(result="failed")
    retrying_before = retrying._value.get()
    failed_before = failed._value.get()

    result = execute_saved_report(recovered.report_id, recovered.run_id)
    assert result["status"] == "success"
    assert retrying._value.get() == retrying_before + 1

    with pytest.raises(ReportRunReclaimExhausted):
        execute_saved_report(exhausted.report_id, exhausted.run_id)
    assert failed._value.get() == failed_before + 1

    with Session(engine) as session:
        terminal = session.get(ReportRun, exhausted.run_id)
        assert terminal is not None
        assert terminal.status == ReportRunStatus.FAILED.value
        assert terminal.error == REPORT_RUN_RECLAIM_EXHAUSTED_CODE


def test_python_scheduler_redispatches_only_after_running_lease_expires(engine):
    scheduled_for = datetime(2026, 8, 13, 12, tzinfo=UTC)
    with Session(engine) as session:
        with session.begin():
            report, job = _seed(session)
            report_id, job_id = report.id, job.id
            first = create_scheduled_report_execution(
                session,
                report,
                job,
                "org-a",
                scheduled_for=scheduled_for,
            )
            assert start_report_run(session, first.run_id) is not None
            live = create_scheduled_report_execution(
                session,
                report,
                job,
                "org-a",
                scheduled_for=scheduled_for,
            )
            assert live.dispatch_required is False
            run = session.get(ReportRun, first.run_id)
            assert run is not None
            run.execution_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)

    with Session(engine) as session:
        with session.begin():
            stored_report = session.get(SavedReport, report_id)
            stored_job = session.get(ScheduledJob, job_id)
            assert stored_report is not None
            assert stored_job is not None
            expired = create_scheduled_report_execution(
                session,
                stored_report,
                stored_job,
                "org-a",
                scheduled_for=scheduled_for,
            )
            assert expired.created is False
            assert expired.run_id == first.run_id
            assert expired.dispatch_required is True


def test_python_worker_renews_execution_lease_during_long_work(engine, monkeypatch):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            claim = start_report_run(session, trigger.run_id)
            assert claim is not None
            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            initial_expiry = run.execution_lease_expires_at

    @contextmanager
    def session_scope():
        with Session(engine) as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        session_scope,
    )

    async def slow_report(*_args):
        await asyncio.sleep(0.04)
        return "finished"

    from dev_health_ops.workers.report_task import _execute_with_report_run_lease

    result = asyncio.run(
        _execute_with_report_run_lease(
            slow_report,
            object(),
            [],
            "clickhouse://unused",
            trigger.run_id,
            replace(claim, lease_seconds=0.03),
        )
    )
    assert result == "finished"
    with Session(engine) as session:
        run = session.get(ReportRun, trigger.run_id)
        assert run is not None
        assert initial_expiry is not None
        assert run.execution_lease_expires_at is not None
        assert run.execution_lease_expires_at > initial_expiry


def test_python_run_claim_terminalizes_after_bounded_reclaims(engine):
    with Session(engine) as session:
        with session.begin():
            report, _ = _seed(session)
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            assert start_report_run(session, trigger.run_id) is not None
            for expected in range(1, MAX_REPORT_RUN_EXECUTION_RECLAIMS + 1):
                run = session.get(ReportRun, trigger.run_id)
                assert run is not None
                run.execution_lease_expires_at = datetime.now(UTC) - timedelta(
                    seconds=1
                )
                reclaimed = start_report_run(session, trigger.run_id)
                assert reclaimed is not None and reclaimed.reclaimed
                assert run.execution_reclaim_count == expected

            run = session.get(ReportRun, trigger.run_id)
            assert run is not None
            run.execution_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            with pytest.raises(ReportRunReclaimExhausted) as failure:
                start_report_run(session, trigger.run_id)
            assert failure.value.terminalized
            assert run.status == ReportRunStatus.FAILED.value
            assert run.error == REPORT_RUN_RECLAIM_EXHAUSTED_CODE
            assert run.execution_claim_token is None
            assert run.execution_lease_expires_at is None

            with pytest.raises(ReportRunReclaimExhausted) as repeated:
                start_report_run(session, trigger.run_id)
            assert not repeated.value.terminalized


@pytest.mark.parametrize("outcome", ["success", "failed", "exhausted"])
def test_scheduled_terminal_run_invalidates_next_due_marker(engine, outcome):
    scheduled_for = datetime(2026, 8, 13, 12, tzinfo=UTC)
    with Session(engine) as session:
        with session.begin():
            report, job = _seed(session)
            job.next_run_at = scheduled_for + timedelta(days=1)
            job_id = job.id
            trigger = create_scheduled_report_execution(
                session,
                report,
                job,
                "org-a",
                scheduled_for=scheduled_for,
            )
            claim = start_report_run(session, trigger.run_id)
            assert claim is not None
            if outcome == "success":
                assert persist_report_run(
                    session,
                    trigger.run_id,
                    trigger.report_id,
                    "# scheduled",
                    [],
                    claim.token,
                )
            elif outcome == "failed":
                assert fail_report_run(
                    session, trigger.run_id, claim.token, "render_failed"
                )
            else:
                for _ in range(MAX_REPORT_RUN_EXECUTION_RECLAIMS):
                    run = session.get(ReportRun, trigger.run_id)
                    assert run is not None
                    run.execution_lease_expires_at = datetime.now(UTC) - timedelta(
                        seconds=1
                    )
                    assert start_report_run(session, trigger.run_id) is not None
                run = session.get(ReportRun, trigger.run_id)
                assert run is not None
                run.execution_lease_expires_at = datetime.now(UTC) - timedelta(
                    seconds=1
                )
                with pytest.raises(ReportRunReclaimExhausted):
                    start_report_run(session, trigger.run_id)

    with Session(engine) as session:
        stored_job = session.get(ScheduledJob, job_id)
        assert stored_job is not None
        assert stored_job.next_run_at is None


@pytest.mark.parametrize("outcome", ["success", "failed"])
def test_manual_terminal_run_preserves_next_due_marker(engine, outcome):
    marker = datetime(2026, 8, 14, 12, tzinfo=UTC)
    with Session(engine) as session:
        with session.begin():
            report, job = _seed(session)
            job.next_run_at = marker
            job_id = job.id
            trigger = create_on_demand_report_execution(session, report.id, "org-a")
            claim = start_report_run(session, trigger.run_id)
            assert claim is not None
            if outcome == "success":
                assert persist_report_run(
                    session,
                    trigger.run_id,
                    trigger.report_id,
                    "# manual",
                    [],
                    claim.token,
                )
            else:
                assert fail_report_run(
                    session, trigger.run_id, claim.token, "render_failed"
                )

    with Session(engine) as session:
        stored_job = session.get(ScheduledJob, job_id)
        assert stored_job is not None
        assert stored_job.next_run_at is not None
        assert stored_job.next_run_at.replace(tzinfo=UTC) == marker


def test_canceling_scheduled_run_advances_the_schedule_occurrence(engine):
    scheduled_for = datetime(2026, 8, 13, 12, tzinfo=UTC)
    with Session(engine) as session:
        with session.begin():
            report, job = _seed(session)
            job.next_run_at = scheduled_for + timedelta(days=1)
            job_id = job.id
            trigger = create_scheduled_report_execution(
                session,
                report,
                job,
                "org-a",
                scheduled_for=scheduled_for,
            )
            assert cancel_report_execution(session, trigger.run_id)

    with Session(engine) as session:
        stored_report = session.get(SavedReport, trigger.report_id)
        stored_job = session.get(ScheduledJob, job_id)
        assert stored_report is not None
        assert stored_job is not None
        assert stored_report.last_run_at is not None
        assert stored_report.last_run_at.replace(tzinfo=UTC) == scheduled_for
        assert stored_report.last_run_status == ReportRunStatus.CANCELED.value
        assert stored_job.next_run_at is None
