"""Atomic, coexistence-safe report execution triggers.

The durable outbox records a versioned future-Go handoff in the same
transaction as ``ReportRun``. While its migration route remains Celery, the
outbox relay deliberately defers it and the caller publishes the established
Celery task only *after* this transaction commits.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from dev_health_ops.models.reports import (
    ReportRun,
    ReportRunStatus,
    SavedReport,
    ScheduledReportOccurrence,
)
from dev_health_ops.models.settings import ScheduledJob
from dev_health_ops.workers.job_contracts import (
    OnDemandReportExecutionPayload,
    ScheduledReportExecutionPayload,
)
from dev_health_ops.workers.job_outbox import enqueue_worker_job

SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION = "report_scheduler_occurrence_v1"


class ReportExecutionConflictError(RuntimeError):
    """A stable occurrence identity resolved to different report inputs."""


class ReportExecutionIneligibleError(RuntimeError):
    """The authoritative report or schedule is inactive or not owned by its org."""


@dataclass(frozen=True, slots=True)
class ReportExecutionTrigger:
    report_id: str
    run_id: str
    outbox_id: str
    created: bool
    dispatch_required: bool


def scheduled_report_occurrence_identity(
    report_id: str | uuid.UUID, scheduled_for: datetime
) -> str:
    """Return a byte-stable cross-runtime identity for a schedule occurrence."""

    instant = _as_utc(scheduled_for)
    fields = (
        ("identity_version", SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION),
        ("report_id", str(report_id)),
        ("scheduled_for", instant.strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"),
    )
    digest = hashlib.sha256()
    for name, value in fields:
        name_bytes = name.encode()
        value_bytes = value.encode()
        digest.update(str(len(name_bytes)).encode())
        digest.update(b":")
        digest.update(name_bytes)
        digest.update(str(len(value_bytes)).encode())
        digest.update(b":")
        digest.update(value_bytes)
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


def create_on_demand_report_execution(
    session: Session,
    report_id: str | uuid.UUID,
    org_id: str,
    *,
    now: datetime | None = None,
) -> ReportExecutionTrigger:
    """Create a new manual run and its durable deferred handoff atomically."""

    locked = _lock_report(session, report_id, org_id)
    return _create_run_and_handoff(
        session,
        locked,
        triggered_by="api",
        scheduled_for=now or datetime.now(UTC),
        occurrence=None,
    )


def create_scheduled_report_execution(
    session: Session,
    report: SavedReport,
    job: ScheduledJob,
    org_id: str,
    *,
    scheduled_for: datetime,
) -> ReportExecutionTrigger:
    """Insert-or-verify exactly one run and outbox row for one due occurrence."""

    if scheduled_for.tzinfo is None or scheduled_for.utcoffset() is None:
        raise ReportExecutionConflictError(
            "report schedule time must be timezone-aware"
        )
    scheduled_for = _as_utc(scheduled_for)
    locked = _lock_report(session, report.id, org_id)
    _require_schedule(locked, job, org_id)
    occurrence_id = scheduled_report_occurrence_identity(locked.id, scheduled_for)
    occurrence = (
        session.query(ScheduledReportOccurrence)
        .filter(ScheduledReportOccurrence.occurrence_id == occurrence_id)
        .with_for_update()
        .one_or_none()
    )
    if occurrence is not None:
        _verify_occurrence(occurrence, locked, job, org_id, scheduled_for)
        if occurrence.report_run_id is not None:
            return _existing_trigger(session, occurrence)
    else:
        occurrence = ScheduledReportOccurrence(
            occurrence_id=occurrence_id,
            identity_version=SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION,
            org_id=org_id,
            report_id=uuid.UUID(str(locked.id)),
            scheduled_job_id=uuid.UUID(str(job.id)),
            scheduled_for=scheduled_for,
        )
        session.add(occurrence)
        session.flush()

    trigger = _create_run_and_handoff(
        session,
        locked,
        triggered_by="scheduler",
        scheduled_for=scheduled_for,
        occurrence=occurrence,
    )
    occurrence.report_run_id = uuid.UUID(trigger.run_id)
    session.flush()
    return trigger


def retry_report_execution(session: Session, run_id: str) -> ReportExecutionTrigger:
    """Requeue the same failed run; no retry is allowed to allocate a new artifact."""

    run = (
        session.query(ReportRun)
        .filter(ReportRun.id == uuid.UUID(run_id))
        .with_for_update()
        .one_or_none()
    )
    if run is None or run.status != ReportRunStatus.FAILED.value:
        raise ReportExecutionIneligibleError("report run is not retryable")
    report = _lock_report(session, run.report_id, None)
    run.status = ReportRunStatus.PENDING.value
    run.error = None
    run.error_traceback = None
    payload = _payload_for_run(run, report)
    row = _enqueue_run(session, run, payload, scheduled_for=run.created_at)
    session.flush()
    return ReportExecutionTrigger(str(report.id), str(run.id), str(row.id), False, True)


def cancel_report_execution(session: Session, run_id: str) -> bool:
    """Cancel only non-terminal work; the ReportRun remains the state authority."""

    run = (
        session.query(ReportRun)
        .filter(ReportRun.id == uuid.UUID(run_id))
        .with_for_update()
        .one_or_none()
    )
    if run is None:
        return False
    if run.status == ReportRunStatus.CANCELED.value:
        return True
    if run.status == ReportRunStatus.SUCCESS.value:
        return False
    now = datetime.now(UTC)
    run.status = ReportRunStatus.CANCELED.value
    run.completed_at = now
    if run.started_at is not None:
        run.duration_seconds = max(0.0, (now - _as_utc(run.started_at)).total_seconds())
    session.flush()
    return True


def _create_run_and_handoff(
    session: Session,
    report: SavedReport,
    *,
    triggered_by: str,
    scheduled_for: datetime,
    occurrence: ScheduledReportOccurrence | None,
) -> ReportExecutionTrigger:
    run = ReportRun(
        report_id=uuid.UUID(str(report.id)),
        triggered_by=triggered_by,
        status=ReportRunStatus.PENDING.value,
    )
    if occurrence is not None:
        run.scheduled_occurrence_id = occurrence.occurrence_id
    session.add(run)
    session.flush()
    payload = _payload_for_run(run, report)
    row = _enqueue_run(session, run, payload, scheduled_for=scheduled_for)
    session.flush()
    return ReportExecutionTrigger(str(report.id), str(run.id), str(row.id), True, True)


def _enqueue_run(
    session: Session,
    run: ReportRun,
    payload: OnDemandReportExecutionPayload | ScheduledReportExecutionPayload,
    *,
    scheduled_for: datetime,
):
    run_id = str(run.id)
    return enqueue_worker_job(
        session,
        payload,
        correlation_id=f"report-run:{run_id}",
        idempotency_key=f"report.run:{run_id}",
        domain_id=run_id,
        scheduled_at=_as_utc(scheduled_for),
        allow_deferred_route=True,
    )


def _payload_for_run(
    run: ReportRun, report: SavedReport
) -> OnDemandReportExecutionPayload | ScheduledReportExecutionPayload:
    if run.triggered_by == "scheduler":
        return ScheduledReportExecutionPayload(report_id=str(report.id))
    return OnDemandReportExecutionPayload(report_id=str(report.id))


def _lock_report(
    session: Session, report_id: object, org_id: str | None
) -> SavedReport:
    query = session.query(SavedReport).filter(
        SavedReport.id == uuid.UUID(str(report_id))
    )
    if org_id is not None:
        query = query.filter(SavedReport.org_id == org_id)
    report = query.with_for_update().one_or_none()
    if report is None or not report.is_active:
        raise ReportExecutionIneligibleError("report is inactive or unavailable")
    return report


def _require_schedule(report: SavedReport, job: ScheduledJob, org_id: str) -> None:
    if (
        report.org_id != org_id
        or report.schedule_id != job.id
        or job.org_id != org_id
        or job.job_type != "report"
    ):
        raise ReportExecutionIneligibleError("scheduled report does not own this job")


def _verify_occurrence(
    occurrence: ScheduledReportOccurrence,
    report: SavedReport,
    job: ScheduledJob,
    org_id: str,
    scheduled_for: datetime,
) -> None:
    if (
        occurrence.identity_version != SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION
        or occurrence.org_id != org_id
        or occurrence.report_id != report.id
        or occurrence.scheduled_job_id != job.id
        or _as_utc(occurrence.scheduled_for) != scheduled_for
    ):
        raise ReportExecutionConflictError("scheduled report occurrence conflicts")


def _existing_trigger(
    session: Session, occurrence: ScheduledReportOccurrence
) -> ReportExecutionTrigger:
    run = session.get(ReportRun, occurrence.report_run_id)
    if run is None:
        raise ReportExecutionConflictError("scheduled report occurrence has no run")
    from dev_health_ops.models.worker_job_outbox import WorkerJobOutbox

    row = (
        session.query(WorkerJobOutbox)
        .filter(WorkerJobOutbox.dedupe_key == f"report.run:{run.id}")
        .one_or_none()
    )
    if row is None:
        raise ReportExecutionConflictError("scheduled report occurrence has no handoff")
    return ReportExecutionTrigger(
        str(run.report_id),
        str(run.id),
        str(row.id),
        False,
        run.status == ReportRunStatus.PENDING.value,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        # SQLite test fixtures do not round-trip tzinfo for DateTime(timezone=True).
        # Persisted report timestamps are UTC, so restore that invariant at the
        # storage boundary while public scheduling inputs remain strict above.
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
