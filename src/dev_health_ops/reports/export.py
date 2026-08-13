from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from dev_health_ops.models.reports import (
    ReportRun,
    ReportRunStatus,
    SavedReport,
    ScheduledReportOccurrence,
)
from dev_health_ops.models.settings import ScheduledJob

REPORT_RUN_EXECUTION_LEASE = timedelta(minutes=5)
MAX_REPORT_RUN_EXECUTION_RECLAIMS = 2
REPORT_RUN_RECLAIM_EXHAUSTED_CODE = "report_run_execution_reclaim_exhausted"


class ReportRunReclaimExhausted(RuntimeError):
    """The durable expired-running reclaim budget is spent."""

    def __init__(self, *, terminalized: bool):
        super().__init__(REPORT_RUN_RECLAIM_EXHAUSTED_CODE)
        self.terminalized = terminalized


class ReportRunLeaseActive(RuntimeError):
    """Another worker owns a report execution lease that has not expired."""

    def __init__(self, retry_after: timedelta):
        super().__init__("report run execution lease is active")
        self.retry_after_seconds = max(0.001, retry_after.total_seconds())


@dataclass(frozen=True)
class ReportRunExecutionClaim:
    token: uuid.UUID
    lease_seconds: float
    reclaimed: bool


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def persist_report_run(
    session: Session,
    run_id: str,
    report_id: str,
    rendered_markdown: str,
    provenance: list[dict] | None = None,
    claim_token: str | uuid.UUID | None = None,
) -> bool:
    """Persist one immutable rendered artifact, returning whether this call won.

    ``ReportRun`` is authoritative across Celery retries and the dormant Go
    handoff. A retry may observe an already-completed identical artifact, but
    it may never replace an artifact or revive a canceled run.
    """

    run = session.execute(
        select(ReportRun).where(ReportRun.id == run_id).with_for_update()
    ).scalar_one()
    fingerprint = "sha256:" + hashlib.sha256(rendered_markdown.encode()).hexdigest()
    if run.status == ReportRunStatus.CANCELED.value:
        return False
    if run.status == ReportRunStatus.SUCCESS.value:
        if run.artifact_fingerprint != fingerprint:
            raise RuntimeError("report artifact conflicts with completed run")
        return False
    if run.status != ReportRunStatus.RUNNING.value or claim_token is None:
        return False

    now = datetime.now(timezone.utc)
    token = uuid.UUID(str(claim_token))
    if (
        run.execution_claim_token != token
        or run.execution_lease_expires_at is None
        or _as_utc(run.execution_lease_expires_at) <= now
    ):
        return False
    run.status = ReportRunStatus.SUCCESS.value
    run.completed_at = now
    if run.started_at:
        run.duration_seconds = (now - _as_utc(run.started_at)).total_seconds()
    run.rendered_markdown = rendered_markdown
    run.provenance_records = provenance or []
    run.artifact_fingerprint = fingerprint
    run.notification_key = f"report.ready:{run.id}"
    run.notification_status = "pending"
    run.notification_claim_token = None
    run.notification_lease_expires_at = None
    run.execution_claim_token = None
    run.execution_lease_expires_at = None

    _advance_saved_report(session, run, now, ReportRunStatus.SUCCESS.value)
    return True


def start_report_run(session: Session, run_id: str) -> ReportRunExecutionClaim | None:
    """Claim new work or replace one expired running holder under a fence."""

    run = session.execute(
        select(ReportRun).where(ReportRun.id == run_id).with_for_update()
    ).scalar_one_or_none()
    if run is None:
        return None

    now = datetime.now(timezone.utc)
    reclaimed = False
    if run.status == ReportRunStatus.RUNNING.value:
        if (
            run.execution_lease_expires_at is not None
            and _as_utc(run.execution_lease_expires_at) > now
        ):
            raise ReportRunLeaseActive(_as_utc(run.execution_lease_expires_at) - now)
        if run.execution_reclaim_count >= MAX_REPORT_RUN_EXECUTION_RECLAIMS:
            _terminalize_reclaim_exhaustion(session, run, now)
            raise ReportRunReclaimExhausted(terminalized=True)
        run.execution_reclaim_count += 1
        reclaimed = True
    elif run.status in {
        ReportRunStatus.PENDING.value,
        ReportRunStatus.FAILED.value,
    }:
        if (
            run.status == ReportRunStatus.FAILED.value
            and run.error == REPORT_RUN_RECLAIM_EXHAUSTED_CODE
        ):
            raise ReportRunReclaimExhausted(terminalized=False)
    else:
        return None

    token = uuid.uuid4()
    run.status = ReportRunStatus.RUNNING.value
    run.started_at = now
    run.completed_at = None
    run.duration_seconds = None
    run.error = None
    run.error_traceback = None
    run.attempt_count += 1
    run.execution_claim_token = token
    run.execution_lease_expires_at = now + REPORT_RUN_EXECUTION_LEASE
    return ReportRunExecutionClaim(
        token=token,
        lease_seconds=REPORT_RUN_EXECUTION_LEASE.total_seconds(),
        reclaimed=reclaimed,
    )


def renew_report_run(
    session: Session,
    run_id: str,
    claim_token: str | uuid.UUID,
) -> bool:
    """Extend only the current live execution fence."""

    run = session.execute(
        select(ReportRun).where(ReportRun.id == run_id).with_for_update()
    ).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    token = uuid.UUID(str(claim_token))
    if (
        run is None
        or run.status != ReportRunStatus.RUNNING.value
        or run.execution_claim_token != token
        or run.execution_lease_expires_at is None
        or _as_utc(run.execution_lease_expires_at) <= now
    ):
        return False
    run.execution_lease_expires_at = now + REPORT_RUN_EXECUTION_LEASE
    return True


def fail_report_run(
    session: Session,
    run_id: str,
    claim_token: str | uuid.UUID,
    error: str,
    error_traceback: str | None = None,
) -> bool:
    """Persist failure only while this worker owns a live execution fence."""

    run = session.execute(
        select(ReportRun).where(ReportRun.id == run_id).with_for_update()
    ).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    token = uuid.UUID(str(claim_token))
    if (
        run is None
        or run.status != ReportRunStatus.RUNNING.value
        or run.execution_claim_token != token
        or run.execution_lease_expires_at is None
        or _as_utc(run.execution_lease_expires_at) <= now
    ):
        return False
    run.status = ReportRunStatus.FAILED.value
    run.completed_at = now
    if run.started_at is not None:
        run.duration_seconds = max(0.0, (now - _as_utc(run.started_at)).total_seconds())
    run.error = error
    run.error_traceback = error_traceback
    run.execution_claim_token = None
    run.execution_lease_expires_at = None
    _advance_saved_report(session, run, now, ReportRunStatus.FAILED.value)
    return True


def _terminalize_reclaim_exhaustion(
    session: Session, run: ReportRun, now: datetime
) -> None:
    run.status = ReportRunStatus.FAILED.value
    run.completed_at = now
    if run.started_at is not None:
        run.duration_seconds = max(0.0, (now - _as_utc(run.started_at)).total_seconds())
    run.error = REPORT_RUN_RECLAIM_EXHAUSTED_CODE
    run.error_traceback = None
    run.execution_claim_token = None
    run.execution_lease_expires_at = None
    _advance_saved_report(session, run, now, ReportRunStatus.FAILED.value)


def _advance_saved_report(
    session: Session,
    run: ReportRun,
    at: datetime,
    status: str,
) -> None:
    occurrence = None
    if run.scheduled_occurrence_id is not None:
        occurrence = session.execute(
            select(ScheduledReportOccurrence)
            .where(
                ScheduledReportOccurrence.occurrence_id == run.scheduled_occurrence_id
            )
            .with_for_update()
        ).scalar_one_or_none()
    report = session.execute(
        select(SavedReport).where(SavedReport.id == run.report_id).with_for_update()
    ).scalar_one()
    report.last_run_at = at
    report.last_run_status = status
    report.updated_at = at
    if occurrence is None:
        return
    job = session.execute(
        select(ScheduledJob)
        .where(ScheduledJob.id == occurrence.scheduled_job_id)
        .with_for_update()
    ).scalar_one_or_none()
    if job is not None:
        job.next_run_at = None
        job.updated_at = at
