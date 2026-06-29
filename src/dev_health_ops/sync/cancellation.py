from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, update
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    BackfillJob,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)

_TERMINAL_RUN_STATUSES = {
    SyncRunStatus.SUCCESS.value,
    SyncRunStatus.PARTIAL_FAILED.value,
    SyncRunStatus.FAILED.value,
}
_TERMINAL_UNIT_STATUSES = {
    SyncRunUnitStatus.SUCCESS.value,
    SyncRunUnitStatus.FAILED.value,
}


@dataclass(frozen=True)
class SyncCancellationResult:
    sync_run_id: str
    status: str
    cancelled_units: int
    cleared_outbox_rows: int
    cancelled_job_runs: int
    cancelled_backfill_jobs: int


def cancel_sync_run(
    session: Session,
    sync_run_id: str | uuid.UUID,
    *,
    org_id: str | None = None,
    reason: str = "cancelled by operator",
) -> SyncCancellationResult | None:
    run_uuid = uuid.UUID(str(sync_run_id))
    query = session.query(SyncRun).filter(SyncRun.id == run_uuid)
    if org_id is not None:
        query = query.filter(SyncRun.org_id == org_id)
    run = query.one_or_none()
    if run is None:
        return None

    if run.status in _TERMINAL_RUN_STATUSES:
        return SyncCancellationResult(
            sync_run_id=str(run.id),
            status="already_terminal",
            cancelled_units=0,
            cleared_outbox_rows=0,
            cancelled_job_runs=0,
            cancelled_backfill_jobs=0,
        )

    now = datetime.now(timezone.utc)
    cancelled_units = _cancel_nonterminal_units(session, run_uuid, reason, now)
    cleared_outbox_rows = _clear_pending_outbox(session, run_uuid)

    run.status = SyncRunStatus.FAILED.value
    run.completed_at = now
    run.error = reason
    run.failed_units = int(run.failed_units or 0) + cancelled_units
    run.result = {
        **(run.result if isinstance(run.result, dict) else {}),
        "cancelled": True,
        "cancelled_at": now.isoformat(),
        "cancel_reason": reason,
        "sync_run_status": SyncRunStatus.FAILED.value,
        "total_units": int(run.total_units or 0),
        "completed_units": int(run.completed_units or 0),
        "failed_units": int(run.failed_units or 0),
    }

    cancelled_job_runs = _cancel_job_runs(session, run, reason, now)
    cancelled_backfill_jobs = _cancel_backfill_jobs(session, run, reason, now)
    session.flush()
    return SyncCancellationResult(
        sync_run_id=str(run.id),
        status="cancelled",
        cancelled_units=cancelled_units,
        cleared_outbox_rows=cleared_outbox_rows,
        cancelled_job_runs=cancelled_job_runs,
        cancelled_backfill_jobs=cancelled_backfill_jobs,
    )


def cancel_sync_run_for_job_run(
    session: Session,
    job_run_id: str | uuid.UUID,
    *,
    org_id: str,
    reason: str = "cancelled by operator",
) -> SyncCancellationResult | None:
    job_run = (
        session.query(JobRun)
        .join(ScheduledJob, ScheduledJob.id == JobRun.job_id)
        .filter(
            JobRun.id == uuid.UUID(str(job_run_id)),
            ScheduledJob.org_id == org_id,
            ScheduledJob.job_type == "sync",
        )
        .one_or_none()
    )
    if job_run is None:
        return None
    sync_run_id = _sync_run_id_from_result(job_run.result)
    if sync_run_id is None:
        return None
    return cancel_sync_run(session, sync_run_id, org_id=org_id, reason=reason)


def cancel_sync_run_for_backfill_job(
    session: Session,
    backfill_job_id: str | uuid.UUID,
    *,
    org_id: str,
    reason: str = "cancelled by operator",
) -> SyncCancellationResult | None:
    job = (
        session.query(BackfillJob)
        .filter(
            BackfillJob.id == uuid.UUID(str(backfill_job_id)),
            BackfillJob.org_id == org_id,
        )
        .one_or_none()
    )
    if job is None:
        return None
    sync_run_id = _sync_run_id_from_backfill_marker(job.celery_task_id)
    if sync_run_id is None:
        return None
    return cancel_sync_run(session, sync_run_id, org_id=org_id, reason=reason)


def _cancel_nonterminal_units(
    session: Session, run_uuid: uuid.UUID, reason: str, now: datetime
) -> int:
    result: Any = session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.not_in(_TERMINAL_UNIT_STATUSES),
        )
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            available_at=None,
            error=reason,
            result={"error_category": "cancelled", "cancelled": True},
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)


def _clear_pending_outbox(session: Session, run_uuid: uuid.UUID) -> int:
    result: Any = session.execute(
        delete(SyncDispatchOutbox).where(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.status == "pending",
        )
    )
    return int(result.rowcount or 0)


def _cancel_job_runs(
    session: Session, run: SyncRun, reason: str, completed_at: datetime
) -> int:
    job_runs = (
        session.query(JobRun)
        .filter(
            JobRun.status.in_({JobRunStatus.PENDING.value, JobRunStatus.RUNNING.value})
        )
        .all()
    )
    cancelled = 0
    for job_run in job_runs:
        result = job_run.result if isinstance(job_run.result, dict) else {}
        if str(result.get("sync_run_id") or "") != str(run.id):
            continue
        job_run.status = JobRunStatus.CANCELLED.value
        job_run.completed_at = completed_at
        job_run.error = reason
        job_run.result = {
            **result,
            **(run.result if isinstance(run.result, dict) else {}),
        }
        if job_run.started_at is not None:
            started_at = job_run.started_at
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=completed_at.tzinfo)
            job_run.duration_seconds = max(
                0, int((completed_at - started_at).total_seconds())
            )
        cancelled += 1
    return cancelled


def _cancel_backfill_jobs(
    session: Session, run: SyncRun, reason: str, completed_at: datetime
) -> int:
    marker = f"sync_run:{run.id}"
    jobs = (
        session.query(BackfillJob)
        .filter(BackfillJob.org_id == str(run.org_id))
        .filter(BackfillJob.celery_task_id.contains(marker))
        .all()
    )
    for job in jobs:
        job.status = "cancelled"
        job.total_chunks = int(run.total_units or 0)
        job.completed_chunks = int(run.completed_units or 0)
        job.failed_chunks = int(run.failed_units or 0)
        job.completed_at = completed_at
        job.error_message = reason
    return len(jobs)


def _sync_run_id_from_result(result: Any) -> str | None:
    if not isinstance(result, dict):
        return None
    value = result.get("sync_run_id")
    if value is None:
        return None
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def _sync_run_id_from_backfill_marker(task_id: str | None) -> str | None:
    marker = "sync_run:"
    raw = str(task_id or "")
    if marker not in raw:
        return None
    candidate = raw.split(marker, 1)[1].split("|", 1)[0]
    try:
        return str(uuid.UUID(candidate))
    except ValueError:
        return None
