from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, update

from dev_health_ops.models import (
    BackfillJob,
    JobRun,
    JobRunStatus,
    SyncRun,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.guard import _acquire_bucket_advisory_locks
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    queue="sync", name="dev_health_ops.workers.tasks.reconcile_sync_dispatch"
)
def reconcile_sync_dispatch(limit: int = 100) -> dict[str, Any]:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.workers.sync_units import (
        _stale_dispatch_seconds,
        dispatch_sync_run,
        finalize_sync_run,
        sync_observers_for_terminal_sync_run,
    )

    now = datetime.now(timezone.utc)
    stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds())
    with get_postgres_session_sync() as session:
        expired_units = (
            session.query(SyncRunUnit)
            .filter(
                SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                SyncRunUnit.lease_owner.is_not(None),
                SyncRunUnit.lease_expires_at.is_not(None),
                SyncRunUnit.lease_expires_at <= now,
            )
            .order_by(SyncRunUnit.lease_expires_at.asc(), SyncRunUnit.id.asc())
            .limit(max(1, int(limit)))
            .all()
        )
        buckets = sorted(
            {
                (str(unit.org_id), str(unit.provider), str(unit.cost_class))
                for unit in expired_units
            }
        )
        _acquire_bucket_advisory_locks(session, buckets)
        expired_run_ids: set[uuid.UUID] = set()
        expired_count = 0
        for unit in expired_units:
            observed_lease_owner = unit.lease_owner
            result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == unit.id,
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == observed_lease_owner,
                    SyncRunUnit.lease_owner.is_not(None),
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at <= now,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.FAILED.value,
                    error="sync unit lease expired",
                    result={
                        "error_category": "worker_lost",
                        "lease_expired_at": now.isoformat(),
                    },
                    updated_at=now,
                    lease_owner=None,
                    lease_expires_at=None,
                )
                .execution_options(synchronize_session=False)
            )
            if int(result.rowcount or 0) > 0:
                expired_count += 1
                expired_run_ids.add(unit.sync_run_id)
        session.flush()
        session.expire_all()
        repaired_observers = 0
        for run in _terminal_runs_with_stale_observers(session, limit):
            sync_observers_for_terminal_sync_run(session, run)
            repaired_observers += 1
        session.flush()
        dispatch_run_ids = _dispatchable_run_ids(session, stale_dispatch_cutoff, limit)
        finalize_run_ids = _finalizable_run_ids(session, limit)
        for run_id in expired_run_ids:
            units = (
                session.query(SyncRunUnit)
                .filter(SyncRunUnit.sync_run_id == run_id)
                .all()
            )
            has_dispatchable = any(
                unit.status == SyncRunUnitStatus.PLANNED.value
                or (
                    unit.status == SyncRunUnitStatus.DISPATCHING.value
                    and _as_aware(unit.updated_at) <= stale_dispatch_cutoff
                )
                for unit in units
            )
            all_terminal = all(
                unit.status
                in {SyncRunUnitStatus.SUCCESS.value, SyncRunUnitStatus.FAILED.value}
                for unit in units
            )
            if has_dispatchable:
                dispatch_run_ids.add(str(run_id))
            elif all_terminal:
                finalize_run_ids.add(str(run_id))

    dispatched = _enqueue_dispatches(dispatch_sync_run, dispatch_run_ids)
    finalized = _enqueue_finalizers(finalize_sync_run, finalize_run_ids)
    return {
        "expired_units": expired_count,
        "dispatches_enqueued": dispatched,
        "finalizers_enqueued": finalized,
        "observer_repairs": repaired_observers,
    }


def _dispatchable_run_ids(
    session, stale_dispatch_cutoff: datetime, limit: int
) -> set[str]:
    rows = (
        session.query(SyncRunUnit.sync_run_id)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .filter(
            SyncRun.status.not_in(_TERMINAL_RUN_STATUSES),
            (
                (SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value)
                | (
                    (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                    & (SyncRunUnit.updated_at <= stale_dispatch_cutoff)
                )
            ),
        )
        .distinct()
        .order_by(SyncRunUnit.sync_run_id.asc())
        .limit(max(1, int(limit)))
        .all()
    )
    return {str(run_id) for (run_id,) in rows}


def _finalizable_run_ids(session, limit: int) -> set[str]:
    terminal_statuses = {
        SyncRunUnitStatus.SUCCESS.value,
        SyncRunUnitStatus.FAILED.value,
    }
    unit_exists = (
        session.query(SyncRunUnit.id)
        .filter(SyncRunUnit.sync_run_id == SyncRun.id)
        .exists()
    )
    nonterminal_unit_exists = (
        session.query(SyncRunUnit.id)
        .filter(
            SyncRunUnit.sync_run_id == SyncRun.id,
            SyncRunUnit.status.not_in(terminal_statuses),
        )
        .exists()
    )
    rows = (
        session.query(SyncRun.id)
        .filter(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))
        .filter(unit_exists)
        .filter(~nonterminal_unit_exists)
        .order_by(SyncRun.created_at.asc(), SyncRun.id.asc())
        .limit(max(1, int(limit)))
        .all()
    )
    return {str(run_id) for (run_id,) in rows}


def _terminal_runs_with_stale_observers(session, limit: int) -> list[SyncRun]:
    max_repairs = max(1, int(limit))
    runs: list[SyncRun] = []
    seen_run_ids: set[uuid.UUID] = set()

    job_runs = (
        session.query(JobRun)
        .filter(
            JobRun.status.in_({JobRunStatus.PENDING.value, JobRunStatus.RUNNING.value})
        )
        .order_by(JobRun.created_at.asc(), JobRun.id.asc())
        .all()
    )
    for job_run in job_runs:
        result = job_run.result if isinstance(job_run.result, dict) else {}
        sync_run_id = result.get("sync_run_id")
        if sync_run_id is None:
            continue
        _append_terminal_observer_run(session, runs, seen_run_ids, sync_run_id)
        if len(runs) >= max_repairs:
            break

    if len(runs) < max_repairs:
        backfill_jobs = (
            session.query(BackfillJob)
            .filter(BackfillJob.status.in_({"pending", "running"}))
            .order_by(BackfillJob.created_at.asc(), BackfillJob.id.asc())
            .all()
        )
        for job in backfill_jobs:
            sync_run_id = _backfill_job_sync_run_id(job)
            if sync_run_id is None:
                continue
            _append_terminal_observer_run(session, runs, seen_run_ids, sync_run_id)
            if len(runs) >= max_repairs:
                break

    return runs


def _append_terminal_observer_run(
    session,
    runs: list[SyncRun],
    seen_run_ids: set[uuid.UUID],
    sync_run_id: object,
) -> None:
    try:
        run_id = uuid.UUID(str(sync_run_id))
    except ValueError:
        return
    if run_id in seen_run_ids:
        return
    run = session.get(SyncRun, run_id)
    if run is None or run.status not in _TERMINAL_RUN_STATUSES:
        return
    seen_run_ids.add(run_id)
    runs.append(run)


def _backfill_job_sync_run_id(job: BackfillJob) -> str | None:
    task_id = str(job.celery_task_id or "")
    marker = "sync_run:"
    if marker not in task_id:
        return None
    return task_id.rsplit(marker, 1)[-1] or None


def _enqueue_dispatches(task, run_ids: set[str]) -> int:
    count = 0
    for run_id in sorted(run_ids):
        try:
            getattr(task, "apply_async")(args=(run_id,), queue="sync")
            count += 1
        except Exception:
            logger.exception(
                "reconcile_sync_dispatch.dispatch_enqueue_failed",
                extra={"sync_run_id": run_id},
            )
    return count


def _enqueue_finalizers(task, run_ids: set[str]) -> int:
    count = 0
    for run_id in sorted(run_ids):
        try:
            getattr(task, "apply_async")(args=(run_id,), queue="sync")
            count += 1
        except Exception:
            logger.exception(
                "reconcile_sync_dispatch.finalize_enqueue_failed",
                extra={"sync_run_id": run_id},
            )
    return count


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


_TERMINAL_RUN_STATUSES = {
    SyncRunStatus.SUCCESS.value,
    SyncRunStatus.PARTIAL_FAILED.value,
    SyncRunStatus.FAILED.value,
}


def _nonterminal_run_ids_select():
    return select(SyncRun.id).where(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))


__all__ = ["reconcile_sync_dispatch"]
