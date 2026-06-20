from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import update

from dev_health_ops.models import SyncRunUnit, SyncRunUnitStatus
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
            result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == unit.id,
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner.is_not(None),
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at <= now,
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
        dispatch_run_ids: set[str] = set()
        finalize_run_ids: set[str] = set()
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
    }


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


__all__ = ["reconcile_sync_dispatch"]
