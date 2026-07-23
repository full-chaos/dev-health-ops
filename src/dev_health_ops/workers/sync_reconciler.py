from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, or_, select, update

from dev_health_ops.models import (
    BackfillJob,
    JobRun,
    JobRunStatus,
    ProviderRateLimitObservation,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunPostDispatch,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.guard import _acquire_bucket_advisory_locks
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

_WORKER_LOST_RETRY_EXHAUSTED_CATEGORY = "worker_lost_retry_exhausted"

_DEFAULT_RATE_LIMIT_OBSERVATION_RETENTION_DAYS = 14


def _emit_sync_dispatch_parity_observation(payload: dict[str, Any]) -> None:
    """Keep optional parity telemetry from owning the Celery claim path."""
    try:
        logger.info("sync_dispatch_parity_observation", extra=payload)
    except Exception:  # noqa: BLE001 - logging must never gate durable dispatch
        return


def _recover_sync_dispatch_parity_capture_transaction(session: Any) -> None:
    """Restore a session after an optional observation statement fails."""
    session.rollback()
    session.expire_all()


def _rate_limit_observation_retention_days() -> int:
    try:
        days = int(
            os.getenv(
                "SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS",
                str(_DEFAULT_RATE_LIMIT_OBSERVATION_RETENTION_DAYS),
            )
        )
    except ValueError:
        return _DEFAULT_RATE_LIMIT_OBSERVATION_RETENTION_DAYS
    return max(0, days)


_DEFAULT_BACKFILL_JOB_ORPHAN_TTL_SECONDS = 3600


def _backfill_job_orphan_ttl_seconds() -> int:
    try:
        seconds = int(
            os.getenv(
                "SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS",
                str(_DEFAULT_BACKFILL_JOB_ORPHAN_TTL_SECONDS),
            )
        )
    except ValueError:
        return _DEFAULT_BACKFILL_JOB_ORPHAN_TTL_SECONDS
    return max(0, seconds)


@celery_app.task(
    queue="sync",
    name="dev_health_ops.workers.tasks.prune_rate_limit_observations",
)
def prune_rate_limit_observations(retention_days: int | None = None) -> dict[str, Any]:
    """Delete durable rate-limit observations older than the retention window.

    Beat-scheduled (``workers/config.py``), env-tunable via
    ``SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS`` (default
    :data:`_DEFAULT_RATE_LIMIT_OBSERVATION_RETENTION_DAYS`). This table has no
    ClickHouse mirror or archival path (CHAOS-2758) -- expired rows are
    deleted outright, matching the "observation, not audit log" scope agreed
    for CHAOS-2742.
    """
    from dev_health_ops.db import get_postgres_session_sync

    days = (
        retention_days
        if retention_days is not None
        else _rate_limit_observation_retention_days()
    )
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(days)))
    with get_postgres_session_sync() as session:
        result: Any = session.execute(
            delete(ProviderRateLimitObservation).where(
                ProviderRateLimitObservation.observed_at < cutoff
            )
        )
        deleted = int(getattr(result, "rowcount", 0) or 0)
        session.flush()
    logger.info(
        "prune_rate_limit_observations.completed",
        extra={
            "deleted": deleted,
            "retention_days": days,
            "cutoff": cutoff.isoformat(),
        },
    )
    return {"status": "completed", "deleted": deleted, "retention_days": days}


# Relay contract (CHAOS-2581 / CHAOS-2596): dispatch_sync_run and
# finalize_sync_run wakeups remain durable at-least-once because their consumers
# are idempotent. post_sync is intentionally AT-MOST-ONCE: the relay marks the
# outbox row dispatched before publishing and never re-arms it on publish
# failure, because downstream metrics readers raw-aggregate computed_at
# generations and can double-count duplicate post-sync fanout. Durable
# exactly-once post-sync re-drive is deferred to CHAOS-2596.


@celery_app.task(
    queue="sync", name="dev_health_ops.workers.tasks.reconcile_sync_dispatch"
)
def reconcile_sync_dispatch(limit: int = 100) -> dict[str, Any]:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.sync.dispatch_outbox import (
        OUTBOX_KIND_DISCOVERY,
        OUTBOX_KIND_DISPATCH,
        OUTBOX_KIND_FINALIZE,
        OUTBOX_KIND_POST_SYNC,
        SyncDispatchParityObservationUnavailable,
        claim_due_outbox_rows,
        lock_outbox_claim_for_publish,
        mark_outbox_dispatched,
        mark_outbox_publish_failed,
        observe_due_outbox_rows,
        upsert_outbox_wakeup,
    )
    from dev_health_ops.workers.post_sync_dispatch import (
        _dispatch_post_sync_tasks,
        build_post_sync_dispatch_payload,
    )
    from dev_health_ops.workers.reference_discovery import (
        REFERENCE_DISCOVERY_ERROR_CATEGORY,
        REFERENCE_DISCOVERY_ERROR_MESSAGE,
        run_sync_reference_discovery,
    )
    from dev_health_ops.workers.sync_units import (
        _expired_lease_retry_backoff_seconds,
        _failed_retry_result_payload,
        _retry_result_payload,
        _stale_dispatch_seconds,
        _sync_unit_expired_lease_retry_decision,
        dispatch_sync_run,
        finalize_sync_run,
        sync_observers_for_terminal_sync_run,
    )

    now = datetime.now(timezone.utc)
    stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds())
    materialized_finalize = 0
    materialized_dispatch = 0
    materialized_discovery = 0
    materialized_post_sync = 0
    relayed_dispatch = 0
    relayed_finalize = 0
    relayed_post_sync = 0
    publish_failures = 0
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
        expired_retry_count = 0
        expired_retry_exhausted_count = 0
        for unit in expired_units:
            observed_lease_owner = unit.lease_owner
            decision = _sync_unit_expired_lease_retry_decision(unit)
            if decision["should_retry"]:
                retry_at = now + timedelta(
                    seconds=_expired_lease_retry_backoff_seconds()
                )
                retry_payload = _retry_result_payload(
                    error_category="worker_lost",
                    retry_reason="expired_lease",
                    decision=decision,
                    next_retry_at=retry_at,
                    last_lease_expired_at=now,
                )
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
                        status=SyncRunUnitStatus.RETRYING.value,
                        available_at=retry_at,
                        error="sync unit lease expired",
                        result=retry_payload,
                        expired_lease_retry_count=(
                            SyncRunUnit.expired_lease_retry_count + 1
                        ),
                        last_retry_reason="expired_lease",
                        retry_exhausted_at=None,
                        # Review finding (round 3, CHAOS-2760): an
                        # expired-lease retry is NOT a rate-limit episode --
                        # clear any stale rate_limit_deferrals/first_seen_at
                        # carried over from an earlier, resolved rate-limit
                        # episode, so BudgetGuard's wall-clock-exhaustion
                        # check (sync/budget_guard.py) never mistakes it for
                        # an ongoing one.
                        rate_limit_deferrals=0,
                        rate_limit_first_seen_at=None,
                        updated_at=now,
                        lease_owner=None,
                        lease_expires_at=None,
                    )
                    .execution_options(synchronize_session=False)
                )
                if int(result.rowcount or 0) > 0:
                    expired_count += 1
                    expired_retry_count += 1
                    expired_run_ids.add(unit.sync_run_id)
                    upsert_outbox_wakeup(
                        session,
                        sync_run_id=unit.sync_run_id,
                        kind=OUTBOX_KIND_DISPATCH,
                        available_at=retry_at,
                        now=now,
                    )
                continue

            error_category = (
                _WORKER_LOST_RETRY_EXHAUSTED_CATEGORY
                if decision["retry_exhausted"]
                else "worker_lost"
            )
            failed_payload = _failed_retry_result_payload(
                error_category=error_category,
                retry_reason="expired_lease",
                decision=decision,
                last_lease_expired_at=now,
            )
            result = session.execute(
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
                    result=failed_payload,
                    last_retry_reason="expired_lease",
                    retry_exhausted_at=now
                    if failed_payload["retry_exhausted"]
                    else None,
                    updated_at=now,
                    lease_owner=None,
                    lease_expires_at=None,
                )
                .execution_options(synchronize_session=False)
            )
            if int(result.rowcount or 0) > 0:
                expired_count += 1
                if failed_payload["retry_exhausted"]:
                    expired_retry_exhausted_count += 1
                expired_run_ids.add(unit.sync_run_id)
        session.flush()
        session.commit()
        session.expire_all()
        repaired_observers = 0
        for run in _terminal_runs_with_stale_observers(session, limit):
            sync_observers_for_terminal_sync_run(session, run)
            repaired_observers += 1
        session.flush()
        orphaned_backfill_jobs = _terminalize_orphaned_backfill_jobs(
            session, now, limit
        )
        session.flush()
        finalize_run_ids = _finalizable_run_ids(session, limit)
        for run_id in expired_run_ids:
            if _run_is_finalizable(session, run_id):
                finalize_run_ids.add(str(run_id))
        materialized_finalize = _materialize_outbox_wakeups(
            session,
            run_ids=finalize_run_ids,
            kind=OUTBOX_KIND_FINALIZE,
            now=now,
            upsert_outbox_wakeup=upsert_outbox_wakeup,
        )

        dispatch_run_ids = _dispatchable_run_ids(session, stale_dispatch_cutoff, limit)
        for run_id in expired_run_ids:
            if _run_has_dispatchable_units(session, run_id, stale_dispatch_cutoff):
                dispatch_run_ids.add(str(run_id))
        dispatch_run_ids -= finalize_run_ids
        materialized_dispatch = _materialize_outbox_wakeups(
            session,
            run_ids=dispatch_run_ids,
            kind=OUTBOX_KIND_DISPATCH,
            now=now,
            upsert_outbox_wakeup=upsert_outbox_wakeup,
        )

        discovery_run_ids = _discoverable_run_ids(session, limit, now)
        materialized_discovery = _materialize_outbox_wakeups(
            session,
            run_ids=discovery_run_ids,
            kind=OUTBOX_KIND_DISCOVERY,
            now=now,
            upsert_outbox_wakeup=upsert_outbox_wakeup,
        )

        missing_post_sync_run_ids = _missing_post_sync_outbox_run_ids(session, limit)
        materialized_post_sync = _materialize_outbox_wakeups(
            session,
            run_ids=missing_post_sync_run_ids,
            kind=OUTBOX_KIND_POST_SYNC,
            now=now,
            upsert_outbox_wakeup=upsert_outbox_wakeup,
        )
        session.commit()
        session.expire_all()

        claim_limit = max(1, int(limit))
        try:
            parity_observation = observe_due_outbox_rows(
                session, now=now, limit=claim_limit
            )
        except SyncDispatchParityObservationUnavailable as error:
            _recover_sync_dispatch_parity_capture_transaction(session)
            _emit_sync_dispatch_parity_observation(
                {
                    "event": "sync_dispatch_parity_observation",
                    "runtime": "celery",
                    "capture_status": "unavailable",
                    "reason": error.reason,
                }
            )
        except Exception:
            _recover_sync_dispatch_parity_capture_transaction(session)
            _emit_sync_dispatch_parity_observation(
                {
                    "event": "sync_dispatch_parity_observation",
                    "runtime": "celery",
                    "capture_status": "unavailable",
                    "reason": "capture_unavailable",
                }
            )
        else:
            _emit_sync_dispatch_parity_observation(parity_observation)

        claimed_rows = claim_due_outbox_rows(session, now=now, limit=claim_limit)
        session.commit()
        session.expire_all()
        for row in claimed_rows:
            if not lock_outbox_claim_for_publish(
                session,
                row.id,
                row.claim_token,
            ):
                session.rollback()
                session.expire_all()
                continue
            if row.kind == OUTBOX_KIND_POST_SYNC:
                post_sync_marked = mark_outbox_dispatched(
                    session,
                    row_id=row.id,
                    claim_token=row.claim_token,
                    now=datetime.now(timezone.utc),
                )
                session.commit()
                session.expire_all()
                if not post_sync_marked:
                    logger.warning(
                        "reconcile_sync_dispatch.post_sync_mark_dispatched_failed",
                        extra={
                            "outbox_id": str(row.id),
                            "sync_run_id": str(row.sync_run_id),
                            "kind": row.kind,
                        },
                    )
                    continue
                try:
                    if _publish_claimed_post_sync_row(
                        session,
                        row=row,
                        build_post_sync_dispatch_payload=build_post_sync_dispatch_payload,
                        dispatch_post_sync_tasks=_dispatch_post_sync_tasks,
                    ):
                        relayed_post_sync += 1
                except Exception:
                    logger.exception(
                        "reconcile_sync_dispatch.post_sync_publish_lost",
                        extra={
                            "outbox_id": str(row.id),
                            "sync_run_id": str(row.sync_run_id),
                            "kind": row.kind,
                        },
                    )
                finally:
                    session.rollback()
                    session.expire_all()
                continue

            try:
                relayed_kind = _publish_claimed_outbox_row(
                    session,
                    row=row,
                    stale_dispatch_cutoff=stale_dispatch_cutoff,
                    dispatch_sync_run=dispatch_sync_run,
                    finalize_sync_run=finalize_sync_run,
                    run_sync_reference_discovery=run_sync_reference_discovery,
                    upsert_outbox_wakeup=upsert_outbox_wakeup,
                )
            except Exception as exc:
                publish_failures += 1
                persisted_error: BaseException | str = exc
                if row.kind == OUTBOX_KIND_DISCOVERY:
                    persisted_error = REFERENCE_DISCOVERY_ERROR_MESSAGE
                    logger.error(
                        "reconcile_sync_dispatch.discovery_publish_failed",
                        extra={
                            "error_code": REFERENCE_DISCOVERY_ERROR_CATEGORY,
                            "outbox_id": str(row.id),
                            "sync_run_id": str(row.sync_run_id),
                            "kind": row.kind,
                        },
                    )
                else:
                    logger.exception(
                        "reconcile_sync_dispatch.outbox_publish_failed",
                        extra={
                            "outbox_id": str(row.id),
                            "sync_run_id": str(row.sync_run_id),
                            "kind": row.kind,
                        },
                    )
                mark_outbox_publish_failed(
                    session,
                    row_id=row.id,
                    claim_token=row.claim_token,
                    error=persisted_error,
                    attempts=row.attempts,
                    now=datetime.now(timezone.utc),
                )
                session.commit()
                session.expire_all()
                continue

            mark_outbox_dispatched(
                session,
                row_id=row.id,
                claim_token=row.claim_token,
                now=datetime.now(timezone.utc),
            )
            session.commit()
            session.expire_all()
            if relayed_kind == OUTBOX_KIND_DISPATCH:
                relayed_dispatch += 1
            elif relayed_kind == OUTBOX_KIND_FINALIZE:
                relayed_finalize += 1

    return {
        "expired_units": expired_count,
        "expired_retry_units": expired_retry_count,
        "expired_retry_exhausted_units": expired_retry_exhausted_count,
        "materialized_dispatch": materialized_dispatch,
        "materialized_discovery": materialized_discovery,
        "materialized_finalize": materialized_finalize,
        "materialized_post_sync": materialized_post_sync,
        "relayed_dispatch": relayed_dispatch,
        "relayed_finalize": relayed_finalize,
        "relayed_post_sync": relayed_post_sync,
        "publish_failures": publish_failures,
        "observer_repairs": repaired_observers,
        "orphaned_backfill_jobs": orphaned_backfill_jobs,
    }


def _dispatchable_run_ids(
    session, stale_dispatch_cutoff: datetime, limit: int
) -> set[str]:
    now = datetime.now(timezone.utc)
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
                | (
                    (SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value)
                    & (SyncRunUnit.available_at.is_not(None))
                    & (SyncRunUnit.available_at <= now)
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
    nonterminal_unit_exists = (
        session.query(SyncRunUnit.id)
        .filter(
            SyncRunUnit.sync_run_id == SyncRun.id,
            SyncRunUnit.status.not_in(terminal_statuses),
        )
        .exists()
    )
    inflight_discovery_exists = (
        session.query(SyncRunReferenceDiscovery.id)
        .filter(
            SyncRunReferenceDiscovery.sync_run_id == SyncRun.id,
            SyncRunReferenceDiscovery.status.in_({"planned", "retrying", "running"}),
        )
        .exists()
    )
    rows = (
        session.query(SyncRun.id)
        .filter(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))
        .filter(~nonterminal_unit_exists)
        .filter(~inflight_discovery_exists)
        .order_by(SyncRun.created_at.asc(), SyncRun.id.asc())
        .limit(max(1, int(limit)))
        .all()
    )
    return {str(run_id) for (run_id,) in rows}


def _missing_post_sync_outbox_run_ids(session, limit: int) -> set[str]:
    from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_POST_SYNC

    rows = (
        session.query(SyncRunPostDispatch.sync_run_id)
        .outerjoin(
            SyncDispatchOutbox,
            (SyncDispatchOutbox.sync_run_id == SyncRunPostDispatch.sync_run_id)
            & (SyncDispatchOutbox.kind == OUTBOX_KIND_POST_SYNC),
        )
        .filter(SyncRunPostDispatch.kind == OUTBOX_KIND_POST_SYNC)
        .filter(SyncDispatchOutbox.id.is_(None))
        .order_by(
            SyncRunPostDispatch.dispatched_at.asc(),
            SyncRunPostDispatch.sync_run_id.asc(),
        )
        .limit(max(1, int(limit)))
        .all()
    )
    return {str(run_id) for (run_id,) in rows}


def _discoverable_run_ids(session, limit: int, now: datetime) -> set[str]:
    rows = (
        session.query(SyncRunReferenceDiscovery.sync_run_id)
        .join(SyncRun, SyncRun.id == SyncRunReferenceDiscovery.sync_run_id)
        .filter(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))
        .filter(
            (
                SyncRunReferenceDiscovery.status.in_({"planned", "retrying"})
                & (SyncRunReferenceDiscovery.available_at <= now)
            )
            | (
                (SyncRunReferenceDiscovery.status == "running")
                & SyncRunReferenceDiscovery.lease_expires_at.is_not(None)
                & (SyncRunReferenceDiscovery.lease_expires_at <= now)
            )
        )
        .order_by(
            SyncRunReferenceDiscovery.available_at.asc(),
            SyncRunReferenceDiscovery.sync_run_id.asc(),
        )
        .limit(max(1, int(limit)))
        .all()
    )
    return {str(run_id) for (run_id,) in rows}


def _materialize_outbox_wakeups(
    session,
    *,
    run_ids: set[str],
    kind: str,
    now: datetime,
    upsert_outbox_wakeup,
) -> int:
    from dev_health_ops.sync.dispatch_outbox import OUTBOX_STATUS_PENDING

    count = 0
    for run_id in sorted(run_ids):
        existing = (
            session.query(SyncDispatchOutbox)
            .filter_by(sync_run_id=uuid.UUID(str(run_id)), kind=kind)
            .one_or_none()
        )
        if existing is not None and existing.status == OUTBOX_STATUS_PENDING:
            continue
        upsert_outbox_wakeup(
            session,
            sync_run_id=run_id,
            kind=kind,
            available_at=now,
            now=now,
        )
        count += 1
    return count


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


def _backfill_job_marker_sync_run_id(job: BackfillJob) -> uuid.UUID | None:
    sync_run_id = _backfill_job_sync_run_id(job)
    if sync_run_id is None:
        return None
    try:
        return uuid.UUID(sync_run_id)
    except ValueError:
        return None


def _backfill_job_is_orphaned(
    sync_run_id: uuid.UUID | None, existing_run_ids: set[uuid.UUID]
) -> bool:
    # Mirrors the admin surface's fallback in api/admin/routers/sync.py
    # (_backfill_job_sync_run_id / _backfill_job_run_counts): a job whose
    # marker is missing/unparseable, or whose marker resolves to a SyncRun
    # that no longer exists, has nothing left to terminalize it -- the
    # merged-status endpoint falls back to the stored pending/running status
    # forever (CHAOS-2868). A marker resolving to an EXISTING run (terminal
    # or not) is owned by the observer-repair pass above or the live
    # dispatch/finalize flow and must not be touched here.
    return sync_run_id is None or sync_run_id not in existing_run_ids


_BACKFILL_JOB_ORPHAN_SCAN_LIMIT_MULTIPLIER = 5
_BACKFILL_JOB_ORPHAN_TOTAL_SCAN_LIMIT_MULTIPLIER = 50


def _terminalize_orphaned_backfill_jobs(session, now: datetime, limit: int) -> int:
    """Terminalize orphaned pending/running BackfillJob rows past the TTL.

    The repair budget (`limit`, jobs actually terminalized) is decoupled
    from the page budget and the total scan budget (candidates examined):
    candidates are loaded oldest-first in keyset pages over `(created_at, id)`.
    Each page is capped at
    `limit * _BACKFILL_JOB_ORPHAN_SCAN_LIMIT_MULTIPLIER`, bulk-checked against
    SyncRun in one follow-up query, then terminalized in order until `limit` is
    reached. The full sweep stops when `limit` jobs have been terminalized,
    when candidates are exhausted, or after examining at most
    `limit * _BACKFILL_JOB_ORPHAN_TOTAL_SCAN_LIMIT_MULTIPLIER` rows so a
    pathological table cannot make the sweep unbounded. Without keyset paging,
    a run of non-orphan jobs at the head of the ordering (markers resolving to
    existing SyncRuns) would consume the entire scan window and permanently
    starve an orphan sitting behind them -- every reconciler run would re-select
    the exact same non-orphan window.
    """
    cutoff = now - timedelta(seconds=_backfill_job_orphan_ttl_seconds())
    max_repairs = max(1, int(limit))
    page_size = max_repairs * _BACKFILL_JOB_ORPHAN_SCAN_LIMIT_MULTIPLIER
    max_scanned = max_repairs * _BACKFILL_JOB_ORPHAN_TOTAL_SCAN_LIMIT_MULTIPLIER

    scanned = 0
    terminalized = 0
    last_created_at: datetime | None = None
    last_id: uuid.UUID | None = None
    while terminalized < max_repairs and scanned < max_scanned:
        remaining_scan = max_scanned - scanned
        batch_limit = min(page_size, remaining_scan)
        query = session.query(BackfillJob).filter(
            BackfillJob.status.in_({"pending", "running"}),
            BackfillJob.created_at <= cutoff,
        )
        if last_created_at is not None and last_id is not None:
            query = query.filter(
                or_(
                    BackfillJob.created_at > last_created_at,
                    (BackfillJob.created_at == last_created_at)
                    & (BackfillJob.id > last_id),
                )
            )
        candidates = (
            query.order_by(BackfillJob.created_at.asc(), BackfillJob.id.asc())
            .limit(batch_limit)
            .all()
        )
        if not candidates:
            break

        scanned += len(candidates)
        last_job = candidates[-1]
        last_created_at = last_job.created_at
        last_id = last_job.id

        marker_run_ids = {
            job.id: _backfill_job_marker_sync_run_id(job) for job in candidates
        }
        candidate_run_ids = {
            run_id for run_id in marker_run_ids.values() if run_id is not None
        }
        existing_run_ids: set[uuid.UUID] = set()
        if candidate_run_ids:
            rows = (
                session.query(SyncRun.id)
                .filter(SyncRun.id.in_(candidate_run_ids))
                .all()
            )
            existing_run_ids = {row_id for (row_id,) in rows}

        for job in candidates:
            if terminalized >= max_repairs:
                break
            if not _backfill_job_is_orphaned(marker_run_ids[job.id], existing_run_ids):
                continue
            job.status = "failed"
            job.error_message = "backfill job orphaned: no linked sync run"
            job.completed_at = now
            terminalized += 1

        if len(candidates) < batch_limit:
            break
    return terminalized


def _publish_claimed_outbox_row(
    session,
    *,
    row,
    stale_dispatch_cutoff: datetime,
    dispatch_sync_run,
    finalize_sync_run,
    run_sync_reference_discovery,
    upsert_outbox_wakeup,
) -> str | None:
    from dev_health_ops.sync.canonical_incident_gate import (
        CanonicalIncidentFeatureDisabledError,
        require_canonical_incident_feature_for_update_sync,
        sync_run_requires_canonical_incident_feature,
    )
    from dev_health_ops.sync.dispatch_outbox import (
        OUTBOX_KIND_DISCOVERY,
        OUTBOX_KIND_DISPATCH,
        OUTBOX_KIND_FINALIZE,
    )
    from dev_health_ops.workers.sync_units import terminalize_feature_disabled_plan

    if row.kind in {OUTBOX_KIND_DISCOVERY, OUTBOX_KIND_DISPATCH}:
        run = session.query(SyncRun).filter(SyncRun.id == row.sync_run_id).one_or_none()
        if run is not None and sync_run_requires_canonical_incident_feature(
            session, run
        ):
            try:
                require_canonical_incident_feature_for_update_sync(session, run.org_id)
            except CanonicalIncidentFeatureDisabledError as exc:
                terminalize_feature_disabled_plan(session, str(run.id), exc)
                logger.warning(
                    "reconcile_sync_dispatch.feature_disabled_before_publish",
                    extra={
                        "outbox_id": str(row.id),
                        "sync_run_id": str(run.id),
                        "org_id": str(run.org_id),
                        "kind": row.kind,
                    },
                )
                return None

    if row.kind == OUTBOX_KIND_DISCOVERY:
        getattr(run_sync_reference_discovery, "apply_async")(
            args=(str(row.sync_run_id),), queue="sync"
        )
        return OUTBOX_KIND_DISCOVERY

    if row.kind == OUTBOX_KIND_DISPATCH:
        if not _reference_discovery_successful(session, row.sync_run_id):
            _ensure_reference_discovery_wakeup(
                session,
                sync_run_id=row.sync_run_id,
                now=datetime.now(timezone.utc),
                upsert_outbox_wakeup=upsert_outbox_wakeup,
            )
            return None
        if not _run_has_dispatchable_units(
            session, row.sync_run_id, stale_dispatch_cutoff
        ):
            return None
        getattr(dispatch_sync_run, "apply_async")(
            args=(str(row.sync_run_id),), queue="sync"
        )
        return OUTBOX_KIND_DISPATCH

    if row.kind == OUTBOX_KIND_FINALIZE:
        if not _run_is_finalizable(session, row.sync_run_id):
            return None
        getattr(finalize_sync_run, "apply_async")(
            args=(str(row.sync_run_id),), queue="sync"
        )
        return OUTBOX_KIND_FINALIZE

    logger.warning(
        "reconcile_sync_dispatch.unknown_outbox_kind",
        extra={
            "outbox_id": str(row.id),
            "sync_run_id": str(row.sync_run_id),
            "kind": row.kind,
        },
    )
    raise ValueError(f"unsupported sync dispatch outbox kind: {row.kind}")


def _reference_discovery_successful(session, sync_run_id: uuid.UUID) -> bool:
    return (
        session.query(SyncRunReferenceDiscovery.id)
        .filter(
            SyncRunReferenceDiscovery.sync_run_id == sync_run_id,
            SyncRunReferenceDiscovery.status == "success",
        )
        .one_or_none()
        is not None
    )


def _ensure_reference_discovery_wakeup(
    session,
    *,
    sync_run_id: uuid.UUID,
    now: datetime,
    upsert_outbox_wakeup,
) -> None:
    ledger = (
        session.query(SyncRunReferenceDiscovery)
        .join(SyncRun, SyncRun.id == SyncRunReferenceDiscovery.sync_run_id)
        .filter(
            SyncRunReferenceDiscovery.sync_run_id == sync_run_id,
            SyncRun.status.not_in(_TERMINAL_RUN_STATUSES),
            SyncRunReferenceDiscovery.status.in_({"planned", "retrying", "running"}),
        )
        .one_or_none()
    )
    if ledger is None:
        return
    available_at = ledger.available_at or now
    if ledger.status == "running":
        if ledger.lease_expires_at is None or _as_aware(ledger.lease_expires_at) > now:
            return
        available_at = now
    upsert_outbox_wakeup(
        session,
        sync_run_id=sync_run_id,
        kind="reference_discovery",
        available_at=available_at,
        now=now,
    )


def _publish_claimed_post_sync_row(
    session,
    *,
    row,
    build_post_sync_dispatch_payload,
    dispatch_post_sync_tasks,
) -> bool:
    if not _run_has_terminal_post_sync_ledger(session, row.sync_run_id):
        return False
    payload = build_post_sync_dispatch_payload(session, row.sync_run_id)
    if payload is None:
        return False
    dispatch_post_sync_tasks(
        provider=payload.provider,
        sync_targets=payload.sync_targets,
        org_id=payload.org_id,
        from_date=payload.from_date,
        to_date=payload.to_date,
        work_graph_from_date=payload.work_graph_from_date,
        work_graph_to_date=payload.work_graph_to_date,
        auto_import_teams=payload.auto_import_teams,
        sync_run_id=str(row.sync_run_id),
    )
    return True


def _run_has_dispatchable_units(
    session, sync_run_id: str | uuid.UUID, stale_dispatch_cutoff: datetime
) -> bool:
    run_uuid = uuid.UUID(str(sync_run_id))
    now = datetime.now(timezone.utc)
    return (
        session.query(SyncRunUnit.id)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .filter(
            SyncRun.id == run_uuid,
            SyncRun.status.not_in(_TERMINAL_RUN_STATUSES),
            (
                (SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value)
                | (
                    (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                    & (SyncRunUnit.updated_at <= stale_dispatch_cutoff)
                )
                | (
                    (SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value)
                    & (SyncRunUnit.available_at.is_not(None))
                    & (SyncRunUnit.available_at <= now)
                )
            ),
        )
        .first()
        is not None
    )


def _run_is_finalizable(session, sync_run_id: str | uuid.UUID) -> bool:
    run_uuid = uuid.UUID(str(sync_run_id))
    run_exists = (
        session.query(SyncRun.id).filter(SyncRun.id == run_uuid).first() is not None
    )
    if not run_exists:
        return False
    inflight_discovery = (
        session.query(SyncRunReferenceDiscovery.id)
        .filter(
            SyncRunReferenceDiscovery.sync_run_id == run_uuid,
            SyncRunReferenceDiscovery.status.in_({"planned", "retrying", "running"}),
        )
        .first()
        is not None
    )
    if inflight_discovery:
        return False
    terminal_statuses = {
        SyncRunUnitStatus.SUCCESS.value,
        SyncRunUnitStatus.FAILED.value,
    }
    return (
        session.query(SyncRunUnit.id)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.not_in(terminal_statuses),
        )
        .first()
        is None
    )


def _run_has_terminal_post_sync_ledger(session, sync_run_id: str | uuid.UUID) -> bool:
    from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_POST_SYNC

    run_uuid = uuid.UUID(str(sync_run_id))
    return (
        session.query(SyncRunPostDispatch.id)
        .join(SyncRun, SyncRun.id == SyncRunPostDispatch.sync_run_id)
        .filter(
            SyncRun.id == run_uuid,
            SyncRun.status.in_(_TERMINAL_RUN_STATUSES),
            SyncRunPostDispatch.kind == OUTBOX_KIND_POST_SYNC,
        )
        .first()
        is not None
    )


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
