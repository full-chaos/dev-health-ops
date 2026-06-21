"""Sync run dispatch + unit worker + finalize contract (CHAOS-2512).

FROZEN CONTRACT — the three entrypoints of the fan-out execution model. Wave 2
(CHAOS-2512) implements the bodies and wraps each with the Celery ``@app.task``
decorator. They take IDs ONLY (no credentials, no DTOs) in their payloads.

Pipeline:
    plan_sync_run (CHAOS-2511)        -> persists SyncRun + units (status=planned)
    dispatch_sync_run(run_id)         -> DispatchGuard.authorize_run, then routes
                                         + queues each unit (group/chord)
    run_sync_unit(unit_id)            -> SyncTaskBootstrap.load + ProviderRuntime,
                                         executes ONE dataset, persists unit status,
                                         updates watermark ONLY if mode==incremental
                                         and the unit succeeded
    finalize_sync_run(run_id)         -> aggregates unit statuses; dispatches
                                         post-sync metrics EXACTLY ONCE via the
                                         SyncRunPostDispatch outbox

Idempotency rules:
  * dispatch_sync_run is redispatchable: it only queues units still in
    planned/stale-dispatching state.
  * finalize_sync_run is a no-op until all units are terminal, and a no-op if
    the run's post-sync outbox row already exists. Each terminal unit enqueues
    finalize; finalize itself enforces once-only via the unique
    (sync_run_id, kind) constraint on SyncRunPostDispatch.
  * Metrics are NEVER dispatched from individual units.

Observability (CHAOS-2519):
  Every structured log line emitted by the three tasks carries the full unit
  context: sync_run_id, unit_id, source_id, dataset_key, provider, cost_class.
  On failure, an error_category is classified and stored in the unit's result
  JSON so operators can distinguish provider-wide outages from source-specific
  or dataset-specific failures without querying raw exception text.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from celery import chord, group
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from dev_health_ops.models import (
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_PENDING,
    build_post_sync_dispatch_payload,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.dispatch_policy import route
from dev_health_ops.sync.guard import DispatchGuard
from dev_health_ops.sync.trigger_routing import (
    canonical_sync_config_for_sync_run,
    inactive_child_configs_for_sync_run,
    stamp_sync_run_canonical_config,
)
from dev_health_ops.sync.watermarks import set_watermark
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.queues import _cost_class_queues_enabled
from dev_health_ops.workers.sync_bootstrap import (
    ProviderRuntimeCache,
    SyncTaskBootstrap,
)

logger = logging.getLogger(__name__)
_runtime_cache = ProviderRuntimeCache()
_TERMINAL_RUN_STATUSES = {
    SyncRunStatus.SUCCESS.value,
    SyncRunStatus.PARTIAL_FAILED.value,
    SyncRunStatus.FAILED.value,
}


# ---------------------------------------------------------------------------
# Error categorisation (CHAOS-2519)
# ---------------------------------------------------------------------------

_PROVIDER_ERROR_PATTERNS: list[tuple[str, str]] = [
    # (substring_lower, category)
    ("rate limit", "rate_limit"),
    ("ratelimit", "rate_limit"),
    ("429", "rate_limit"),
    ("timeout", "timeout"),
    ("timed out", "timeout"),
    ("connection", "network"),
    ("network", "network"),
    ("ssl", "network"),
    ("certificate", "network"),
    ("401", "auth"),
    ("403", "auth"),
    ("unauthorized", "auth"),
    ("forbidden", "auth"),
    ("not found", "not_found"),
    ("404", "not_found"),
    ("500", "provider_error"),
    ("502", "provider_error"),
    ("503", "provider_error"),
    ("server error", "provider_error"),
]


def _classify_error(exc: BaseException) -> str:
    """Return a coarse error category string from an exception.

    Categories: rate_limit, timeout, network, auth, not_found,
    provider_error, adapter_error.
    """
    msg = str(exc).lower()
    for pattern, category in _PROVIDER_ERROR_PATTERNS:
        if pattern in msg:
            return category
    return "adapter_error"


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.dispatch_sync_run")
def dispatch_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Authorize, route, and queue all pending units of a planned run.

    Idempotent / redispatchable. Implemented in CHAOS-2512.
    """

    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        decision = DispatchGuard.authorize_run(session, sync_run_id)
        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            logger.warning(
                "dispatch_sync_run.missing",
                extra={"sync_run_id": sync_run_id},
            )
            return {"status": "missing", "sync_run_id": sync_run_id}

        # --- Total-cap hard-deny: whole run is over the org unit ceiling ---
        if not decision.allowed:
            error = decision.reason or "sync dispatch denied"
            if _run_has_dispatching_or_running_units(session, run_uuid):
                failed_planned = _fail_planned_units(session, run_uuid, error)
                failed_stale_dispatching = _fail_stale_dispatching_units(
                    session, run_uuid, error
                )
                session.flush()
                logger.warning(
                    "dispatch_sync_run.denied_with_active_units",
                    extra={
                        "sync_run_id": sync_run_id,
                        "reason": error,
                        "failed_planned_units": failed_planned,
                        "failed_stale_dispatching_units": failed_stale_dispatching,
                    },
                )
                _enqueue_denied_active_finalize(sync_run_id)
                return {
                    "status": "denied_active",
                    "reason": error,
                    "failed_planned_units": failed_planned,
                    "failed_stale_dispatching_units": failed_stale_dispatching,
                }
            else:
                completed_at = datetime.now(timezone.utc)
                run.status = SyncRunStatus.FAILED.value
                run.completed_at = completed_at
                run.error = error
                run.result = {"capped_unit_ids": list(decision.capped_unit_ids)}
                session.flush()
                logger.warning(
                    "dispatch_sync_run.denied",
                    extra={
                        "sync_run_id": sync_run_id,
                        "reason": run.error,
                    },
                )
                return {"status": "denied", "reason": run.error}

        if not decision.allowed:
            logger.warning(
                "dispatch_sync_run.continuing_after_denial_for_active_units",
                extra={
                    "sync_run_id": sync_run_id,
                    "reason": decision.reason or "sync dispatch denied",
                },
            )

        # --- Concurrency partial-cap: defer overflow units, proceed with rest ---
        capped_ids: frozenset[str] = frozenset()
        if decision.concurrency_capped and decision.capped_unit_ids:
            capped_ids = frozenset(decision.capped_unit_ids)
            logger.info(
                "dispatch_sync_run.concurrency_capped",
                extra={
                    "sync_run_id": sync_run_id,
                    "capped_count": len(capped_ids),
                    "reason": decision.reason,
                },
            )

        canonical_config = canonical_sync_config_for_sync_run(session, run)
        inactive_config = (
            canonical_config
            if canonical_config is not None and not bool(canonical_config.is_active)
            else None
        )
        if inactive_config is None:
            inactive_child_configs = inactive_child_configs_for_sync_run(session, run)
            inactive_config = (
                inactive_child_configs[0] if inactive_child_configs else None
            )
        if inactive_config is not None:
            error = "sync configuration is paused"
            if _run_has_dispatching_or_running_units(session, run_uuid):
                failed_planned = _fail_planned_units(session, run_uuid, error)
                failed_stale_dispatching = _fail_stale_dispatching_units(
                    session, run_uuid, error
                )
                session.flush()
                logger.warning(
                    "dispatch_sync_run.inactive_config_with_active_units",
                    extra={
                        "sync_run_id": sync_run_id,
                        "config_id": str(inactive_config.id),
                        "failed_planned_units": failed_planned,
                        "failed_stale_dispatching_units": failed_stale_dispatching,
                    },
                )
                _enqueue_denied_active_finalize(sync_run_id)
                return {
                    "status": "denied_active",
                    "reason": error,
                    "failed_planned_units": failed_planned,
                    "failed_stale_dispatching_units": failed_stale_dispatching,
                }
            else:
                completed_at = datetime.now(timezone.utc)
                run.status = SyncRunStatus.FAILED.value
                run.completed_at = completed_at
                run.error = error
                run.result = {"reason": "inactive_sync_configuration"}
                session.query(SyncRunUnit).filter(
                    SyncRunUnit.sync_run_id == run_uuid,
                    SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
                ).update(
                    {
                        SyncRunUnit.status: SyncRunUnitStatus.FAILED.value,
                        SyncRunUnit.error: error,
                        SyncRunUnit.updated_at: completed_at,
                    },
                    synchronize_session=False,
                )
                stamp_sync_run_canonical_config(
                    session,
                    run,
                    completed_at=completed_at,
                    success=False,
                    error=error,
                    stats={"reason": "inactive_sync_configuration"},
                )
                session.flush()
                logger.warning(
                    "dispatch_sync_run.inactive_config",
                    extra={
                        "sync_run_id": sync_run_id,
                        "config_id": str(inactive_config.id),
                    },
                )
                return {"status": "denied", "reason": error}

        units = _claim_units(session, run_uuid, capped_ids=capped_ids)
        signatures = []
        for unit in units:
            dispatch_route = route(
                org_id=str(unit.org_id),
                provider=str(unit.provider),
                cost_class=str(unit.cost_class),
                cost_class_queues_enabled=_cost_class_queues_enabled(),
            )
            signatures.append(
                getattr(run_sync_unit, "s")(str(unit.id)).set(
                    queue=dispatch_route.queue
                )
            )

        if signatures:
            now = datetime.now(timezone.utc)
            run.status = SyncRunStatus.DISPATCHING.value
            run.started_at = run.started_at or now
            session.flush()
        else:
            session.flush()

    if signatures:
        logger.info(
            "dispatch_sync_run.dispatched",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": len(signatures),
            },
        )
        callback = getattr(finalize_sync_run, "si")(sync_run_id)
        callback.set(queue="sync")
        try:
            chord(group(signatures), callback).apply_async()
            if capped_ids:
                _schedule_redispatch(sync_run_id)
        except Exception as exc:
            logger.exception(
                "dispatch_sync_run.publish_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
            raise
        return {"status": "dispatched", "queued_units": len(signatures)}

    # Fix 2: no units were claimable this pass.  Distinguish two cases:
    #   a) Deferred work remains (PLANNED units exist, not all terminal) →
    #      schedule a countdown redispatch so they drain when slots free up.
    #   b) No deferred work (zero-unit run, or every unit already terminal) →
    #      call finalize directly; redispatching would loop forever.
    with get_postgres_session_sync() as session:
        run_uuid_check = uuid.UUID(str(sync_run_id))
        pending_count = (
            session.query(SyncRunUnit)
            .filter(
                SyncRunUnit.sync_run_id == run_uuid_check,
                SyncRunUnit.status.in_(
                    {
                        SyncRunUnitStatus.PLANNED.value,
                        SyncRunUnitStatus.DISPATCHING.value,
                        SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnitStatus.RETRYING.value,
                    }
                ),
            )
            .count()
        )
    if pending_count > 0:
        _schedule_redispatch(sync_run_id)
        logger.info(
            "dispatch_sync_run.noop",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": 0,
                "pending_units": pending_count,
            },
        )
        return {"status": "noop", "queued_units": 0}
    # No pending work — finalize (idempotent; handles zero-unit and already-finalized).
    logger.info(
        "dispatch_sync_run.noop_finalize",
        extra={"sync_run_id": sync_run_id, "queued_units": 0},
    )
    finalize_sync_run(sync_run_id)
    return {"status": "noop", "queued_units": 0}


@celery_app.task(
    bind=True,
    max_retries=0,
    queue="sync",
    name="dev_health_ops.workers.tasks.run_sync_unit",
)
def run_sync_unit(self, unit_id: str) -> dict[str, Any]:
    """Execute exactly one (source, dataset, window) unit.

    Loads context via SyncTaskBootstrap, runs the provider dataset adapter
    (CHAOS-2513), persists status/attempts/duration/result, and updates the
    watermark only when mode=="incremental" and the unit succeeded. Never
    dispatches metrics. Implemented in CHAOS-2512.
    """

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.processors.dataset_adapters import run_dataset_unit

    sync_run_id: str | None = None
    should_finalize = False
    started_at = datetime.now(timezone.utc)
    lease_owner: str | None = None
    terminal_txn_started = False
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None
    # Unit context fields for structured logging — populated once ctx is loaded.
    _log_ctx: dict[str, Any] = {"unit_id": unit_id}
    try:
        with get_postgres_session_sync() as session:
            unit = _load_unit(session, unit_id)
            sync_run_id = str(unit.sync_run_id)
            run = (
                session.query(SyncRun)
                .filter(SyncRun.id == unit.sync_run_id)
                .one_or_none()
            )
            if unit.status in {
                SyncRunUnitStatus.SUCCESS.value,
                SyncRunUnitStatus.FAILED.value,
            } or (run is not None and run.status in _TERMINAL_RUN_STATUSES):
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "terminal",
                }
            lease_owner = str(uuid.uuid4())
            lease_expires_at = started_at + timedelta(seconds=_running_lease_seconds())
            claim_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == unit.id,
                    SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.RUNNING.value,
                    attempts=SyncRunUnit.attempts + 1,
                    error=None,
                    lease_owner=lease_owner,
                    lease_expires_at=lease_expires_at,
                    last_heartbeat_at=started_at,
                    updated_at=started_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(claim_result.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "not_dispatchable",
                }
            session.flush()

        heartbeat_stop, heartbeat_thread = _start_unit_heartbeat(unit_id, lease_owner)

        with get_postgres_session_sync() as session:
            ctx = SyncTaskBootstrap.load(session, unit_id)
            sync_run_id = ctx.sync_run_id
            unit = _load_unit(session, unit_id)
            _log_ctx = {
                "sync_run_id": ctx.sync_run_id,
                "unit_id": unit_id,
                "source_id": str(unit.source_id),
                "dataset_key": ctx.dataset_key,
                "provider": ctx.provider,
                "cost_class": ctx.cost_class,
            }
            now = datetime.now(timezone.utc)
            live_lease_refresh: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at > now,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    lease_expires_at=now + timedelta(seconds=_running_lease_seconds()),
                    last_heartbeat_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if int(live_lease_refresh.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }

        logger.info("run_sync_unit.started", extra=_log_ctx)

        runtime = _runtime_cache.get(ctx)
        result = run_dataset_unit(ctx, runtime)

        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        terminal_txn_started = True
        with get_postgres_session_sync() as session:
            terminal_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at > completed_at,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.SUCCESS.value,
                    duration_seconds=duration_seconds,
                    result=dict(result or {}),
                    error=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(terminal_result.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            upsert_outbox_wakeup(
                session,
                sync_run_id=ctx.sync_run_id,
                kind=OUTBOX_KIND_FINALIZE,
                available_at=completed_at,
                now=completed_at,
            )
            if ctx.mode in {
                SyncRunMode.INCREMENTAL.value,
                SyncRunMode.FULL_RESYNC.value,  # full_resync stamps watermark on success
            }:
                set_watermark(
                    session,
                    ctx.org_id,
                    ctx.source_external_id,
                    ctx.dataset_key,
                    started_at,
                )
            session.flush()
            should_finalize = True
        terminal_txn_started = False
        logger.info(
            "run_sync_unit.success",
            extra={**_log_ctx, "duration_seconds": duration_seconds},
        )
        return {
            "status": "success",
            "unit_id": unit_id,
            "duration_seconds": duration_seconds,
        }
    except Exception as exc:
        if terminal_txn_started:
            raise
        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        error_category = _classify_error(exc)
        terminal_txn_started = True
        with get_postgres_session_sync() as session:
            terminal_result = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_expires_at.is_not(None),
                    SyncRunUnit.lease_expires_at > completed_at,
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.FAILED.value,
                    duration_seconds=duration_seconds,
                    error=str(exc),
                    result={"error_category": error_category},
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(terminal_result.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            if sync_run_id is not None:
                upsert_outbox_wakeup(
                    session,
                    sync_run_id=sync_run_id,
                    kind=OUTBOX_KIND_FINALIZE,
                    available_at=completed_at,
                    now=completed_at,
                )
            session.flush()
            should_finalize = True
        terminal_txn_started = False
        logger.exception(
            "run_sync_unit.failed",
            extra={
                **_log_ctx,
                "duration_seconds": duration_seconds,
                "error_category": error_category,
            },
        )
        return {
            "status": "failed",
            "unit_id": unit_id,
            "error": str(exc),
            "error_category": error_category,
        }
    finally:
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=2)
        if should_finalize and sync_run_id is not None:
            try:
                getattr(finalize_sync_run, "apply_async")(
                    args=(sync_run_id,), queue="sync"
                )
            except Exception:
                logger.exception(
                    "run_sync_unit.finalize_enqueue_failed",
                    extra={"sync_run_id": sync_run_id, "unit_id": unit_id},
                )


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.finalize_sync_run")
def finalize_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Aggregate unit statuses and materialize post-sync metrics once per run.

    No-op until all units are terminal; once-only via the SyncRunPostDispatch
    outbox. Maps completed dataset keys back to legacy sync_targets for the
    reconciler relay, which is the sole post-sync publisher.
    """

    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            logger.warning(
                "finalize_sync_run.missing",
                extra={"sync_run_id": sync_run_id},
            )
            return {"status": "missing", "sync_run_id": sync_run_id}

        units = (
            session.query(SyncRunUnit)
            .filter(SyncRunUnit.sync_run_id == run_uuid)
            .order_by(SyncRunUnit.id)
            .all()
        )
        terminal_statuses = {
            SyncRunUnitStatus.SUCCESS.value,
            SyncRunUnitStatus.FAILED.value,
        }
        if any(unit.status not in terminal_statuses for unit in units):
            logger.debug(
                "finalize_sync_run.pending",
                extra={"sync_run_id": sync_run_id, "total_units": len(units)},
            )
            return {"status": "pending", "sync_run_id": sync_run_id}

        success_count = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.SUCCESS.value
        )
        failed_count = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.FAILED.value
        )
        total_count = len(units)
        completed_at = datetime.now(timezone.utc)
        run.completed_units = success_count
        run.failed_units = failed_count
        run.completed_at = run.completed_at or completed_at
        run.status = _aggregate_run_status(total_count, success_count, failed_count)
        result_payload: dict[str, Any] = {
            "completed_units": success_count,
            "failed_units": failed_count,
        }
        if total_count == 0:
            run.error = "No sync units planned"
            result_payload["reason"] = "no_sync_units_planned"
        run.result = result_payload
        run_success = run.status == SyncRunStatus.SUCCESS.value
        run_error = (
            None
            if run_success
            else (run.error or "Sync run completed with failed units")
        )
        stamp_sync_run_canonical_config(
            session,
            run,
            completed_at=run.completed_at,
            success=run_success,
            error=run_error,
            stats=result_payload,
        )
        session.flush()

        nested = session.begin_nested()
        try:
            session.add(
                SyncRunPostDispatch(
                    org_id=str(run.org_id),
                    sync_run_id=run_uuid,
                    kind=OUTBOX_KIND_POST_SYNC,
                    dispatched_at=completed_at,
                )
            )
            session.flush()
        except IntegrityError:
            nested.rollback()
            return {"status": "already_dispatched", "sync_run_id": sync_run_id}
        else:
            upsert_outbox_wakeup(
                session,
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_POST_SYNC,
                available_at=completed_at,
                now=completed_at,
            )
            nested.commit()
        post_sync_payload = build_post_sync_dispatch_payload(session, run_uuid)
        post_sync_targets = (
            post_sync_payload.sync_targets if post_sync_payload is not None else []
        )
        session.flush()

    run_status = _aggregate_run_status(len(units), success_count, failed_count)
    logger.info(
        "finalize_sync_run.finalized",
        extra={
            "sync_run_id": sync_run_id,
            "completed_units": success_count,
            "failed_units": failed_count,
            "run_status": run_status,
        },
    )

    return {
        "status": "finalized",
        "sync_run_id": sync_run_id,
        "completed_units": success_count,
        "failed_units": failed_count,
        "post_sync_targets": post_sync_targets,
    }


def _run_has_dispatching_or_running_units(session, run_uuid: uuid.UUID) -> bool:
    return (
        session.query(SyncRunUnit.id)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnitStatus.RUNNING.value,
                }
            ),
        )
        .first()
        is not None
    )


def _fail_planned_units(session, run_uuid: uuid.UUID, error: str) -> int:
    now = datetime.now(timezone.utc)
    result = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
        )
        .update(
            {
                SyncRunUnit.status: SyncRunUnitStatus.FAILED.value,
                SyncRunUnit.error: error,
                SyncRunUnit.updated_at: now,
            },
            synchronize_session=False,
        )
    )
    return int(result or 0)


def _fail_stale_dispatching_units(session, run_uuid: uuid.UUID, error: str) -> int:
    now = datetime.now(timezone.utc)
    stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds())
    # Write-time CAS (NOT load-and-mutate): the ``status == 'dispatching'`` predicate
    # is evaluated by the database at UPDATE time, so a stale row that a delayed
    # ``run_sync_unit`` concurrently claimed to RUNNING (DISPATCHING->RUNNING + live
    # lease) between our read and write is EXCLUDED -- we never overwrite a live
    # worker's claim with FAILED.  ``updated_at <= cutoff`` scopes to genuinely stale
    # rows exactly as the prior load-and-mutate did, still scoped to this run.
    result = session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
            SyncRunUnit.updated_at <= stale_dispatch_cutoff,
        )
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            error=error,
            result={"error_category": "dispatch_denied"},
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)


def _enqueue_denied_active_finalize(sync_run_id: str) -> None:
    try:
        getattr(finalize_sync_run, "apply_async")(args=(sync_run_id,), queue="sync")
    except Exception:
        logger.exception(
            "dispatch_sync_run.denied_active_finalize_enqueue_failed",
            extra={"sync_run_id": sync_run_id},
        )
        raise


def _claim_units(
    session,
    run_uuid: uuid.UUID,
    *,
    capped_ids: frozenset[str] = frozenset(),
) -> list[SyncRunUnit]:
    """Atomically claim dispatchable units for a run.

    Fresh ``planned`` units are claimed with an atomic ``UPDATE ... RETURNING``
    so two concurrent ``dispatch_sync_run`` calls cannot both enqueue the same
    unit (no double-queue / duplicate provider writes).  Stale ``dispatching``
    units (a worker died before the unit started running) are reclaimed by age.

    F2 fix: RUNNING units are NEVER reclaimed here.  ``run_sync_unit`` does not
    heartbeat during the provider call, so a legitimately long-running unit past
    SYNC_UNIT_RUNNING_STALE_SECONDS would be re-dispatched and run a second time
    concurrently, causing duplicate provider writes.  Durable dead-worker
    recovery (heartbeat + lease) is a separate follow-up (CHAOS-2577).

    INTERIM: a capped run whose only blocker is a DEAD RUNNING unit may stall
    — acceptable, preserves at-most-once provider execution.

    ``capped_ids`` is the set of unit IDs that the concurrency guard deferred.
    Those units are left in PLANNED status so a later redispatch can claim them
    once slots free up.
    """
    now = datetime.now(timezone.utc)
    # Build the WHERE clause for the atomic claim, excluding capped units.
    planned_where = [
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
    ]
    if capped_ids:
        planned_where.append(
            ~SyncRunUnit.id.in_([uuid.UUID(cid) for cid in capped_ids])
        )
    claimed_ids: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(*planned_where)
            .values(status=SyncRunUnitStatus.DISPATCHING.value, updated_at=now)
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )

    # Reclaim stale DISPATCHING units only (F2: RUNNING is never reclaimed).
    # A DISPATCHING unit that is stale means the worker was enqueued but never
    # picked up (e.g. broker restart).  It is safe to re-enqueue because the
    # worker never started the provider call.
    #
    # Atomic CAS: this single UPDATE re-checks status='dispatching' AND
    # updated_at <= stale_dispatch at write time, so a row that a delayed
    # run_sync_unit concurrently claimed to RUNNING is excluded by
    # construction -- it can never be reclaimed/requeued, and no status
    # rewrite of a RUNNING row is possible.  status stays DISPATCHING; only
    # updated_at is refreshed so a later redispatch re-enqueues the unit.
    stale_dispatch = now - timedelta(seconds=_stale_dispatch_seconds())
    stale_where = [
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
        SyncRunUnit.updated_at <= stale_dispatch,
        ~SyncRunUnit.id.in_(claimed_ids),
    ]
    if capped_ids:
        stale_where.append(~SyncRunUnit.id.in_([uuid.UUID(cid) for cid in capped_ids]))
    stale_reclaimed: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(*stale_where)
            .values(updated_at=now)
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )
    claimed_ids.update(stale_reclaimed)

    session.flush()
    if not claimed_ids:
        return []
    return (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.id.in_(claimed_ids))
        .order_by(SyncRunUnit.id)
        .all()
    )


def _load_unit(session, unit_id: str) -> SyncRunUnit:
    unit_uuid = uuid.UUID(str(unit_id))
    unit = session.query(SyncRunUnit).filter(SyncRunUnit.id == unit_uuid).one_or_none()
    if unit is None:
        raise ValueError(f"Sync run unit not found: {unit_id}")
    return unit


def _nonterminal_run_ids_select():
    return select(SyncRun.id).where(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))


def _aggregate_run_status(
    total_count: int, success_count: int, failed_count: int
) -> str:
    if total_count == 0:
        return SyncRunStatus.FAILED.value
    if failed_count == 0:
        return SyncRunStatus.SUCCESS.value
    if success_count == 0:
        return SyncRunStatus.FAILED.value
    return SyncRunStatus.PARTIAL_FAILED.value


def _stale_dispatch_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "900")))
    except ValueError:
        return 900


def _stale_running_seconds() -> int:
    try:
        return max(
            1,
            int(os.getenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "3600")),
        )
    except ValueError:
        return 3600


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _running_lease_seconds() -> int:
    return _stale_running_seconds()


def _heartbeat_interval_seconds() -> int:
    return max(1, min(60, _running_lease_seconds() // 4))


def _unit_lease_is_owned_and_live(
    unit: SyncRunUnit,
    lease_owner: str | None,
    now: datetime,
) -> bool:
    if lease_owner is None or unit.lease_owner != lease_owner:
        return False
    if unit.lease_expires_at is None:
        return False
    return _as_aware(unit.lease_expires_at) > now


def _start_unit_heartbeat(
    unit_id: str,
    lease_owner: str | None,
) -> tuple[threading.Event, threading.Thread] | tuple[None, None]:
    if lease_owner is None:
        return None, None
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_unit_lease,
        args=(unit_id, lease_owner, stop_event),
        name=f"sync-unit-heartbeat-{unit_id}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _heartbeat_unit_lease(
    unit_id: str,
    lease_owner: str,
    stop_event: threading.Event,
) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    interval = _heartbeat_interval_seconds()
    lease_seconds = _running_lease_seconds()
    while not stop_event.wait(interval):
        now = datetime.now(timezone.utc)
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        try:
            with get_postgres_session_sync() as session:
                heartbeat_result: Any = session.execute(
                    update(SyncRunUnit)
                    .where(
                        SyncRunUnit.id == uuid.UUID(str(unit_id)),
                        SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.lease_owner == lease_owner,
                        SyncRunUnit.lease_expires_at > now,
                        SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                    )
                    .values(
                        lease_expires_at=lease_expires_at,
                        last_heartbeat_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
                if int(heartbeat_result.rowcount or 0) == 0:
                    logger.info(
                        "run_sync_unit.heartbeat_lease_lost",
                        extra={"unit_id": unit_id},
                    )
                    stop_event.set()
        except Exception:
            logger.exception(
                "run_sync_unit.heartbeat_failed",
                extra={"unit_id": unit_id},
            )


def _schedule_redispatch(sync_run_id: str) -> None:
    try:
        from dev_health_ops.db import get_postgres_session_sync

        countdown = int(os.getenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60"))
        now = datetime.now(timezone.utc)
        with get_postgres_session_sync() as session:
            upsert_outbox_wakeup(
                session,
                sync_run_id=sync_run_id,
                kind=OUTBOX_KIND_DISPATCH,
                available_at=now + timedelta(seconds=countdown),
                now=now,
            )
            session.execute(
                update(SyncDispatchOutbox)
                .where(
                    SyncDispatchOutbox.sync_run_id == uuid.UUID(str(sync_run_id)),
                    SyncDispatchOutbox.kind == OUTBOX_KIND_DISPATCH,
                    SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
                    SyncDispatchOutbox.claim_token.is_(None),
                )
                .values(
                    available_at=now + timedelta(seconds=countdown),
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            session.flush()
        logger.info(
            "dispatch_sync_run.redispatch_rearmed",
            extra={"sync_run_id": sync_run_id, "countdown": countdown},
        )
    except Exception:
        logger.exception(
            "dispatch_sync_run.redispatch_rearm_failed",
            extra={"sync_run_id": sync_run_id},
        )
