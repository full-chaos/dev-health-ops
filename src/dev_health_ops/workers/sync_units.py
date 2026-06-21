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
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from celery import chord, group
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from dev_health_ops.models import (
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_policy import route
from dev_health_ops.sync.guard import DispatchGuard
from dev_health_ops.sync.planner import map_datasets_to_legacy_targets
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
from dev_health_ops.workers.sync_runtime import _dispatch_post_sync_tasks

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
            completed_at = datetime.now(timezone.utc)
            run.status = SyncRunStatus.FAILED.value
            run.completed_at = completed_at
            run.error = decision.reason or "sync dispatch denied"
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
            completed_at = datetime.now(timezone.utc)
            error = "sync configuration is paused"
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
        try:
            _schedule_redispatch(sync_run_id)
        except Exception as exc:
            logger.exception(
                "dispatch_sync_run.redispatch_publish_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
            raise
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
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None
    # Unit context fields for structured logging — populated once ctx is loaded.
    _log_ctx: dict[str, Any] = {"unit_id": unit_id}
    try:
        with get_postgres_session_sync() as session:
            unit = _load_unit(session, unit_id)
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
                )
                .values(
                    status=SyncRunUnitStatus.RUNNING.value,
                    attempts=SyncRunUnit.attempts + 1,
                    error=None,
                    lease_owner=lease_owner,
                    lease_expires_at=lease_expires_at,
                    last_heartbeat_at=started_at,
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
            session.refresh(unit)
            should_finalize = True
            ctx = SyncTaskBootstrap.load(session, unit_id)
            sync_run_id = ctx.sync_run_id
            _log_ctx = {
                "sync_run_id": ctx.sync_run_id,
                "unit_id": unit_id,
                "source_id": str(unit.source_id),
                "dataset_key": ctx.dataset_key,
                "provider": ctx.provider,
                "cost_class": ctx.cost_class,
            }

        logger.info("run_sync_unit.started", extra=_log_ctx)

        heartbeat_stop, heartbeat_thread = _start_unit_heartbeat(unit_id, lease_owner)

        runtime = _runtime_cache.get(ctx)
        result = run_dataset_unit(ctx, runtime)

        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        with get_postgres_session_sync() as session:
            unit = _load_unit(session, unit_id)
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
            if not _unit_lease_is_owned_and_live(unit, lease_owner, completed_at):
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            unit.status = SyncRunUnitStatus.SUCCESS.value
            unit.duration_seconds = duration_seconds
            unit.result = dict(result or {})
            unit.error = None
            unit.lease_owner = None
            unit.lease_expires_at = None
            unit.last_heartbeat_at = completed_at
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
        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        error_category = _classify_error(exc)
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
            if lease_owner is not None and not _unit_lease_is_owned_and_live(
                unit, lease_owner, completed_at
            ):
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            unit.status = SyncRunUnitStatus.FAILED.value
            unit.duration_seconds = duration_seconds
            unit.error = str(exc)
            unit.result = {"error_category": error_category}
            unit.lease_owner = None
            unit.lease_expires_at = None
            unit.last_heartbeat_at = completed_at
            session.flush()
            should_finalize = True
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
    """Aggregate unit statuses and dispatch post-sync metrics once per run.

    No-op until all units are terminal; once-only via the SyncRunPostDispatch
    outbox. Maps completed dataset keys back to legacy sync_targets via
    ``planner.map_datasets_to_legacy_targets`` before calling the existing
    ``_dispatch_post_sync_tasks``. Implemented in CHAOS-2512.
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
                    kind="post_sync",
                    dispatched_at=completed_at,
                )
            )
            session.flush()
        except IntegrityError:
            nested.rollback()
            return {"status": "already_dispatched", "sync_run_id": sync_run_id}
        else:
            nested.commit()

        successful_by_provider: dict[str, set[str]] = defaultdict(set)
        for unit in units:
            if unit.status == SyncRunUnitStatus.SUCCESS.value:
                successful_by_provider[str(unit.provider)].add(str(unit.dataset_key))

        legacy_targets: set[str] = set()
        for provider, dataset_keys in successful_by_provider.items():
            legacy_targets.update(
                map_datasets_to_legacy_targets(provider, dataset_keys)
            )

        # --- WS-E / CHAOS-2577: compute covered window from successful units ---
        # Thread min(since_at)/max(before_at) of successful units into
        # _dispatch_post_sync_tasks so metrics/work-graph cover the backfilled
        # range.  Confined to this distinct block; WS-C later advances a
        # coverage marker in a separate block below.
        successful_units = [
            u for u in units if u.status == SyncRunUnitStatus.SUCCESS.value
        ]
        covered_since: datetime | None = None
        covered_before: datetime | None = None
        if successful_units:
            # If ANY unit has since_at=None the lower bound is unbounded;
            # only compute min when ALL units carry an explicit lower bound.
            any_unbounded_lower = any(u.since_at is None for u in successful_units)
            any_unbounded_upper = any(u.before_at is None for u in successful_units)
            if not any_unbounded_lower:
                since_values = [
                    _as_aware(u.since_at)
                    for u in successful_units
                    if u.since_at is not None
                ]
                covered_since = min(since_values)
            # else: covered_since stays None → unbounded lower
            if not any_unbounded_upper:
                before_values = [
                    _as_aware(u.before_at)
                    for u in successful_units
                    if u.before_at is not None
                ]
                covered_before = max(before_values)
            # else: covered_before stays None → unbounded upper

        provider_for_dispatch = next(iter(successful_by_provider), "unknown")
        run_org_id = str(run.org_id)
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

    # The outbox claim is now durably committed (the session block exited and
    # committed). Dispatch post-sync metrics AFTER that commit so a crash
    # between claim and dispatch cannot roll back the claim and let a later
    # finalize re-dispatch (at-most-once, not duplicate-on-retry).
    if legacy_targets:
        # Thread the covered window so downstream metrics/work-graph tasks
        # know the exact date range that was backfilled (CHAOS-2577).
        from_date_str = (
            covered_since.date().isoformat() if covered_since is not None else None
        )
        to_date_str = (
            covered_before.date().isoformat() if covered_before is not None else None
        )
        # F3: pass full ISO datetimes via work_graph_from/to_date so the
        # work-graph build covers the full final day (not truncated to midnight).
        # from_date/to_date remain date-only for _parse_materialize_window().
        # Mirror the precedent in sync_backfill.py:374-383.
        from datetime import time as _time

        work_graph_from_date_str = (
            datetime.combine(
                covered_since.date(),
                _time.min,
                tzinfo=timezone.utc,
            ).isoformat()
            if covered_since is not None
            else None
        )
        work_graph_to_date_str = (
            datetime.combine(
                covered_before.date() + timedelta(days=1),
                _time.min,
                tzinfo=timezone.utc,
            ).isoformat()
            if covered_before is not None
            else None
        )
        _dispatch_post_sync_tasks(
            provider=provider_for_dispatch,
            sync_targets=sorted(legacy_targets),
            org_id=run_org_id,
            from_date=from_date_str,
            to_date=to_date_str,
            work_graph_from_date=work_graph_from_date_str,
            work_graph_to_date=work_graph_to_date_str,
        )

    return {
        "status": "finalized",
        "sync_run_id": sync_run_id,
        "completed_units": success_count,
        "failed_units": failed_count,
        "post_sync_targets": sorted(legacy_targets),
    }


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
    stale_dispatch = now - timedelta(seconds=_stale_dispatch_seconds())
    reclaim_candidates = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
        )
        .all()
    )
    for unit in reclaim_candidates:
        if unit.id in claimed_ids:
            continue
        # Never reclaim a unit that the concurrency guard deferred.
        if str(unit.id) in capped_ids:
            continue
        if _as_aware(unit.updated_at) <= stale_dispatch:
            unit.status = SyncRunUnitStatus.DISPATCHING.value
            unit.updated_at = now
            claimed_ids.add(unit.id)

    session.flush()
    if not claimed_ids:
        return []
    return (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.id.in_(claimed_ids))
        .order_by(SyncRunUnit.id)
        .all()
    )


def _mark_dispatch_enqueue_failed(sync_run_id: str, error: str) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    completed_at = datetime.now(timezone.utc)
    run_uuid = uuid.UUID(str(sync_run_id))
    with get_postgres_session_sync() as session:
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            return
        run.status = SyncRunStatus.FAILED.value
        run.completed_at = completed_at
        run.error = error
        run.result = {"error": error, "phase": "dispatch_enqueue"}
        units = (
            session.query(SyncRunUnit).filter(SyncRunUnit.sync_run_id == run_uuid).all()
        )
        for unit in units:
            if unit.status not in {
                SyncRunUnitStatus.SUCCESS.value,
                SyncRunUnitStatus.FAILED.value,
            }:
                unit.status = SyncRunUnitStatus.FAILED.value
                unit.error = error
                unit.updated_at = completed_at
        run.completed_units = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.SUCCESS.value
        )
        run.failed_units = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.FAILED.value
        )
        stamp_sync_run_canonical_config(
            session,
            run,
            completed_at=completed_at,
            success=False,
            error=error,
            stats={"error": error, "phase": "dispatch_enqueue"},
        )
        session.flush()


def _load_unit(session, unit_id: str) -> SyncRunUnit:
    unit_uuid = uuid.UUID(str(unit_id))
    unit = session.query(SyncRunUnit).filter(SyncRunUnit.id == unit_uuid).one_or_none()
    if unit is None:
        raise ValueError(f"Sync run unit not found: {unit_id}")
    return unit


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
                session.execute(
                    update(SyncRunUnit)
                    .where(
                        SyncRunUnit.id == uuid.UUID(str(unit_id)),
                        SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.lease_owner == lease_owner,
                        SyncRunUnit.lease_expires_at > now,
                    )
                    .values(
                        lease_expires_at=lease_expires_at,
                        last_heartbeat_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
        except Exception:
            logger.exception(
                "run_sync_unit.heartbeat_failed",
                extra={"unit_id": unit_id},
            )


def _schedule_redispatch(sync_run_id: str) -> None:
    """Schedule a delayed redispatch of a run with deferred (PLANNED) units.

    Uses ``apply_async(countdown=...)`` so capped units are retried after
    in-flight units from other runs have had a chance to complete.
    This is the D3 deferral mechanism: idempotent, no new status, no Celery retry.

    Raises on broker enqueue failure so the caller can propagate the error
    rather than leaving the run silently non-terminal.
    """
    countdown = int(os.getenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60"))
    try:
        getattr(dispatch_sync_run, "apply_async")(
            args=(sync_run_id,),
            queue="sync",
            countdown=countdown,
        )
        logger.info(
            "dispatch_sync_run.redispatch_scheduled",
            extra={"sync_run_id": sync_run_id, "countdown": countdown},
        )
    except Exception:
        logger.exception(
            "dispatch_sync_run.redispatch_schedule_failed",
            extra={"sync_run_id": sync_run_id},
        )
        raise
