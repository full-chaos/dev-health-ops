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

        units = _claim_units(session, run_uuid)
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
        except Exception as exc:
            _mark_dispatch_enqueue_failed(sync_run_id, str(exc))
            raise
        return {"status": "dispatched", "queued_units": len(signatures)}

    logger.info(
        "dispatch_sync_run.noop",
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
    # Unit context fields for structured logging — populated once ctx is loaded.
    _log_ctx: dict[str, Any] = {"unit_id": unit_id}
    try:
        with get_postgres_session_sync() as session:
            ctx = SyncTaskBootstrap.load(session, unit_id)
            sync_run_id = ctx.sync_run_id
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
            unit.status = SyncRunUnitStatus.RUNNING.value
            unit.attempts = int(unit.attempts or 0) + 1
            unit.error = None
            session.flush()
            should_finalize = True
            _log_ctx = {
                "sync_run_id": ctx.sync_run_id,
                "unit_id": unit_id,
                "source_id": str(unit.source_id),
                "dataset_key": ctx.dataset_key,
                "provider": ctx.provider,
                "cost_class": ctx.cost_class,
            }

        logger.info("run_sync_unit.started", extra=_log_ctx)

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
            unit.status = SyncRunUnitStatus.SUCCESS.value
            unit.duration_seconds = duration_seconds
            unit.result = dict(result or {})
            unit.error = None
            if ctx.mode == SyncRunMode.INCREMENTAL.value:
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
            unit.status = SyncRunUnitStatus.FAILED.value
            unit.duration_seconds = duration_seconds
            unit.error = str(exc)
            unit.result = {"error_category": error_category}
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
        _dispatch_post_sync_tasks(
            provider=provider_for_dispatch,
            sync_targets=sorted(legacy_targets),
            org_id=run_org_id,
        )

    return {
        "status": "finalized",
        "sync_run_id": sync_run_id,
        "completed_units": success_count,
        "failed_units": failed_count,
        "post_sync_targets": sorted(legacy_targets),
    }


def _claim_units(session, run_uuid: uuid.UUID) -> list[SyncRunUnit]:
    """Atomically claim dispatchable units for a run.

    Fresh ``planned`` units are claimed with an atomic ``UPDATE ... RETURNING``
    so two concurrent ``dispatch_sync_run`` calls cannot both enqueue the same
    unit (no double-queue / duplicate provider writes). Stale ``dispatching``
    or ``running`` units (a worker died mid-flight) are reclaimed by age so a
    crash cannot leave the run unfinishable.
    """
    now = datetime.now(timezone.utc)
    claimed_ids: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(
                SyncRunUnit.sync_run_id == run_uuid,
                SyncRunUnit.status == SyncRunUnitStatus.PLANNED.value,
            )
            .values(status=SyncRunUnitStatus.DISPATCHING.value, updated_at=now)
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )

    stale_dispatch = now - timedelta(seconds=_stale_dispatch_seconds())
    stale_running = now - timedelta(seconds=_stale_running_seconds())
    reclaim_candidates = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnitStatus.RUNNING.value,
                }
            ),
        )
        .all()
    )
    for unit in reclaim_candidates:
        if unit.id in claimed_ids:
            continue
        threshold = (
            stale_dispatch
            if unit.status == SyncRunUnitStatus.DISPATCHING.value
            else stale_running
        )
        if _as_aware(unit.updated_at) <= threshold:
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
        return max(
            1, int(__import__("os").getenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "900"))
        )
    except ValueError:
        return 900


def _stale_running_seconds() -> int:
    try:
        return max(
            1,
            int(__import__("os").getenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "3600")),
        )
    except ValueError:
        return 3600


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
