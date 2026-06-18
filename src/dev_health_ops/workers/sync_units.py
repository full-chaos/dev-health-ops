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
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from celery import chord, group
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
            return {"status": "missing", "sync_run_id": sync_run_id}

        if not decision.allowed:
            completed_at = datetime.now(timezone.utc)
            run.status = SyncRunStatus.FAILED.value
            run.completed_at = completed_at
            run.error = decision.reason or "sync dispatch denied"
            run.result = {"capped_unit_ids": list(decision.capped_unit_ids)}
            session.flush()
            return {"status": "denied", "reason": run.error}

        units = _dispatchable_units(session, run_uuid)
        signatures = []
        for unit in units:
            dispatch_route = route(
                org_id=str(unit.org_id),
                provider=str(unit.provider),
                cost_class=str(unit.cost_class),
                cost_class_queues_enabled=_cost_class_queues_enabled(),
            )
            unit.status = SyncRunUnitStatus.DISPATCHING.value
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
        callback = getattr(finalize_sync_run, "si")(sync_run_id)
        callback.set(queue="sync")
        chord(group(signatures), callback).apply_async()
        return {"status": "dispatched", "queued_units": len(signatures)}

    getattr(finalize_sync_run, "apply_async")(args=(sync_run_id,), queue="sync")
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
    started_at = datetime.now(timezone.utc)
    try:
        with get_postgres_session_sync() as session:
            ctx = SyncTaskBootstrap.load(session, unit_id)
            sync_run_id = ctx.sync_run_id
            unit = _load_unit(session, unit_id)
            unit.status = SyncRunUnitStatus.RUNNING.value
            unit.attempts = int(unit.attempts or 0) + 1
            unit.error = None
            session.flush()

        runtime = _runtime_cache.get(ctx)
        result = run_dataset_unit(ctx, runtime)

        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        with get_postgres_session_sync() as session:
            unit = _load_unit(session, unit_id)
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
        return {
            "status": "success",
            "unit_id": unit_id,
            "duration_seconds": duration_seconds,
        }
    except Exception as exc:
        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
        with get_postgres_session_sync() as session:
            unit = _load_unit(session, unit_id)
            sync_run_id = str(unit.sync_run_id)
            unit.status = SyncRunUnitStatus.FAILED.value
            unit.duration_seconds = duration_seconds
            unit.error = str(exc)
            unit.result = None
            session.flush()
        logger.exception("Sync unit failed: unit_id=%s", unit_id)
        return {"status": "failed", "unit_id": unit_id, "error": str(exc)}
    finally:
        if sync_run_id is not None:
            getattr(finalize_sync_run, "apply_async")(args=(sync_run_id,), queue="sync")


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
            return {"status": "pending", "sync_run_id": sync_run_id}

        success_count = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.SUCCESS.value
        )
        failed_count = sum(
            1 for unit in units if unit.status == SyncRunUnitStatus.FAILED.value
        )
        completed_at = datetime.now(timezone.utc)
        run.completed_units = success_count
        run.failed_units = failed_count
        run.completed_at = run.completed_at or completed_at
        run.status = _aggregate_run_status(success_count, failed_count)
        run.result = {"completed_units": success_count, "failed_units": failed_count}
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
        if legacy_targets:
            _dispatch_post_sync_tasks(
                provider=provider_for_dispatch,
                sync_targets=sorted(legacy_targets),
                org_id=str(run.org_id),
            )
        session.flush()

    return {
        "status": "finalized",
        "sync_run_id": sync_run_id,
        "completed_units": success_count,
        "failed_units": failed_count,
        "post_sync_targets": sorted(legacy_targets),
    }


def _dispatchable_units(session, run_uuid: uuid.UUID) -> list[SyncRunUnit]:
    now = datetime.now(timezone.utc)
    stale_before = now - timedelta(seconds=_stale_dispatch_seconds())
    units = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.PLANNED.value,
                    SyncRunUnitStatus.DISPATCHING.value,
                }
            ),
        )
        .order_by(SyncRunUnit.id)
        .all()
    )
    return [
        unit
        for unit in units
        if unit.status == SyncRunUnitStatus.PLANNED.value
        or _as_aware(unit.updated_at) <= stale_before
    ]


def _load_unit(session, unit_id: str) -> SyncRunUnit:
    unit_uuid = uuid.UUID(str(unit_id))
    unit = session.query(SyncRunUnit).filter(SyncRunUnit.id == unit_uuid).one_or_none()
    if unit is None:
        raise ValueError(f"Sync run unit not found: {unit_id}")
    return unit


def _aggregate_run_status(success_count: int, failed_count: int) -> str:
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


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
