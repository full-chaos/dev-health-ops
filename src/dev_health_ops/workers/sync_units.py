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
    finalize_sync_run(run_id)         -> aggregates unit statuses; materializes
                                         post-sync metrics via the
                                         SyncRunPostDispatch/outbox ledger

Idempotency and Durability rules:
  * dispatch_sync_run is redispatchable: it only queues units still in
    planned/stale-dispatching state.
  * finalize_sync_run is a no-op until all units are terminal, and a no-op if
    the run's post-sync outbox row already exists. Each terminal unit enqueues
    finalize. Finalize itself enforces once-only via the unique
    (sync_run_id, kind) constraint on SyncRunPostDispatch.
  * Metrics are never dispatched from individual units. Post-sync durability
    flows through the sync_dispatch_outbox table and the reconciler relay,
    rather than only the SyncRunPostDispatch ledger. The post_sync kind is
    relayed at-most-once by the reconciler. It marks the outbox row dispatched
    before publishing and never re-arms on publish failure. This prevents
    downstream metrics readers from double-counting duplicate computed_at
    generations. Durable exactly-once post-sync is deferred to CHAOS-2596.
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
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict

from billiard.exceptions import SoftTimeLimitExceeded
from celery import chord, group
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.models import (
    BackfillJob,
    JobRun,
    JobRunStatus,
    SyncComputeCheckpoint,
    SyncComputeCheckpointStatus,
    SyncComputeType,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.budget import estimate_provider_budget
from dev_health_ops.sync.budget_guard import BudgetGuard
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_PENDING,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.dispatch_policy import route
from dev_health_ops.sync.guard import DispatchGuard
from dev_health_ops.sync.trigger_routing import (
    stamp_sync_run_canonical_config,
)
from dev_health_ops.sync.watermarks import set_watermark
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.post_sync_dispatch import build_post_sync_dispatch_payload
from dev_health_ops.workers.queues import _cost_class_queues_enabled
from dev_health_ops.workers.rate_limit_defer import plan_rate_limit_deferral
from dev_health_ops.workers.sync_bootstrap import (
    ProviderRuntimeCache,
    SyncTaskBootstrap,
    SyncTaskContext,
)
from dev_health_ops.workers.task_utils import _GIT_TARGETS, _WORK_ITEM_TARGETS

logger = logging.getLogger(__name__)
_runtime_cache = ProviderRuntimeCache()
_TERMINAL_RUN_STATUSES = {
    SyncRunStatus.SUCCESS.value,
    SyncRunStatus.PARTIAL_FAILED.value,
    SyncRunStatus.FAILED.value,
}
_WORK_ITEM_RESULT_OBSERVATION_FIELDS = (
    "linear_page_count",
    "linear_batch_count",
)
_LINEAR_BACKFILL_WORK_ITEM_DATASETS = frozenset(
    {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }
)
_LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES = frozenset(
    {
        "work_items",
        "work_item_transitions",
        "work_item_dependencies",
        "work_item_reopen_events",
        "work_item_interactions",
        "sprints",
        "ai_attribution",
        "work_item_metrics_daily",
        "work_item_user_metrics_daily",
        "work_item_cycle_times",
        "work_item_state_durations_daily",
        "work_item_team_attributions",
        "issue_type_metrics_daily",
        "investment_metrics_daily",
        "investment_classifications_daily",
    }
)
# CHAOS-2710 retry idempotency matrix. A Linear backfill unit's retry re-writes the
# COMPLETE window for every surface below; each is provably idempotent under a
# same-natural-key rewrite with a newer version (computed_at/last_synced):
#   work_items / transitions / reopen_events / interactions / sprints
#                                 -> RMT + reader-side FINAL or semantic-row dedupe (Phase 2)
#   work_item_dependencies        -> RMT(last_synced); loader reads FINAL, and the work-graph
#                                 builder's raw read only feeds work_graph_edges, itself an RMT
#                                 keyed on a deterministic edge_id hash so duplicate dependency
#                                 rows collapse to one persisted edge (no global FINAL needed)
#   ai_attribution                -> RMT(computed_at) + FINAL+ROW_NUMBER resolved view
#   work_item_metrics_daily       -> RMT(computed_at) (migration 055) + FINAL/argMax readers
#   work_item_user_metrics_daily  -> RMT(computed_at) (migration 055) + FINAL readers
#   work_item_cycle_times         -> RMT(computed_at) + argMax/FINAL readers
#   work_item_state_durations_daily -> argMax(duration, computed_at) readers over the key
#   work_item_team_attributions   -> latest-snapshot (max computed_at) + FINAL resolver
#   issue_type_metrics_daily      -> only read via SELECT DISTINCT (no aggregation)
#   investment_metrics_daily      -> argMax(col, computed_at) over the natural key in every
#                                 reader, incl. the analytics templates (compiler dedup CTE)
#   investment_classifications_daily -> no production reader (deterministic rule-based rows)
#   manual_attribution_fallbacks  -> RMT(updated_at) + FINAL reader (registry entry; this
#                                 job does not write it, but it is a proven-safe surface)
# Retry stays disabled for any unit whose write set is NOT a subset of this set.
_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES = frozenset(
    {
        "work_items",
        "work_item_transitions",
        "work_item_dependencies",
        "work_item_reopen_events",
        "work_item_interactions",
        "sprints",
        "ai_attribution",
        "work_item_metrics_daily",
        "work_item_user_metrics_daily",
        "work_item_cycle_times",
        "work_item_state_durations_daily",
        "work_item_team_attributions",
        "issue_type_metrics_daily",
        "investment_metrics_daily",
        "investment_classifications_daily",
        "manual_attribution_fallbacks",
    }
)


class _PendingUnitCounts(TypedDict):
    dispatchable: int
    in_flight: int
    next_deferred_at: datetime | None


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


def _budget_estimate_audit(
    ctx: SyncTaskContext, log_ctx: dict[str, Any]
) -> list[dict[str, Any]] | None:
    try:
        estimates = estimate_provider_budget(ctx)
    except Exception as exc:
        logger.warning(
            "run_sync_unit.budget_estimate_failed",
            extra={**log_ctx, "error": str(exc)},
        )
        return None
    if not estimates:
        return None
    return [estimate.to_dict() for estimate in estimates]


def _attach_budget_observation(
    result: dict[str, Any], budget_audit: list[dict[str, Any]] | None
) -> dict[str, Any]:
    if budget_audit is None:
        return result
    result_payload = dict(result)
    raw_observations = result_payload.get("observations")
    observations = (
        dict(raw_observations) if isinstance(raw_observations, Mapping) else {}
    )
    observations["budget_estimate"] = budget_audit
    result_payload["observations"] = observations
    return result_payload


def _promote_result_observation_fields(result: dict[str, Any]) -> dict[str, Any]:
    observations = result.get("observations")
    if not isinstance(observations, Mapping):
        return result
    for field_name in _WORK_ITEM_RESULT_OBSERVATION_FIELDS:
        if field_name in observations:
            result[field_name] = observations[field_name]
    return result


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
                sync_observers_for_terminal_sync_run(session, run)
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

        BudgetGuard.observe_run(session, sync_run_id, capped_unit_ids=capped_ids)
        budget_result = BudgetGuard.enforce_run(
            session, sync_run_id, capped_unit_ids=capped_ids
        )
        capped_ids = frozenset((*capped_ids, *budget_result.deferred_unit_ids))

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
            if budget_result.next_deferred_at is not None:
                _schedule_redispatch(
                    sync_run_id, available_at=budget_result.next_deferred_at
                )
            elif capped_ids:
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
        pending_counts = _pending_unit_counts(session, run_uuid_check)
    next_deferred_at = pending_counts["next_deferred_at"]
    if pending_counts["dispatchable"] > 0:
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
                "pending_units": pending_counts["dispatchable"],
            },
        )
        return {"status": "noop", "queued_units": 0}
    if pending_counts["in_flight"] > 0:
        logger.info(
            "dispatch_sync_run.waiting_inflight",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": 0,
                "in_flight_units": pending_counts["in_flight"],
            },
        )
        return {
            "status": "waiting_inflight",
            "queued_units": 0,
            "in_flight_units": pending_counts["in_flight"],
        }
    if next_deferred_at is not None:
        try:
            _schedule_redispatch(sync_run_id, available_at=next_deferred_at)
        except Exception as exc:
            logger.exception(
                "dispatch_sync_run.deferred_redispatch_publish_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
            )
            raise
        logger.info(
            "dispatch_sync_run.deferred",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": 0,
                "next_deferred_at": next_deferred_at.isoformat(),
            },
        )
        return {
            "status": "deferred",
            "queued_units": 0,
            "next_deferred_at": next_deferred_at.isoformat(),
        }
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
    deadline: datetime = started_at + timedelta(seconds=_max_unit_lifetime_seconds())
    terminal_txn_started = False
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None
    # Unit context fields for structured logging — populated once ctx is loaded.
    _log_ctx: dict[str, Any] = {"unit_id": unit_id}
    unit: SyncRunUnit | None = None
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
            deadline = started_at + timedelta(seconds=_max_unit_lifetime_seconds())
            lease_expires_at = min(
                started_at + timedelta(seconds=_running_lease_seconds()), deadline
            )
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

        heartbeat_stop, heartbeat_thread = _start_unit_heartbeat(
            unit_id, lease_owner, deadline
        )

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
                    lease_expires_at=min(
                        now + timedelta(seconds=_running_lease_seconds()), deadline
                    ),
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

        budget_audit = _budget_estimate_audit(ctx, _log_ctx)
        started_extra = dict(_log_ctx)
        if budget_audit is not None:
            started_extra["budget_estimate"] = budget_audit
        logger.info("run_sync_unit.started", extra=started_extra)

        runtime = _runtime_cache.get(ctx)
        if not _sync_unit_lease_is_owned_and_live(unit_id, lease_owner):
            logger.info(
                "run_sync_unit.lease_lost_before_dataset",
                extra={**_log_ctx},
            )
            return {
                "status": "skipped",
                "unit_id": unit_id,
                "reason": "lease_lost",
            }
        from dev_health_ops.metrics.job_work_items import (
            WorkItemsSyncLeaseLost,
            work_items_sync_lease_check,
        )

        try:
            with work_items_sync_lease_check(
                lambda _surface: _sync_unit_lease_is_owned_and_live(
                    unit_id, lease_owner
                )
            ):
                result = run_dataset_unit(ctx, runtime)
        except WorkItemsSyncLeaseLost as exc:
            logger.warning(
                "run_sync_unit.lease_lost_before_sink_write",
                extra={**_log_ctx, "surface": exc.surface},
            )
            return {
                "status": "skipped",
                "unit_id": unit_id,
                "reason": "lease_lost",
                "surface": exc.surface,
            }
        result_payload = _promote_result_observation_fields(
            _attach_budget_observation(dict(result or {}), budget_audit)
        )

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
                    result=result_payload,
                    error=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(terminal_result.rowcount or 0) == 0:
                logger.warning(
                    "run_sync_unit.success_stamp_noop",
                    extra={**_log_ctx},
                )
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
        logger.info(
            "run_sync_unit.success",
            extra={**_log_ctx, "duration_seconds": duration_seconds},
        )
        return {
            "status": "success",
            "unit_id": unit_id,
            "duration_seconds": duration_seconds,
        }
    except RateLimitException as exc:
        if terminal_txn_started:
            raise
        if unit is None:
            terminal_txn_started = True
            failure_result, should_finalize = _stamp_sync_unit_failed(
                unit_id=unit_id,
                sync_run_id=sync_run_id,
                lease_owner=lease_owner,
                started_at=started_at,
                exc=exc,
                log_ctx=_log_ctx,
            )
            return failure_result
        deferral = plan_rate_limit_deferral(
            retry_after_seconds=getattr(exc, "retry_after_seconds", None),
            attempts=unit.rate_limit_deferrals,
            first_seen_at=unit.rate_limit_first_seen_at.isoformat()
            if unit.rate_limit_first_seen_at
            else None,
        )
        if deferral is None:
            terminal_txn_started = True
            failure_result, should_finalize = _stamp_sync_unit_failed(
                unit_id=unit_id,
                sync_run_id=sync_run_id,
                lease_owner=lease_owner,
                started_at=started_at,
                exc=exc,
                log_ctx=_log_ctx,
            )
            return failure_result

        now = datetime.now(timezone.utc)
        not_before = datetime.fromisoformat(deferral.not_before)
        first_seen_at = datetime.fromisoformat(deferral.first_seen_at)
        terminal_txn_started = True
        with get_postgres_session_sync() as session:
            deferred_result: Any = session.execute(
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
                    status=SyncRunUnitStatus.RETRYING.value,
                    available_at=not_before,
                    rate_limit_deferrals=deferral.attempts,
                    rate_limit_first_seen_at=first_seen_at,
                    error=str(exc),
                    result={
                        "error_category": "rate_limit",
                        "retry_after_seconds": getattr(
                            exc, "retry_after_seconds", None
                        ),
                        "not_before": deferral.not_before,
                        "rate_limit_deferrals": deferral.attempts,
                    },
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=now,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if int(deferred_result.rowcount or 0) == 0:
                return {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                }
            if sync_run_id is not None:
                # Earlier-wins upsert (CHAOS-2647): we deliberately do NOT
                # force-set available_at=not_before here. A revived past dispatch
                # wakeup may be consumed as a no-op while all remaining units are
                # future RETRYING; the reconciler's periodic _dispatchable_run_ids
                # scan re-materializes dispatch once available_at <= now (bounded
                # delay, never stuck). Forcing not_before is unsafe: it would
                # overwrite the earlier countdown _schedule_redispatch arms for
                # capped PLANNED siblings, delaying their dispatch. The precision
                # loss is negligible versus provider rate-limit backoff windows.
                upsert_outbox_wakeup(
                    session,
                    sync_run_id=sync_run_id,
                    kind=OUTBOX_KIND_DISPATCH,
                    available_at=not_before,
                    now=now,
                )
            session.flush()
        logger.info(
            "run_sync_unit.rate_limited_deferred",
            extra={
                **_log_ctx,
                "not_before": deferral.not_before,
                "rate_limit_deferrals": deferral.attempts,
            },
        )
        return {
            "status": "rate_limited_deferred",
            "unit_id": unit_id,
            "not_before": deferral.not_before,
            "rate_limit_deferrals": deferral.attempts,
        }
    except SoftTimeLimitExceeded as exc:
        if terminal_txn_started:
            raise
        timeout_result, should_finalize = _stamp_sync_unit_soft_timeout(
            unit_id=unit_id,
            lease_owner=lease_owner,
            started_at=started_at,
            exc=exc,
            log_ctx=_log_ctx,
        )
        return timeout_result
    except Exception as exc:
        if terminal_txn_started:
            raise
        terminal_txn_started = True
        failure_result, should_finalize = _stamp_sync_unit_failed(
            unit_id=unit_id,
            sync_run_id=sync_run_id,
            lease_owner=lease_owner,
            started_at=started_at,
            exc=exc,
            log_ctx=_log_ctx,
        )
        return failure_result
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


def _sync_unit_lease_is_owned_and_live(
    unit_id: str,
    lease_owner: str | None,
) -> bool:
    from dev_health_ops.db import get_postgres_session_sync

    if lease_owner is None:
        return False
    now = datetime.now(timezone.utc)
    with get_postgres_session_sync() as session:
        unit = _load_unit(session, unit_id)
        if unit.status != SyncRunUnitStatus.RUNNING.value:
            return False
        run_status = (
            session.query(SyncRun.status)
            .filter(SyncRun.id == unit.sync_run_id)
            .scalar()
        )
        if run_status in _TERMINAL_RUN_STATUSES:
            return False
        return _unit_lease_is_owned_and_live(unit, lease_owner, now)


def _expired_lease_max_retries() -> int:
    try:
        return max(0, int(os.getenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "1")))
    except ValueError:
        return 1


def _expired_lease_retry_backoff_seconds() -> int:
    try:
        return max(
            0,
            int(os.getenv("SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS", "60")),
        )
    except ValueError:
        return 60


def _retry_surfaces_for_unit(unit: SyncRunUnit) -> frozenset[str]:
    if (
        str(unit.provider) == "linear"
        and str(unit.mode) == SyncRunMode.BACKFILL.value
        and str(unit.dataset_key) in _LINEAR_BACKFILL_WORK_ITEM_DATASETS
    ):
        return _LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES
    return frozenset()


def _sync_unit_expired_lease_retry_decision(unit: SyncRunUnit) -> dict[str, Any]:
    retry_count = int(unit.expired_lease_retry_count or 0)
    retry_surfaces = _retry_surfaces_for_unit(unit)
    base_eligible = (
        str(unit.provider) == "linear"
        and str(unit.mode) == SyncRunMode.BACKFILL.value
        and str(unit.dataset_key) in _LINEAR_BACKFILL_WORK_ITEM_DATASETS
        and bool(retry_surfaces)
        and retry_surfaces.issubset(_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES)
    )
    max_retries = _expired_lease_max_retries()
    exhausted = base_eligible and retry_count >= max_retries
    return {
        "should_retry": base_eligible and not exhausted,
        "retry_exhausted": exhausted,
        "retry_count": retry_count,
        "next_retry_count": retry_count + 1,
        "retry_surfaces": tuple(sorted(retry_surfaces)),
        "max_retries": max_retries,
    }


def _retry_result_payload(
    *,
    error_category: str,
    retry_reason: str,
    decision: dict[str, Any],
    next_retry_at: datetime | None,
    last_lease_expired_at: datetime | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error_category": error_category,
        "retry_count": decision["next_retry_count"],
        "retry_reason": retry_reason,
        "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
        "retry_exhausted": False,
        "retry_surfaces": list(decision["retry_surfaces"]),
    }
    if last_lease_expired_at is not None:
        payload["last_lease_expired_at"] = last_lease_expired_at.isoformat()
    return payload


def _failed_retry_result_payload(
    *,
    error_category: str,
    retry_reason: str,
    decision: dict[str, Any],
    last_lease_expired_at: datetime | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error_category": error_category,
        "retry_count": decision["retry_count"],
        "retry_reason": retry_reason,
        "next_retry_at": None,
        "retry_exhausted": bool(decision["retry_exhausted"]),
        "retry_surfaces": list(decision["retry_surfaces"]),
    }
    if last_lease_expired_at is not None:
        payload["last_lease_expired_at"] = last_lease_expired_at.isoformat()
    return payload


def _stamp_sync_unit_soft_timeout(
    *,
    unit_id: str,
    lease_owner: str | None,
    started_at: datetime,
    exc: SoftTimeLimitExceeded,
    log_ctx: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    from dev_health_ops.db import get_postgres_session_sync

    completed_at = datetime.now(timezone.utc)
    duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    with get_postgres_session_sync() as session:
        unit = _load_unit(session, unit_id)
        decision = _sync_unit_expired_lease_retry_decision(unit)
        if decision["should_retry"]:
            available_at = completed_at + timedelta(
                seconds=_expired_lease_retry_backoff_seconds()
            )
            result_payload = _retry_result_payload(
                error_category="soft_timeout",
                retry_reason="soft_timeout",
                decision=decision,
                next_retry_at=available_at,
                last_lease_expired_at=None,
            )
            retry_result: Any = session.execute(
                update(SyncRunUnit)
                .where(
                    SyncRunUnit.id == uuid.UUID(str(unit_id)),
                    SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnit.lease_owner == lease_owner,
                    SyncRunUnit.lease_owner.is_not(None),
                    SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
                )
                .values(
                    status=SyncRunUnitStatus.RETRYING.value,
                    available_at=available_at,
                    duration_seconds=duration_seconds,
                    error=str(exc),
                    result=result_payload,
                    expired_lease_retry_count=(
                        SyncRunUnit.expired_lease_retry_count + 1
                    ),
                    last_retry_reason="soft_timeout",
                    retry_exhausted_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=completed_at,
                    updated_at=completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            if int(retry_result.rowcount or 0) == 0:
                return (
                    {
                        "status": "skipped",
                        "unit_id": unit_id,
                        "reason": "lease_lost",
                    },
                    False,
                )
            session.flush()
            logger.warning(
                "run_sync_unit.soft_timeout_deferred",
                extra={
                    **log_ctx,
                    "duration_seconds": duration_seconds,
                    "retry_count": decision["next_retry_count"],
                    "next_retry_at": available_at.isoformat(),
                },
            )
            return (
                {
                    "status": "soft_timeout_deferred",
                    "unit_id": unit_id,
                    "error_category": "soft_timeout",
                    "retry_count": decision["next_retry_count"],
                    "next_retry_at": available_at.isoformat(),
                },
                False,
            )

        failed_payload = _failed_retry_result_payload(
            error_category="soft_timeout",
            retry_reason="soft_timeout",
            decision=decision,
            last_lease_expired_at=None,
        )
        failed_result: Any = session.execute(
            update(SyncRunUnit)
            .where(
                SyncRunUnit.id == uuid.UUID(str(unit_id)),
                SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                SyncRunUnit.lease_owner == lease_owner,
                SyncRunUnit.lease_owner.is_not(None),
                SyncRunUnit.sync_run_id.in_(_nonterminal_run_ids_select()),
            )
            .values(
                status=SyncRunUnitStatus.FAILED.value,
                available_at=None,
                duration_seconds=duration_seconds,
                error=str(exc),
                result=failed_payload,
                last_retry_reason="soft_timeout",
                retry_exhausted_at=completed_at
                if failed_payload["retry_exhausted"]
                else None,
                lease_owner=None,
                lease_expires_at=None,
                last_heartbeat_at=completed_at,
                updated_at=completed_at,
            )
            .execution_options(synchronize_session=False)
        )
        if int(failed_result.rowcount or 0) == 0:
            return (
                {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                },
                False,
            )
        session.flush()
    logger.warning(
        "run_sync_unit.soft_timeout_failed",
        extra={
            **log_ctx,
            "duration_seconds": duration_seconds,
            "error_category": "soft_timeout",
            "retry_exhausted": failed_payload["retry_exhausted"],
        },
    )
    return (
        {
            "status": "failed",
            "unit_id": unit_id,
            "error": str(exc),
            "error_category": "soft_timeout",
            "retry_exhausted": failed_payload["retry_exhausted"],
        },
        False,
    )


def _stamp_sync_unit_failed(
    *,
    unit_id: str,
    sync_run_id: str | None,
    lease_owner: str | None,
    started_at: datetime,
    exc: BaseException,
    log_ctx: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    from dev_health_ops.db import get_postgres_session_sync

    completed_at = datetime.now(timezone.utc)
    duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    error_category = _classify_error(exc)
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
            return (
                {
                    "status": "skipped",
                    "unit_id": unit_id,
                    "reason": "lease_lost",
                },
                False,
            )
        if sync_run_id is not None:
            upsert_outbox_wakeup(
                session,
                sync_run_id=sync_run_id,
                kind=OUTBOX_KIND_FINALIZE,
                available_at=completed_at,
                now=completed_at,
            )
        session.flush()
    logger.exception(
        "run_sync_unit.failed",
        extra={
            **log_ctx,
            "duration_seconds": duration_seconds,
            "error_category": error_category,
        },
    )
    return (
        {
            "status": "failed",
            "unit_id": unit_id,
            "error": str(exc),
            "error_category": error_category,
        },
        True,
    )


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.finalize_sync_run")
def finalize_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Aggregate unit statuses and materialize post-sync metrics once per run.

    No-op until all units are terminal; once-only via the SyncRunPostDispatch
    ledger. The reconciler relay is the sole post-sync publisher. post_sync is
    at-most-once: the relay marks dispatched before publishing and never re-arms
    on publish failure because downstream raw-aggregation readers can
    double-count duplicate computed_at generations. Durable exactly-once
    post-sync re-drive is deferred to CHAOS-2596; dispatch/finalize wakeups
    remain at-least-once because their consumers are idempotent.
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
        sync_observers_for_terminal_sync_run(session, run)
        try:
            _checkpoint_successful_compute_inputs(
                session, units, checkpointed_at=completed_at
            )
        except SQLAlchemyError as exc:
            logger.warning(
                "finalize_sync_run.compute_checkpoint_failed",
                extra={"sync_run_id": sync_run_id, "error": str(exc)},
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


def _checkpoint_successful_compute_inputs(
    session,
    units: list[SyncRunUnit],
    *,
    checkpointed_at: datetime,
) -> None:
    from dev_health_ops.sync.planner import map_datasets_to_legacy_targets

    work_graph_targets = _GIT_TARGETS | _WORK_ITEM_TARGETS
    for unit in units:
        if unit.status != SyncRunUnitStatus.SUCCESS.value:
            continue
        legacy_targets = map_datasets_to_legacy_targets(
            str(unit.provider), [str(unit.dataset_key)]
        )
        if not legacy_targets.intersection(work_graph_targets):
            continue
        checkpoint = SyncComputeCheckpoint(
            org_id=str(unit.org_id),
            sync_run_id=unit.sync_run_id,
            sync_run_unit_id=unit.id,
            source_id=unit.source_id,
            provider=str(unit.provider),
            dataset_key=str(unit.dataset_key),
            compute_type=SyncComputeType.WORK_GRAPH.value,
            status=SyncComputeCheckpointStatus.READY.value,
            window_start=unit.since_at,
            window_end=unit.before_at,
            checkpointed_at=checkpointed_at,
            checkpoint_metadata={
                "cost_class": str(unit.cost_class),
                "mode": str(unit.mode),
                "legacy_targets": sorted(legacy_targets),
            },
        )
        nested = session.begin_nested()
        try:
            session.add(checkpoint)
            session.flush()
        except IntegrityError:
            nested.rollback()
        except SQLAlchemyError as exc:
            nested.rollback()
            logger.warning(
                "finalize_sync_run.compute_checkpoint_unit_failed",
                extra={
                    "sync_run_id": str(unit.sync_run_id),
                    "unit_id": str(unit.id),
                    "compute_type": SyncComputeType.WORK_GRAPH.value,
                    "error": str(exc),
                },
            )
        else:
            nested.commit()


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


def _pending_unit_counts(session, run_uuid: uuid.UUID) -> _PendingUnitCounts:
    now = datetime.now(timezone.utc)
    stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds())
    units = (
        session.query(
            SyncRunUnit.status, SyncRunUnit.updated_at, SyncRunUnit.available_at
        )
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.PLANNED.value,
                    SyncRunUnitStatus.DISPATCHING.value,
                    SyncRunUnitStatus.RUNNING.value,
                    SyncRunUnitStatus.RETRYING.value,
                }
            ),
        )
        .all()
    )
    dispatchable = 0
    in_flight = 0
    next_deferred_at: datetime | None = None
    for status, updated_at, available_at in units:
        if status == SyncRunUnitStatus.PLANNED.value:
            dispatchable += 1
        elif status == SyncRunUnitStatus.DISPATCHING.value:
            if (
                updated_at is not None
                and _as_aware(updated_at) <= stale_dispatch_cutoff
            ):
                dispatchable += 1
            else:
                in_flight += 1
        elif status == SyncRunUnitStatus.RUNNING.value:
            in_flight += 1
        elif available_at is not None:
            deferred_at = _as_aware(available_at)
            if deferred_at <= now:
                dispatchable += 1
            elif next_deferred_at is None or deferred_at < next_deferred_at:
                next_deferred_at = deferred_at
    return {
        "dispatchable": dispatchable,
        "in_flight": in_flight,
        "next_deferred_at": next_deferred_at,
    }


def sync_observers_for_terminal_sync_run(session, run: SyncRun) -> None:
    if run.status not in _TERMINAL_RUN_STATUSES:
        return
    completed_at = run.completed_at or datetime.now(timezone.utc)
    run.completed_at = completed_at
    success = run.status == SyncRunStatus.SUCCESS.value
    run_result = run.result if isinstance(run.result, dict) else {}
    cancelled = bool(run_result.get("cancelled"))
    job_run_status = (
        JobRunStatus.SUCCESS.value
        if success
        else JobRunStatus.CANCELLED.value
        if cancelled
        else JobRunStatus.FAILED.value
    )
    backfill_status = "completed" if success else "cancelled" if cancelled else "failed"
    error = None if success else (run.error or "Sync run completed with failed units")
    result_patch = {
        "sync_run_status": run.status,
        "total_units": int(run.total_units or 0),
        "completed_units": int(run.completed_units or 0),
        "failed_units": int(run.failed_units or 0),
        **({"cancelled": True} if cancelled else {}),
    }

    marker = f"sync_run:{run.id}"
    backfill_jobs = (
        session.query(BackfillJob)
        .filter(BackfillJob.org_id == str(run.org_id))
        .filter(BackfillJob.celery_task_id.contains(marker))
        .all()
    )
    for job in backfill_jobs:
        job.status = backfill_status
        job.total_chunks = int(run.total_units or 0)
        job.completed_chunks = int(run.completed_units or 0)
        job.failed_chunks = int(run.failed_units or 0)
        job.completed_at = completed_at
        job.error_message = error

    job_runs = (
        session.query(JobRun)
        .filter(
            JobRun.status.in_({JobRunStatus.PENDING.value, JobRunStatus.RUNNING.value})
        )
        .all()
    )
    for job_run in job_runs:
        result = job_run.result if isinstance(job_run.result, dict) else {}
        if str(result.get("sync_run_id") or "") != str(run.id):
            continue
        job_run.status = job_run_status
        job_run.completed_at = completed_at
        job_run.error = error
        job_run.result = {**result, **result_patch}


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

    Fresh ``planned`` units and due ``retrying`` units are claimed with atomic
    ``UPDATE ... RETURNING`` statements so two concurrent ``dispatch_sync_run``
    calls cannot both enqueue the same unit (no double-queue / duplicate provider
    writes).  Stale ``dispatching`` units (a worker died before the unit started
    running) are reclaimed by age.

    F2: RUNNING units are NEVER reclaimed by the dispatch path — re-dispatching a
    RUNNING unit would run it a second time concurrently and cause duplicate
    provider writes.  Durable dead-worker recovery is handled instead by
    ``reconcile_sync_dispatch``, which fails a RUNNING unit once its
    ``lease_expires_at`` lapses and re-arms dispatch/finalize.  ``run_sync_unit``
    renews that lease via a heartbeat bounded by an absolute deadline
    (``SYNC_UNIT_MAX_LIFETIME_SECONDS``), so even a wedged-but-alive worker's lease
    eventually lapses and the unit is reclaimed (CHAOS-2705).

    ``capped_ids`` is the set of unit IDs that the concurrency guard deferred.
    Those units are left in PLANNED status so a later redispatch can claim them
    once slots free up.  Due RETRYING units obey the same cap exclusion.
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

    retrying_where = [
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.status == SyncRunUnitStatus.RETRYING.value,
        SyncRunUnit.available_at.is_not(None),
        SyncRunUnit.available_at <= now,
    ]
    if capped_ids:
        retrying_where.append(
            ~SyncRunUnit.id.in_([uuid.UUID(cid) for cid in capped_ids])
        )
    due_retrying: set[uuid.UUID] = set(
        session.execute(
            update(SyncRunUnit)
            .where(*retrying_where)
            .values(
                status=SyncRunUnitStatus.DISPATCHING.value,
                updated_at=now,
                available_at=None,
            )
            .returning(SyncRunUnit.id)
            .execution_options(synchronize_session=False)
        )
        .scalars()
        .all()
    )
    claimed_ids.update(due_retrying)

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


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _running_lease_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_UNIT_RUNNING_LEASE_SECONDS", "300")))
    except ValueError:
        return 300


def _heartbeat_interval_seconds() -> int:
    return max(1, min(60, _running_lease_seconds() // 4))


def _max_unit_lifetime_seconds() -> int:
    """Absolute cap on how long a heartbeat may renew a unit's lease.

    Floored at 3600 (the Celery hard task_time_limit) so a misconfigured value
    cannot prematurely expire a still-progressing unit.  The heartbeat will stop
    renewing once this deadline is reached, allowing the reconciler to reclaim.
    """
    try:
        return max(3600, int(os.getenv("SYNC_UNIT_MAX_LIFETIME_SECONDS", "3720")))
    except ValueError:
        return 3720


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
    deadline: datetime,
) -> tuple[threading.Event, threading.Thread] | tuple[None, None]:
    if lease_owner is None:
        return None, None
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_unit_lease,
        args=(unit_id, lease_owner, stop_event, deadline),
        name=f"sync-unit-heartbeat-{unit_id}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _heartbeat_unit_lease(
    unit_id: str,
    lease_owner: str,
    stop_event: threading.Event,
    deadline: datetime,
) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    interval = _heartbeat_interval_seconds()
    lease_seconds = _running_lease_seconds()
    while not stop_event.wait(interval):
        now = datetime.now(timezone.utc)
        if now >= deadline:
            logger.warning(
                "run_sync_unit.heartbeat_deadline_exceeded",
                extra={"unit_id": unit_id},
            )
            stop_event.set()
            break
        lease_expires_at = min(now + timedelta(seconds=lease_seconds), deadline)
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


def _schedule_redispatch(
    sync_run_id: str, *, available_at: datetime | None = None
) -> None:
    try:
        from dev_health_ops.db import get_postgres_session_sync

        countdown = int(os.getenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60"))
        now = datetime.now(timezone.utc)
        redispatch_at = available_at or now + timedelta(seconds=countdown)
        with get_postgres_session_sync() as session:
            upsert_outbox_wakeup(
                session,
                sync_run_id=sync_run_id,
                kind=OUTBOX_KIND_DISPATCH,
                available_at=redispatch_at,
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
                    available_at=redispatch_at,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            session.flush()
        logger.info(
            "dispatch_sync_run.redispatch_rearmed",
            extra={
                "sync_run_id": sync_run_id,
                "countdown": countdown,
                "available_at": redispatch_at.isoformat(),
            },
        )
    except Exception:
        logger.exception(
            "dispatch_sync_run.redispatch_rearm_failed",
            extra={"sync_run_id": sync_run_id},
        )
