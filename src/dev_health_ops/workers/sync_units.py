"""Sync run dispatch + finalize contract (CHAOS-2512).

FROZEN CONTRACT — the two remaining Celery entrypoints of the fan-out
execution model, each wrapped with the ``@app.task`` decorator. They take IDs
ONLY (no credentials, no DTOs) in their payloads. Per-unit execution
(formerly ``run_sync_unit``) is native Go now (``internal/jobs/providerunit``,
routed via the durable ``sync.provider_unit`` outbox) -- there is no Celery
consumer or HTTP bridge for it any more, so this module only dispatches units
into that outbox and finalizes the run once every unit reaches a terminal
state.

Pipeline:
    plan_sync_run (CHAOS-2511)        -> persists SyncRun + units (status=planned)
    dispatch_sync_run(run_id)         -> DispatchGuard.authorize_run, then routes
                                         + queues each unit independently
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
    relayed guarded at-least-once: the reconciler terminally marks it only
    after scheduling succeeds, and a failure releases the claim with bounded
    backoff. CHAOS-2596 made supported downstream readers generation-safe, so
    a duplicate compute generation cannot inflate their results.
Observability (CHAOS-2519):
  Every structured log line emitted by these tasks carries the full unit
  context: sync_run_id, unit_id, source_id, dataset_key, provider, cost_class.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, SessionTransactionOrigin

from dev_health_ops.api.services.sync_coverage import (
    invalidate_sync_coverage_projection_sync,
)
from dev_health_ops.jobs.contracts import ProviderUnitPayload
from dev_health_ops.jobs.outbox import enqueue_worker_job
from dev_health_ops.jobs.routes import (
    PROVIDER_UNIT_OUTBOX_ROUTES,
    WorkerJobRouteError,
    resolve_worker_job_route,
)
from dev_health_ops.models import (
    BackfillJob,
    Integration,
    JobRun,
    JobRunStatus,
    SyncComputeCheckpoint,
    SyncComputeCheckpointStatus,
    SyncComputeType,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunPostDispatch,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.budget_guard import BudgetGuard
from dev_health_ops.sync.canonical_incident_gate import (
    FEATURE_DISABLED_ERROR_CATEGORY,
    CanonicalIncidentFeatureDisabledError,
    require_canonical_incident_feature_for_update_sync,
    sync_run_requires_canonical_incident_feature,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_DISPATCHED,
    OUTBOX_STATUS_PENDING,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.error_sanitize import sanitize_error_text
from dev_health_ops.sync.feature_denial import (
    FeatureDisabledRunTransition,
    terminalize_feature_disabled_run,
)
from dev_health_ops.sync.guard import DispatchGuard
from dev_health_ops.sync.trigger_routing import (
    stamp_sync_run_canonical_config,
)
from dev_health_ops.sync.zero_unit_telemetry import ZERO_UNIT_FINALIZATIONS_TOTAL
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.post_sync_dispatch import build_post_sync_dispatch_payload
from dev_health_ops.workers.provider_family_contract import (
    validate_provider_family_claim,
)
from dev_health_ops.workers.provider_unit_route import (
    is_atomic_provider_family_direct_alias,
    is_fold_family_direct_alias,
    routes_to_river,
)
from dev_health_ops.workers.task_utils import _GIT_TARGETS, _WORK_ITEM_TARGETS

logger = logging.getLogger(__name__)
_TERMINAL_RUN_STATUSES = {
    SyncRunStatus.SUCCESS.value,
    SyncRunStatus.PARTIAL_FAILED.value,
    SyncRunStatus.FAILED.value,
}


class _PendingUnitCounts(TypedDict):
    dispatchable: int
    in_flight: int
    next_deferred_at: datetime | None


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.dispatch_sync_run")
def dispatch_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Authorize, route, and queue all pending units of a planned run.

    Idempotent / redispatchable. Implemented in CHAOS-2512.
    """

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.workers.reference_discovery import (
        ensure_reference_discovery_wakeup,
        reference_discovery_succeeded,
    )

    river_queued = 0
    with get_postgres_session_sync() as session:
        # The provider-unit outbox row and DISPATCHING claim must share one
        # explicit transaction. A process death therefore commits both or
        # neither, closing the producer kill window without serializing
        # credentials, callables, or route configuration into River.
        transaction = session.get_transaction()
        if (
            transaction is not None
            and transaction.origin is SessionTransactionOrigin.AUTOBEGIN
        ):
            # A task-owned fresh session begins implicitly when it first reads
            # its run. Persist that read/fixture transaction rather than
            # rolling it back: rollback can erase a just-planned run before
            # dispatch sees it. The explicit producer transaction below then
            # fences the unit claim and outbox row together.
            session.commit()
        if not session.in_transaction():
            session.begin()
        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            logger.warning(
                "dispatch_sync_run.missing",
                extra={"sync_run_id": sync_run_id},
            )
            return {"status": "missing", "sync_run_id": sync_run_id}
        requires_canonical_feature = sync_run_requires_canonical_incident_feature(
            session, run
        )
        if requires_canonical_feature:
            try:
                require_canonical_incident_feature_for_update_sync(session, run.org_id)
            except CanonicalIncidentFeatureDisabledError as exc:
                transition = terminalize_feature_disabled_run(session, run, exc)
                if transition.run_terminal:
                    _terminalize_feature_disabled_graph(
                        session,
                        run,
                        exc,
                    )
                else:
                    _arm_feature_disabled_finalize(
                        session,
                        run,
                        datetime.now(timezone.utc),
                    )
                session.flush()
                session.commit()
                logger.warning(
                    "dispatch_sync_run.feature_disabled",
                    extra={
                        "sync_run_id": str(run.id),
                        "org_id": str(run.org_id),
                        "error_category": FEATURE_DISABLED_ERROR_CATEGORY,
                        "running_units": transition.running_units,
                    },
                )
                return {
                    "status": FEATURE_DISABLED_ERROR_CATEGORY,
                    "sync_run_id": sync_run_id,
                    "dispatched": 0,
                    "failed_units": transition.failed_units,
                }
        if not reference_discovery_succeeded(session, run_uuid):
            now = datetime.now(timezone.utc)
            ensure_reference_discovery_wakeup(session, run_uuid, now=now)
            session.flush()
            logger.info(
                "dispatch_sync_run.blocked_on_reference_discovery",
                extra={"sync_run_id": sync_run_id},
            )
            return {
                "status": "blocked_on_reference_discovery",
                "sync_run_id": sync_run_id,
            }

        decision = DispatchGuard.authorize_run(session, sync_run_id)

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
                # No unit is DISPATCHING/RUNNING, so every remaining
                # non-terminal unit (PLANNED / RETRYING) can never legally
                # dispatch again — the guard re-denies every redispatch.
                # Fail them NOW: leaving them stranded under a terminal run
                # is invisible to the reconciler (it skips terminal runs)
                # and pollutes coverage as permanent requested-but-uncovered
                # windows.
                completed_at = datetime.now(timezone.utc)
                failed_planned = _fail_planned_units(session, run_uuid, error)
                run.status = SyncRunStatus.FAILED.value
                run.completed_at = completed_at
                run.error = error
                run.failed_units = int(run.failed_units or 0) + failed_planned
                run.result = {"capped_unit_ids": list(decision.capped_unit_ids)}
                sync_observers_for_terminal_sync_run(session, run)
                session.flush()
                logger.warning(
                    "dispatch_sync_run.denied",
                    extra={
                        "sync_run_id": sync_run_id,
                        "reason": run.error,
                        "failed_planned_units": failed_planned,
                    },
                )
                return {
                    "status": "denied",
                    "reason": run.error,
                    "failed_planned_units": failed_planned,
                }

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
        # CHAOS-3465: slot_headroom is what lets the budget guard spend a
        # leftover budget on units an EARLIER deferral is still holding back
        # without stepping around this pass's concurrency cap. Surplus retry
        # is disabled outright if it is absent, so this is the wiring that
        # makes the feature exist -- not a hint it can do without.
        budget_result = BudgetGuard.enforce_run(
            session,
            sync_run_id,
            capped_unit_ids=capped_ids,
            slot_headroom=decision.slot_headroom,
        )
        capped_ids = frozenset((*capped_ids, *budget_result.deferred_unit_ids))

        # CHAOS-2760 TOCTOU closure (review finding): enforce_run's cooldown
        # snapshot can go stale by the time we reach the claim below —
        # budget admission does real DB work (re-estimating every active
        # unit in the bucket) in between, during which a sibling unit's 429
        # can commit a brand-new observation this pass never saw. Re-check
        # once more, right here, as the LAST read before the atomic claim —
        # reusing the estimates enforce_run already computed, no
        # re-estimation / credential decryption. reconfirm_cooldowns fully
        # defers/terminalizes any match it catches (same write path
        # enforce_run's own cooldown loop uses) — a bare exclusion here
        # would leave the unit PLANNED with no deferral-budget bookkeeping
        # and livelock the run on a bare ~60s redispatch countdown (review
        # finding, round 2).
        reconfirm_result = BudgetGuard.reconfirm_cooldowns(
            session,
            sync_run_id,
            units=budget_result.candidate_units,
            estimates_by_unit=budget_result.estimates_by_unit,
            already_excluded_ids=capped_ids,
            jitter_seconds=budget_result.jitter_seconds,
            # CHAOS-3465 review (CRITICAL): candidate_units now includes units
            # the surplus phase pulled forward. Without their pre-promotion
            # available_at, a cooldown landing in this window would deferral-
            # stamp them and wipe the budget episode that was the whole reason
            # they were deferred -- the guard's own offer, withdrawn, costing
            # the unit its CHAOS-3412 exhaustion evidence.
            surplus_prior_available_at=budget_result.surplus_prior_available_at,
        )
        capped_ids = frozenset((*capped_ids, *reconfirm_result.excluded_unit_ids))
        next_deferred_at = budget_result.next_deferred_at
        if reconfirm_result.next_deferred_at is not None and (
            next_deferred_at is None
            or reconfirm_result.next_deferred_at < next_deferred_at
        ):
            next_deferred_at = reconfirm_result.next_deferred_at

        # CHAOS-4054 step 4 deleted the Celery dispatch plane, but NOT the
        # durable route control plane -- that is CHAOS-4082, still open. So
        # this producer still resolves the route, and still holds the FOR
        # SHARE lock it returns through the claim and the outbox commit.
        #
        # The lock is not ceremony. An operator rollback takes FOR UPDATE on
        # this same row, so holding it here is what stops the rollback
        # reporting quiescence while a producer that observed the OLD route is
        # still staging work (see resolve_worker_job_route's own docstring,
        # and the sync-provider quiescer hardened under CHAOS-3929).
        #
        # What DID change is the answer when River does not own the route.
        # There is no second runtime left to fall through to, so a non-River
        # route is a fail-closed route fault rather than a Celery dispatch.
        # Staging anyway would be worse than either: the Go relay resolves
        # this row every step and RELEASES the claim for a Celery route, so
        # the unit would sit in `dispatching` behind an outbox row nothing
        # delivers, holding a DispatchGuard slot with no lease to expire --
        # the exact wedge class CHAOS-3990 exists to prevent.
        provider_unit_route = resolve_worker_job_route(session, "sync.provider_unit")
        if provider_unit_route not in PROVIDER_UNIT_OUTBOX_ROUTES:
            raise WorkerJobRouteError(
                "sync.provider_unit route does not select the River outbox"
            )
        units = _claim_units(session, run_uuid, capped_ids=capped_ids)
        unroutable_units: list[SyncRunUnit] = []
        for unit in units:
            unit_provider = str(unit.provider)
            unit_dataset = str(unit.dataset_key)
            # Atomic provider families are admitted before transport selection,
            # so a malformed claim can reach neither River nor rollback Celery.
            # CHAOS-4054: every atomic family is validated strictly. Strictness
            # used to be gated on each provider's route switch, which meant a
            # malformed persisted claim was admitted purely because a
            # deployment had not turned the route on.
            if not validate_provider_family_claim(
                unit_provider,
                unit_dataset,
                unit.processor_flags,
                strict_atomic=True,
            ):
                # A non-canonical FOLD_CONTRIBUTING alias (CHAOS-4078:
                # github/tests, pr-reviews, pr-comments and their gitlab
                # equivalents) is "malformed" per the family contract, but it
                # is not the atomic "a corrupted claim could reach provider
                # execution" hazard that error guards against -- the
                # capability matrix never marks an alias plannable, so
                # routes_to_river below fails it closed regardless. CHAOS-3990
                # pins graceful per-unit termination for exactly this shape
                # (a stale or pre-fold-legacy alias unit reaching dispatch);
                # raising here would abort THIS ENTIRE RUN's dispatch over one
                # unroutable unit, reintroducing the stranding class CHAOS-3990
                # exists to prevent. Fall through to the unroutable path below.
                if not is_fold_family_direct_alias(unit_provider, unit_dataset):
                    raise WorkerJobRouteError(
                        "provider-unit claim requires the complete canonical family"
                    )
                unroutable_units.append(unit)
                continue
            # Routability is decided per pair, not per run, and the capability
            # matrix is its only source: a pair is executable when the matrix
            # marks it route-ready AND plannable (the canonical writer identity
            # of its family). There is no second runtime and no environment
            # switch left to consult -- CHAOS-4054 deleted both planes.
            if routes_to_river(unit_provider, unit_dataset):
                # The `continue` is the one-writer fence: an admitted unit is
                # staged in the durable outbox, and nothing else may claim it.
                enqueue_worker_job(
                    session,
                    ProviderUnitPayload(unit_id=str(unit.id)),
                    correlation_id=f"sync-run:{run.id}",
                    idempotency_key=f"sync.provider_unit:{unit.id}",
                    domain_id=str(unit.id),
                    organization_id=str(unit.org_id),
                )
                river_queued += 1
                continue
            # The matrix does not route this pair and there is no fallback
            # runtime to publish it to. Publishing into a queue with no
            # consumer is what wedged 27 units for 90 minutes in production;
            # terminalize instead, so finalize can aggregate the run and the
            # DispatchGuard budget is released rather than held forever.
            unroutable_units.append(unit)

        if unroutable_units:
            terminalized = _terminalize_unroutable_units(session, unroutable_units)
            logger.warning(
                "dispatch_sync_run.unroutable_units_terminalized",
                extra={
                    "sync_run_id": sync_run_id,
                    "unroutable_units": terminalized,
                    "error_category": FEATURE_DISABLED_ERROR_CATEGORY,
                    "pairs": sorted(
                        {
                            f"{unit.provider}/{unit.dataset_key}"
                            for unit in unroutable_units
                        }
                    ),
                },
            )

        if river_queued:
            now = datetime.now(timezone.utc)
            run.status = SyncRunStatus.DISPATCHING.value
            run.started_at = run.started_at or now
            session.flush()
        else:
            session.flush()

    if river_queued:
        if next_deferred_at is not None:
            _schedule_redispatch(sync_run_id, available_at=next_deferred_at)
        elif capped_ids:
            _schedule_redispatch(sync_run_id)
        logger.info(
            "dispatch_sync_run.dispatched",
            extra={
                "sync_run_id": sync_run_id,
                "queued_units": river_queued,
                "celery_units": 0,
                "river_units": river_queued,
            },
        )
        return {"status": "dispatched", "queued_units": river_queued}

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


@celery_app.task(queue="sync", name="dev_health_ops.workers.tasks.finalize_sync_run")
def finalize_sync_run(sync_run_id: str) -> dict[str, Any]:
    """Aggregate unit statuses and materialize post-sync metrics once per run.

    No-op until all units are terminal; once-only via the SyncRunPostDispatch
    ledger. The reconciler relay is the sole post-sync publisher. post_sync is
    at-least-once: a publish failure releases its guarded outbox claim for a
    bounded re-drive. Downstream readers select the newest compute generation
    per logical key, so duplicate deliveries cannot inflate supported metrics.
    """

    from dev_health_ops.db import get_postgres_session_sync

    # Captured inside the transaction, emitted AFTER it commits. See the
    # assignment site for why the increment cannot live where it is decided.
    zero_unit_labels: tuple[str, str] | None = None

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
        error_category = next(
            (
                unit.result.get("error_category")
                for unit in units
                if unit.status == SyncRunUnitStatus.FAILED.value
                and isinstance(unit.result, dict)
                and unit.result.get("error_category")
            ),
            None,
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
        if error_category is not None:
            result_payload["error_category"] = error_category
        if error_category == FEATURE_DISABLED_ERROR_CATEGORY and run.error is None:
            run.error = next(
                (
                    unit.error
                    for unit in units
                    if unit.status == SyncRunUnitStatus.FAILED.value and unit.error
                ),
                None,
            )
        if total_count == 0:
            # CHAOS-4159. A zero-unit run still finalizes FAILED -- that trade is
            # ratified (see
            # tests/test_sync_units.py::
            # test_fully_caught_up_plan_finalizes_failed_not_silently_successful:
            # a loud, honest failure beats a quiet success that falsely claims
            # coverage was refreshed). What is NOT ratified is finalizing it
            # ANONYMOUSLY.
            #
            # Finalize cannot see why the plan was empty -- by definition it has
            # no units to read a cause off. The planner can, and some planner
            # paths already write one onto the run before dispatch (the
            # PagerDuty terminalization writes `error` plus a
            # `result.error_category`). Overwriting those unconditionally, as
            # this branch used to, destroyed the only diagnosis that existed
            # and left three different causes -- a missing PagerDuty
            # credential, a disabled sync target, a genuinely empty plan --
            # wearing one identical label. An operator reading
            # `sync_runs.error` then has no way to tell "attach a credential"
            # from "this is expected".
            #
            # So: the planner's recorded cause WINS, and the generic label is
            # the residual for "nobody recorded one". The distinction is
            # carried explicitly on the run row, never inferred from
            # total_units.
            planner_result = run.result if isinstance(run.result, dict) else {}
            planner_category = planner_result.get("error_category")
            if isinstance(planner_category, str) and planner_category:
                result_payload.setdefault("error_category", planner_category)
            zero_unit_reason = _zero_unit_reason(planner_result)
            # Blank counts as absent, the same rule _zero_unit_reason applies
            # to the reason one column over: preserving "" would leave a FAILED
            # run showing no cause at all, which reads as "nothing to say"
            # rather than "not captured".
            if run.error is None or not run.error.strip():
                run.error = _ZERO_UNIT_GENERIC_ERROR
            else:
                # The generic literal this branch used to write unconditionally
                # was, by accident, also a scrub: whatever a planner (or a
                # pre-sanitization row from an older release) had left in
                # sync_runs.error was thrown away before anyone could read it.
                # Preserving the cause removes that accidental scrub, and
                # sync_runs.error is served raw to operators through the admin
                # job-history endpoint -- the `run_error` sanitization below
                # only guards the COPY into SyncConfiguration.last_sync_error,
                # not the column itself. So sanitize what we keep, at the point
                # we decide to keep it. Idempotent on already-clean text; same
                # discipline, and same reasoning, as the CHAOS-2766 comment
                # immediately below.
                run.error = sanitize_error_text(run.error)
            result_payload["reason"] = zero_unit_reason
        run.result = result_payload
        run_success = run.status == SyncRunStatus.SUCCESS.value
        # sanitize_error_text is applied here too, not just at the original
        # write site: run.error is copied straight into
        # SyncConfiguration.last_sync_error below (another variable-to-column
        # assignment an str(exc)-focused AST guard can't see), and a row
        # written before this column was brought under sanitize_error_text's
        # discipline could still carry raw credential text (CHAOS-2766 codex
        # review finding, round 2 -- same class as
        # sync_observers_for_terminal_sync_run below). Idempotent/harmless on
        # already-sanitized text.
        run_error = (
            None
            if run_success
            else sanitize_error_text(
                run.error or "Sync run completed with failed units"
            )
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
            invalidate_sync_coverage_projection_sync(
                session,
                str(run.org_id),
                integration_id=run.integration_id,
            )
            upsert_outbox_wakeup(
                session,
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_POST_SYNC,
                available_at=completed_at,
                now=completed_at,
            )
            nested.commit()
            if total_count == 0:
                # CHAOS-4159, standing telemetry order. Emitted from the
                # once-only branch -- the IntegrityError arm above is a
                # re-finalization of a run that already terminalized, and
                # counting it again would turn retry pressure into what looks
                # like more zero-unit runs. The `reason` label is the
                # classification this finalization actually recorded, so a
                # series dominated by the generic residual is itself the
                # signal that an upstream planner path is still discarding
                # its own diagnosis.
                #
                # DECIDED here, not incremented here. `nested.commit()` only
                # releases a savepoint; the outer transaction commits when the
                # session context exits, and anything between here and there
                # can still raise. A counter incremented inside a transaction
                # that then rolls back is wrong in the one direction that
                # matters for an alert threshold -- N failed attempts plus one
                # eventual success would publish N+1 for a single durable
                # finalization. So capture the labels and defer the increment
                # to after the commit.
                zero_unit_labels = (
                    _run_provider(session, run),
                    str(result_payload.get("reason", _ZERO_UNIT_GENERIC_REASON)),
                )
        post_sync_payload = build_post_sync_dispatch_payload(session, run_uuid)
        post_sync_targets = (
            post_sync_payload.sync_targets if post_sync_payload is not None else []
        )
        session.flush()

    if zero_unit_labels is not None:
        provider_label, reason_label = zero_unit_labels
        ZERO_UNIT_FINALIZATIONS_TOTAL.labels(
            provider=provider_label, reason=reason_label
        ).inc()

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


def _family_dataset_audit_metadata(unit: SyncRunUnit) -> dict[str, list[str]]:
    """CHAOS-2721: a collapsed work-item-family unit records only its canonical
    "work-items" dataset_key, which would hide that labels/projects/history/
    comments also ran. Surface the enabled family datasets in the compute
    checkpoint metadata so admin/API/debug views keep per-dataset provenance.
    """
    from dev_health_ops.sync.planner import family_dataset_keys_from_flags

    family_keys = family_dataset_keys_from_flags(unit.processor_flags)
    if not family_keys:
        return {}
    return {"family_datasets": family_keys}


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
                **_family_dataset_audit_metadata(unit),
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
    job_run_status = (
        JobRunStatus.SUCCESS.value if success else JobRunStatus.FAILED.value
    )
    backfill_status = "completed" if success else "failed"
    # sanitize_error_text is applied here too, not just at the original
    # write site: this function copies SyncRun.error directly into
    # BackfillJob.error_message / JobRun.error (a variable-to-column
    # assignment the str(exc)-focused AST guard can't see), and a row
    # written before this column was brought under sanitize_error_text's
    # discipline could still carry raw credential text (CHAOS-2766 codex
    # review finding, round 2). Idempotent/harmless on already-sanitized
    # text.
    error = (
        None
        if success
        else sanitize_error_text(run.error or "Sync run completed with failed units")
    )
    result_patch = {
        "sync_run_status": run.status,
        "total_units": int(run.total_units or 0),
        "completed_units": int(run.completed_units or 0),
        "failed_units": int(run.failed_units or 0),
    }
    run_result = run.result if isinstance(run.result, dict) else {}
    error_category = run_result.get("error_category")
    if error_category is not None:
        result_patch["error_category"] = error_category

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
    """Fail every unit of the run that is not dispatched and never will be.

    Covers PLANNED and RETRYING: on a total-cap hard deny the guard re-denies
    every future redispatch, so a deferred RETRYING unit is just as stranded
    as a PLANNED one — and a lingering RETRYING unit blocks finalize_sync_run
    (it requires all units terminal) forever.
    """
    now = datetime.now(timezone.utc)
    result = (
        session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run_uuid,
            SyncRunUnit.status.in_(
                {
                    SyncRunUnitStatus.PLANNED.value,
                    SyncRunUnitStatus.RETRYING.value,
                }
            ),
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
    # is evaluated by the database at UPDATE time, so a stale row that the native Go
    # unit worker concurrently claimed to RUNNING (DISPATCHING->RUNNING + live
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


def _terminalize_unroutable_units(
    session,
    units: Sequence[SyncRunUnit],
) -> int:
    """Fail claimed units that no runtime can execute (CHAOS-3941).

    Write-time CAS on ``status == 'dispatching'``: these units were claimed by
    this pass, and the predicate keeps a concurrent worker's DISPATCHING ->
    RUNNING claim from being overwritten. ``feature_disabled`` is the same
    terminal-denial idiom used to recover the wedged production units, so the
    unit is terminal, finalize can aggregate the run, and the DispatchGuard
    budget is released instead of being held forever.
    """

    if not units:
        return 0
    now = datetime.now(timezone.utc)

    # CHAOS-3990: group by pair so the durable reason NAMES the pair and why it
    # was refused. The previous single-string bulk update stamped only the bare
    # category, which is why an operator querying these rows saw a retry loop
    # with thousands of attempts and no reason attached.
    units_by_pair: dict[tuple[str, str], list[uuid.UUID]] = defaultdict(list)
    for unit in units:
        units_by_pair[(str(unit.provider), str(unit.dataset_key))].append(unit.id)

    terminalized = 0
    for (provider, dataset_key), unit_ids in sorted(units_by_pair.items()):
        reason = _unroutable_reason(provider, dataset_key)
        result = session.execute(
            update(SyncRunUnit)
            .where(
                SyncRunUnit.id.in_(unit_ids),
                SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
            )
            .values(
                status=SyncRunUnitStatus.FAILED.value,
                available_at=None,
                error=FEATURE_DISABLED_ERROR_CATEGORY,
                # The durable record an operator actually reads. ``error``
                # stays the stable machine category every downstream reader
                # already keys on; the prose reason goes here beside it.
                last_retry_reason=reason,
                result={
                    "error_category": FEATURE_DISABLED_ERROR_CATEGORY,
                    "reason": reason,
                    "provider": provider,
                    "dataset_key": dataset_key,
                },
                lease_owner=None,
                lease_expires_at=None,
                updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        terminalized += int(result.rowcount or 0)
    return terminalized


def _unroutable_reason(provider: str, dataset_key: str) -> str:
    """The operator-facing sentence for a pair no runtime will execute.

    Two distinct causes remain, and they need different actions, so the reason
    must not assert the wrong one (review finding):

      * the pair is a non-canonical member of an atomic provider family, served
        only through its canonical claim; or
      * the capability matrix does not mark the pair route-ready and plannable,
        so no shipped writer owns it.

    Neither is ever "a switch is off" -- CHAOS-4054 deleted that plane, so an
    operator is never sent to flip a variable. Nor is either one "the fallback
    runtime was down": step 4 deleted the Celery fallthrough, so River is the
    only runtime and a pair it does not route has nowhere else to go.
    """

    prefix = f"no worker can execute {provider}/{dataset_key}"
    if is_atomic_provider_family_direct_alias(provider, dataset_key):
        return (
            f"{prefix}: it is a non-canonical member of an atomic provider "
            f"family, which is never routed on its own (the family is served "
            f"by its canonical work-items claim)"
        )
    return (
        f"{prefix}: the provider capability matrix does not mark it "
        f"route-ready and plannable, so no shipped writer owns it"
    )


def _enqueue_denied_active_finalize(sync_run_id: str) -> None:
    try:
        getattr(finalize_sync_run, "apply_async")(args=(sync_run_id,), queue="sync")
    except Exception:
        logger.exception(
            "dispatch_sync_run.denied_active_finalize_enqueue_failed",
            extra={"sync_run_id": sync_run_id},
        )
        raise


def terminalize_feature_disabled_plan(
    session: Session,
    sync_run_id: str,
    error: CanonicalIncidentFeatureDisabledError,
) -> FeatureDisabledRunTransition:
    run_uuid = uuid.UUID(str(sync_run_id))
    run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one()
    transition = terminalize_feature_disabled_run(session, run, error)
    if not transition.run_terminal:
        raise RuntimeError(
            f"feature-disabled planned run retained nonterminal units: {sync_run_id}"
        )

    _terminalize_feature_disabled_graph(session, run, error)
    return transition


def _terminalize_feature_disabled_graph(
    session: Session,
    run: SyncRun,
    error: CanonicalIncidentFeatureDisabledError,
) -> None:
    run_uuid = run.id

    now = run.completed_at or datetime.now(timezone.utc)
    error_text = sanitize_error_text(error)
    result_payload = {"error_category": FEATURE_DISABLED_ERROR_CATEGORY}
    session.execute(
        update(SyncRunReferenceDiscovery)
        .where(
            SyncRunReferenceDiscovery.sync_run_id == run_uuid,
            SyncRunReferenceDiscovery.status.in_({"planned", "retrying", "running"}),
        )
        .values(
            status="failed",
            lease_owner=None,
            lease_expires_at=None,
            last_heartbeat_at=now,
            completed_at=now,
            error=error_text,
            result=result_payload,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    session.execute(
        update(SyncDispatchOutbox)
        .where(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
        )
        .values(
            status=OUTBOX_STATUS_DISPATCHED,
            dispatched_at=now,
            last_error=FEATURE_DISABLED_ERROR_CATEGORY,
            claim_token=None,
            claim_expires_at=None,
            claim_transport=None,
            claim_route_generation=None,
            dispatched_transport=None,
            dispatched_route_generation=None,
            transport_job_id=None,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    finalize_row = (
        session.query(SyncDispatchOutbox)
        .filter(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.kind == OUTBOX_KIND_FINALIZE,
        )
        .one_or_none()
    )
    if finalize_row is None:
        session.add(
            SyncDispatchOutbox(
                org_id=str(run.org_id),
                sync_run_id=run_uuid,
                kind=OUTBOX_KIND_FINALIZE,
                status=OUTBOX_STATUS_DISPATCHED,
                available_at=now,
                attempts=0,
                dispatched_at=now,
                last_error=FEATURE_DISABLED_ERROR_CATEGORY,
            )
        )
    else:
        finalize_row.status = OUTBOX_STATUS_DISPATCHED
        finalize_row.last_error = FEATURE_DISABLED_ERROR_CATEGORY
        finalize_row.dispatched_at = now
        finalize_row.claim_token = None
        finalize_row.claim_expires_at = None
        finalize_row.claim_transport = None
        finalize_row.claim_route_generation = None
        finalize_row.dispatched_transport = None
        finalize_row.dispatched_route_generation = None
        finalize_row.transport_job_id = None
        finalize_row.updated_at = now

    sync_observers_for_terminal_sync_run(session, run)
    session.flush()


def _arm_feature_disabled_finalize(
    session: Session,
    run: SyncRun,
    available_at: datetime,
) -> bool:
    nested = session.begin_nested()
    try:
        session.add(
            SyncDispatchOutbox(
                org_id=str(run.org_id),
                sync_run_id=run.id,
                kind=OUTBOX_KIND_FINALIZE,
                status=OUTBOX_STATUS_PENDING,
                available_at=available_at,
                attempts=0,
            )
        )
        session.flush()
    except IntegrityError:
        nested.rollback()
        return False
    nested.commit()
    return True


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
    ``lease_expires_at`` lapses and re-arms dispatch/finalize (CHAOS-2705). Unit
    execution is native Go now (``internal/jobs/providerunit``), which owns
    that lease for the unit's run.

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
            .values(
                status=SyncRunUnitStatus.DISPATCHING.value,
                updated_at=now,
                # CHAOS-3412 (review round 2): a successful claim is the
                # moment the unit stops being blocked, so the aggregate
                # blocked clock stops with it. Set on PLANNED too (where it
                # is already NULL) so the claim path has ONE rule rather than
                # two that could drift.
                first_blocked_at=None,
            )
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
                # The claim that actually matters for CHAOS-3412: a
                # previously-deferred unit is being dispatched, so it is no
                # longer going nowhere and the aggregate clock resets.
                first_blocked_at=None,
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
    # updated_at <= stale_dispatch at write time, so a row that the native Go
    # unit worker concurrently claimed to RUNNING is excluded by
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


def _nonterminal_run_ids_select():
    return select(SyncRun.id).where(SyncRun.status.not_in(_TERMINAL_RUN_STATUSES))


# CHAOS-4159: the residual label for a zero-unit run whose planner recorded no
# cause at all. It is deliberately NOT the label for every zero-unit run any
# more -- see the total_count == 0 branch in finalize_sync_run.
_ZERO_UNIT_GENERIC_ERROR = "No sync units planned"
_ZERO_UNIT_GENERIC_REASON = "no_sync_units_planned"


def _zero_unit_reason(planner_result: Mapping[str, Any]) -> str:
    """Classify a zero-unit run from what the planner recorded on it.

    Prefers an explicit ``reason``, falls back to the ``error_category`` the
    terminalizing planner paths write, and only then to the generic residual.
    A blank or non-string value is treated as absent rather than propagated:
    an empty reason label is worse than the honest generic one because it
    reads as a classification that happened and found nothing.
    """
    for key in ("reason", "error_category"):
        value = planner_result.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return _ZERO_UNIT_GENERIC_REASON


def _run_provider(session: Session, run: SyncRun) -> str:
    """Provider label for a run with no units to read one off.

    A zero-unit run has no SyncRunUnit rows, and provider lives on the unit --
    so the only remaining source is the integration. Falls back to "unknown"
    rather than raising: telemetry must never be the thing that fails a
    finalization.
    """
    provider = (
        session.query(Integration.provider)
        .filter(Integration.id == run.integration_id)
        .scalar()
    )
    if isinstance(provider, str) and provider.strip():
        return provider.strip().lower()
    return "unknown"


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
