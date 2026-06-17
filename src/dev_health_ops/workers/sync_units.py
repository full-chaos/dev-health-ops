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


def dispatch_sync_run(sync_run_id: str) -> None:
    """Authorize, route, and queue all pending units of a planned run.

    Idempotent / redispatchable. Implemented in CHAOS-2512.
    """

    raise NotImplementedError("CHAOS-2512: implement dispatch_sync_run")


def run_sync_unit(unit_id: str) -> None:
    """Execute exactly one (source, dataset, window) unit.

    Loads context via SyncTaskBootstrap, runs the provider dataset adapter
    (CHAOS-2513), persists status/attempts/duration/result, and updates the
    watermark only when mode=="incremental" and the unit succeeded. Never
    dispatches metrics. Implemented in CHAOS-2512.
    """

    raise NotImplementedError("CHAOS-2512: implement run_sync_unit")


def finalize_sync_run(sync_run_id: str) -> None:
    """Aggregate unit statuses and dispatch post-sync metrics once per run.

    No-op until all units are terminal; once-only via the SyncRunPostDispatch
    outbox. Maps completed dataset keys back to legacy sync_targets via
    ``planner.map_datasets_to_legacy_targets`` before calling the existing
    ``_dispatch_post_sync_tasks``. Implemented in CHAOS-2512.
    """

    raise NotImplementedError("CHAOS-2512: implement finalize_sync_run")
