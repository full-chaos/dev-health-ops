"""Ask Dev retention sweep production caller (CHAOS-3404).

``DevPersistenceService.cleanup_expired`` (``api/dev/persistence/service.py``)
purges conversations whose persisted ``expires_at`` has passed, but until
this module existed nothing ever called it in production: no Celery beat
entry, no CLI command, no caller anywhere. The 30-day retention policy
(``ask_dev_retention_days`` org setting) therefore never executed -- expired
conversations were retained indefinitely regardless of configuration.

This sweep is the ONLY mechanism for 30-day rows -- they only ever expire on
a schedule, never synchronously. For 0-day (ephemeral) conversations it is
the SAFETY NET, not the primary mechanism: ``update_run`` (the common,
live-success path) purges an ephemeral conversation synchronously the
instant its run completes. ``force_terminal_fallback`` and
``recover_stale_non_terminal_run`` -- the documented CHAOS-3297 last-resort
recovery paths -- deliberately do NOT purge synchronously (Codex
adversarial-review round 2, CHAOS-3404, MEDIUM, confirmed: an immediate
purge there can race the same request's own replay-response read of the
run's answer/frame content). All three instead stamp ``expires_at = now()``
on any 0-day conversation the instant its run goes terminal
(``DevPersistenceService._stamp_ephemeral_expiry_if_terminal``, committed
independently of any purge attempt), which is exactly what makes THIS sweep
able to collect the ones the synchronous path didn't -- before that stamp
existed, a 0-day conversation whose run terminated via either recovery path
was permanently unpurgeable by anything, this sweep included, because
``expires_at`` was never set at all for a ``retention_days == 0`` row.

Wired via ``workers/config.py``'s ``beat_schedule`` (production caller) and
instrumented via ``metrics/prometheus.py``'s
``devhealth_ask_dev_retention_sweep_*`` series -- the
``..._last_success_timestamp`` gauge only advances when a run actually
drains its backlog (``status="completed"``); a run that errors
(``"failed"``) or doesn't verify the backlog is genuinely empty
(``"partial"``) never advances it, so an alert on ``time() - gauge``
growing past the beat interval catches a skipped/failed/perpetually-behind
sweep alike -- not just a per-run failure. Caveat, stated honestly rather
than assumed: these are plain ``prometheus_client`` instruments updated
inside the Celery worker process; this codebase's only ``/metrics`` HTTP
endpoint is mounted on the FastAPI app (``api/_observability.py``), and no
worker container exposes or is scraped for one (same pre-existing gap every
other Celery-task metric in ``metrics/prometheus.py`` has -- not new here,
not fixed here). The structured ``ask_dev_retention_sweep.*`` log lines
below are therefore the one receipt actually guaranteed to reach wherever
this deployment ships worker stdout; treat the gauge as best-effort until
worker-side Prometheus export exists.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import Any

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

# Bounded: one beat tick purges at most _DEFAULT_MAX_BATCHES * limit rows.
# If a backlog is larger than that (e.g. after a long outage), the sweep
# reports drained=False and the next scheduled tick continues the drain --
# this keeps a single task run from blocking the worker indefinitely instead
# of silently dropping the remainder.
_DEFAULT_LIMIT = 500
_DEFAULT_MAX_BATCHES = 20


async def _run_ask_dev_retention_cleanup(
    *,
    limit: int = _DEFAULT_LIMIT,
    max_batches: int = _DEFAULT_MAX_BATCHES,
    session_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Drain expired Ask Dev conversations in bounded, committed batches.

    ``session_factory`` defaults to ``dev_health_ops.db.get_postgres_session``
    (production) and is overridable so tests can drive the real sweep logic
    against a seeded, DB-level-aged test database instead of a live Postgres.
    Each batch is its own transaction (explicit commit), matching
    ``prune_external_ingest_batches``' chunked-commit shape, so a crash
    mid-drain loses at most the in-flight batch, never already-purged rows.
    """

    from dev_health_ops.api.dev.persistence import DevPersistenceService
    from dev_health_ops.metrics.prometheus import record_ask_dev_retention_sweep

    factory: Callable[[], Any]
    if session_factory is None:
        from dev_health_ops.db import get_postgres_session

        factory = get_postgres_session
    else:
        factory = session_factory

    total_purged = 0
    total_stamped = 0
    batches_run = 0

    try:
        # CHAOS-3544: stamp stranded rows BEFORE sweeping, so anything
        # repaired on this tick is collected by the same tick rather than
        # waiting another day.
        #
        # This task previously called only `cleanup_expired`, and the
        # backfill's only caller was a `dev-hops maintenance` command. That
        # left the repair operator-dependent -- and "the CLI drain can simply
        # not happen" is exactly the failure mode that let a retained-forever
        # population stay invisible for the whole life of the defect. A
        # scheduled repair is self-healing; a documented manual one is a
        # promise about human behaviour.
        #
        # Bounded and idempotent like the purge loop below, and cheap once
        # drained: the selection is one predicate that returns nothing when
        # nothing is stranded.
        for _ in range(max(1, int(max_batches))):
            async with factory() as session:
                stamp_service = DevPersistenceService(session)
                stamped = await stamp_service.backfill_stranded_ephemeral_expiry(
                    limit=limit
                )
                await session.commit()
            total_stamped += stamped
            if stamped < limit:
                break

        for _ in range(max(1, int(max_batches))):
            batches_run += 1
            async with factory() as session:
                service = DevPersistenceService(session)
                result = await service.cleanup_expired(limit=limit)
                await session.commit()
            total_purged += result.purged
            if result.selected < limit:
                break

        # Codex adversarial-review round 2 (CHAOS-3404, HIGH, confirmed): a
        # batch returning fewer than `limit` rows is NOT proof the backlog
        # is empty -- `cleanup_expired` selects with `FOR UPDATE SKIP
        # LOCKED`, so a genuinely concurrent invocation (a manual trigger
        # overlapping the beat tick, say) can make a batch look short
        # purely because it's holding the remaining expired rows' locks,
        # not because none remain. `count_expired()` is a plain,
        # non-locking COUNT -- row locks block writers, not reads, so it
        # sees every still-due row regardless of who holds a lock on it.
        # Only a confirmed-zero count may report "completed" (and advance
        # the last-success gauge); anything else -- the batch cap, or a
        # contended backlog a SKIP LOCKED read alone couldn't rule out --
        # reports "partial", honestly withholding the gauge until a later,
        # uncontended tick actually verifies the backlog is clear.
        async with factory() as session:
            drained = await DevPersistenceService(session).count_expired() == 0
        # CHAOS-3544, Codex adversarial review (MEDIUM): `count_expired`
        # counts rows with a DUE expires_at, so it is structurally blind to
        # the stranded population this task now repairs -- those still carry
        # expires_at IS NULL. With more stranded rows than
        # max_batches * limit, the stamp loop hits its cap, the purge loop
        # finds nothing left, and the task would report "completed" and
        # advance the last-success gauge while an older backlog remained.
        # That is the same false-drained claim CHAOS-3404 round 2 closed for
        # the purge half, reintroduced through the repair half.
        #
        # Only a VERIFIED-empty stranded backlog may contribute to drained.
        #
        # Codex adversarial review round 2 (MEDIUM, confirmed) corrected the
        # first version of this, which trusted a short stamp batch. It cannot
        # be trusted, for exactly the reason CHAOS-3404 round 2 established
        # for the purge half: backfill_stranded_ephemeral_expiry selects FOR
        # UPDATE SKIP LOCKED, so a concurrent stamper holding the remaining
        # rows makes the batch look short while the backlog is untouched --
        # and if that peer rolls back, those rows still need stamping. The
        # non-locking count is the same instrument count_expired already is,
        # pointed at the population count_expired structurally cannot see.
        async with factory() as session:
            stranded_cleared = (
                await DevPersistenceService(session).count_stranded_ephemeral() == 0
            )
        drained = drained and stranded_cleared
    except Exception:
        logger.exception(
            "ask_dev_retention_sweep.failed",
            extra={"purged_before_failure": total_purged, "batches": batches_run},
        )
        record_ask_dev_retention_sweep(status="failed", purged=total_purged)
        raise

    status = "completed" if drained else "partial"
    if not drained:
        logger.warning(
            "ask_dev_retention_sweep.backlog_not_verified_empty",
            extra={
                "purged": total_purged,
                "batches": batches_run,
                "limit": limit,
                "max_batches": max_batches,
            },
        )

    logger.info(
        "ask_dev_retention_sweep.completed",
        extra={
            "status": status,
            "purged": total_purged,
            "batches": batches_run,
            "drained": drained,
            "stamped": total_stamped,
        },
    )
    record_ask_dev_retention_sweep(status=status, purged=total_purged)
    return {
        "status": status,
        "purged": total_purged,
        "batches": batches_run,
        "drained": drained,
    }


@celery_app.task(
    queue="default",
    name="dev_health_ops.workers.tasks.run_ask_dev_retention_cleanup",
)
def run_ask_dev_retention_cleanup(
    limit: int = _DEFAULT_LIMIT, max_batches: int = _DEFAULT_MAX_BATCHES
) -> dict[str, Any]:
    """Beat-scheduled production caller for ``cleanup_expired`` (CHAOS-3404)."""

    coro: Coroutine[Any, Any, dict[str, Any]] = _run_ask_dev_retention_cleanup(
        limit=limit, max_batches=max_batches
    )
    return run_async(coro)


__all__ = ["run_ask_dev_retention_cleanup"]
