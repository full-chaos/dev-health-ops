"""Sync dispatch policy contract (CHAOS-2284 folded -> CHAOS-2517).

FROZEN CONTRACT — provider/cost-class -> queue routing. Absorbs
``workers.queues.sync_queue_for_provider`` (kept as a compatibility wrapper).
Implemented in Wave 1 (CHAOS-2517). The dispatcher (CHAOS-2512) routes each
unit through :func:`route`; the planner does NOT know about queues.
"""

from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.workers.queues import (
    DEFAULT_SYNC_QUEUE,
    SYNC_COST_CLASS_QUEUES,
    SYNC_QUEUE_PROVIDERS,
    _provider_sync_queues_enabled,
)


@dataclass(frozen=True)
class DispatchRoute:
    """Where and under what concurrency budget a unit runs.

    ``concurrency_key`` is the bucket the DispatchGuard meters against
    (typically ``f"{org_id}:{provider}:{cost_class}"``).
    """

    queue: str
    cost_class: str
    concurrency_key: str


def route(
    *,
    org_id: str,
    provider: str,
    cost_class: str,
    cost_class_queues_enabled: bool,
) -> DispatchRoute:
    """Resolve the Celery queue for a unit.

    Queue fallback order (CHAOS-2517)::

        sync.<provider>.<cost_class>  (when cost_class_queues_enabled)
        -> sync.<provider>            (when PROVIDER_SYNC_QUEUES_ENABLED)
        -> sync

    The ``cost_class_queues_enabled`` argument is the caller-supplied flag
    (typically read from env via :func:`workers.queues._cost_class_queues_enabled`).
    Provider-level routing still requires ``PROVIDER_SYNC_QUEUES_ENABLED``.

    Tests must assert no route targets an unconsumed queue.
    """
    normalized_provider = (provider or "").strip().lower()
    concurrency_key = f"{org_id}:{normalized_provider}:{cost_class}"

    # Tier 1: cost-class queue (most specific)
    if cost_class_queues_enabled and _provider_sync_queues_enabled():
        cost_class_key = (normalized_provider, cost_class)
        if cost_class_key in SYNC_COST_CLASS_QUEUES:
            return DispatchRoute(
                queue=SYNC_COST_CLASS_QUEUES[cost_class_key],
                cost_class=cost_class,
                concurrency_key=concurrency_key,
            )

    # Tier 2: per-provider queue
    if _provider_sync_queues_enabled() and normalized_provider in SYNC_QUEUE_PROVIDERS:
        return DispatchRoute(
            queue=f"sync.{normalized_provider}",
            cost_class=cost_class,
            concurrency_key=concurrency_key,
        )

    # Tier 3: shared fallback
    return DispatchRoute(
        queue=DEFAULT_SYNC_QUEUE,
        cost_class=cost_class,
        concurrency_key=concurrency_key,
    )
