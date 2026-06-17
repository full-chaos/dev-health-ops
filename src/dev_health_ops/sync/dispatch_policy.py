"""Sync dispatch policy contract (CHAOS-2284 folded -> CHAOS-2517).

FROZEN CONTRACT — provider/cost-class -> queue routing. Absorbs
``workers.queues.sync_queue_for_provider`` (kept as a compatibility wrapper).
Implemented in Wave 1 (CHAOS-2517). The dispatcher (CHAOS-2512) routes each
unit through :func:`route`; the planner does NOT know about queues.
"""

from __future__ import annotations

from dataclasses import dataclass


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

    Queue fallback order (CHAOS-2517):
        sync.<provider>.<cost_class>  (when cost_class_queues_enabled)
        -> sync.<provider>
        -> sync

    Implemented in CHAOS-2517. Tests must assert no route targets an
    unconsumed queue.
    """

    raise NotImplementedError("CHAOS-2517: implement dispatch route")
