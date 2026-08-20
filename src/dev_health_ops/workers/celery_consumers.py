"""Empirical Celery consumer presence for provider-unit dispatch (CHAOS-3941).

``dispatch_sync_run`` publishes a unit to a Celery queue whenever the pair is
not routable to River. That fallthrough is correct only while Celery workers
are actually consuming THAT QUEUE. When Celery scaled to zero replicas the
publish kept succeeding -- a broker accepts a message with no consumer -- and
every published unit sat in ``dispatching`` forever, holding DispatchGuard
concurrency and starving the units that *were* routable.

Nothing in the process detected that. This module is the detector.

WHY ``active_queues`` AND NOT ``ping`` (review finding). A control ``ping`` is
answered by every worker process that is up, regardless of which queues it
consumes. This deployment runs Celery workers for other queues well past the
point where the provider-unit ``sync`` queue has no consumer -- so a ping-based
probe would report "consumers present", republish into the empty queue, and
restore the exact bug this module exists to prevent. ``active_queues`` reports
what each worker is actually consuming, which is the only thing that makes
``dispatch_policy.route``'s own contract checkable: "no route targets an
unconsumed queue".

The three states:

``PRESENT``
    Some worker is consuming at least one of the queues asked about. The Celery
    fallthrough is live -- the mixed-run/rollback state it exists for.

``ABSENT``
    The broker was reachable and NO worker consumes any of those queues. A
    publish would land in a queue with no consumer. Callers must refuse.

``UNKNOWN``
    The probe itself could not run (broker unreachable, control transport
    disabled, probe raised). Deliberately NOT treated as ``ABSENT``: a broker
    outage would otherwise terminalize healthy work on a transient fault. Nor
    is it dangerous -- if the broker is genuinely unreachable, ``apply_async``
    raises and the existing ``dispatch_sync_run.publish_failed`` path handles
    it. The void only opens when the broker is *up* and consumers are *gone*,
    which is exactly ``ABSENT``.

The probe is a broadcast round-trip, so the consumed-queue snapshot is cached
process-wide for a short TTL, and callers only ask when a unit actually needs
the Celery fallthrough (see ``provider_unit_transport.resolve_celery_presence``).
"""

from __future__ import annotations

import enum
import logging
import os
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

PROBE_TIMEOUT_ENV = "WORKER_CELERY_CONSUMER_PROBE_TIMEOUT_SECONDS"
PROBE_TTL_ENV = "WORKER_CELERY_CONSUMER_PROBE_TTL_SECONDS"

_DEFAULT_PROBE_TIMEOUT_SECONDS = 1.5
_DEFAULT_PROBE_TTL_SECONDS = 60.0


class CeleryConsumerPresence(enum.Enum):
    """Whether any Celery worker consumes the queues in question."""

    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ConsumedQueues:
    """What the broker said is being consumed, right now.

    ``queues`` empty with ``reachable`` true means the broker answered and no
    worker is consuming anything -- the scaled-to-zero state.
    """

    reachable: bool
    queues: frozenset[str]


_cache_lock = threading.Lock()
_cached: tuple[float, ConsumedQueues | None] | None = None


def reset_celery_consumer_probe_cache() -> None:
    """Drop the memoized snapshot (tests, forks, operator-driven rechecks)."""

    global _cached
    with _cache_lock:
        _cached = None


def _positive_float(raw: str | None, default: float) -> float:
    if raw is None:
        return default
    try:
        value = float(raw.strip())
    except ValueError:
        return default
    return value if value > 0 else default


def _probe_timeout_seconds() -> float:
    # Literal name on purpose: tests/_env_isolation.py derives its scrub list
    # by scanning for os.getenv string literals.
    return _positive_float(
        os.getenv("WORKER_CELERY_CONSUMER_PROBE_TIMEOUT_SECONDS"),
        _DEFAULT_PROBE_TIMEOUT_SECONDS,
    )


def _probe_ttl_seconds() -> float:
    return _positive_float(
        os.getenv("WORKER_CELERY_CONSUMER_PROBE_TTL_SECONDS"),
        _DEFAULT_PROBE_TTL_SECONDS,
    )


def _queue_names(replies: object) -> frozenset[str]:
    """Flatten ``{worker: [{"name": "sync", ...}, ...]}`` to a name set."""

    names: set[str] = set()
    if not isinstance(replies, dict):
        return frozenset()
    for queues in replies.values():
        if not isinstance(queues, list):
            continue
        for queue in queues:
            if isinstance(queue, dict):
                name = queue.get("name")
            else:
                name = getattr(queue, "name", None)
            if isinstance(name, str) and name:
                names.add(name)
    return frozenset(names)


def _probe_once() -> ConsumedQueues | None:
    """Ask the broker which queues are consumed. ``None`` means "could not ask"."""

    timeout = _probe_timeout_seconds()
    try:
        from dev_health_ops.workers.celery_app import celery_app

        with celery_app.connection_for_read() as connection:
            # Bound the connect attempt explicitly. Celery's default broker
            # policy retries ~100 times with backoff, which would stall a
            # dispatch pass for minutes on a broker outage.
            connection.ensure_connection(max_retries=0, timeout=timeout)
            replies = celery_app.control.inspect(
                timeout=timeout, connection=connection
            ).active_queues()
    except Exception as error:  # broker down, control disabled, transport fault
        logger.warning(
            "celery_consumers.probe_unavailable",
            extra={"error": str(error)},
        )
        return None
    if not replies:
        logger.warning(
            "celery_consumers.no_workers",
            extra={"probe_timeout_seconds": timeout},
        )
        return ConsumedQueues(reachable=True, queues=frozenset())
    return ConsumedQueues(reachable=True, queues=_queue_names(replies))


def consumed_queues(*, now: float | None = None) -> ConsumedQueues | None:
    """Cached-or-fresh consumed-queue snapshot; ``None`` when unprobeable.

    Cached for ``WORKER_CELERY_CONSUMER_PROBE_TTL_SECONDS`` (default 60s) so a
    per-minute dispatch cadence pays at most one broadcast per TTL window. The
    SNAPSHOT is cached rather than a yes/no answer, so different callers asking
    about different queues still share one round-trip.
    """

    global _cached
    ttl = _probe_ttl_seconds()
    stamp = time.monotonic() if now is None else now
    with _cache_lock:
        cached = _cached
        if cached is not None and stamp - cached[0] < ttl:
            return cached[1]
    snapshot = _probe_once()
    with _cache_lock:
        _cached = (stamp, snapshot)
    return snapshot


def _verdict(snapshot: ConsumedQueues | None, wanted: frozenset[str]):
    if snapshot is None:
        return CeleryConsumerPresence.UNKNOWN
    if not wanted:
        return (
            CeleryConsumerPresence.PRESENT
            if snapshot.queues
            else CeleryConsumerPresence.ABSENT
        )
    if wanted & snapshot.queues:
        return CeleryConsumerPresence.PRESENT
    return CeleryConsumerPresence.ABSENT


def probe_celery_consumers(
    queues: Iterable[str] | None = None,
    *,
    now: float | None = None,
) -> CeleryConsumerPresence:
    """Is any Celery worker consuming ``queues``?

    ``queues`` empty or ``None`` asks the weaker "is anything consumed at all"
    question. Callers that know where their message would land should always
    pass the queue names -- a worker on an unrelated queue is not a consumer of
    yours.

    ABSENT IS CONFIRMED BEFORE IT IS RETURNED (review finding). ABSENT is the
    destructive verdict: it terminalizes units. A pidbox broadcast under load
    can time out and return an empty reply set even while workers are happily
    consuming, which would be indistinguishable from "scaled to zero" -- and
    would fail live work. So a first ABSENT triggers one fresh, uncached
    re-probe, and the verdict only stands if the second round-trip agrees. Two
    independent timeouts in a row is a far weaker coincidence than one, and the
    cost is paid only on the path that is about to destroy something.
    """

    wanted = frozenset(queues or ())
    verdict = _verdict(consumed_queues(now=now), wanted)
    if verdict is not CeleryConsumerPresence.ABSENT:
        return verdict

    reset_celery_consumer_probe_cache()
    confirmation = _verdict(consumed_queues(now=now), wanted)
    if confirmation is not CeleryConsumerPresence.ABSENT:
        logger.warning(
            "celery_consumers.absent_not_confirmed",
            extra={
                "requested_queues": sorted(wanted),
                "confirmation": confirmation.value,
            },
        )
        return confirmation
    logger.warning(
        "celery_consumers.queues_unconsumed",
        extra={"requested_queues": sorted(wanted)},
    )
    return CeleryConsumerPresence.ABSENT


try:  # pragma: no cover - import-order dependent, and optional by design
    from celery.signals import worker_process_init

    @worker_process_init.connect
    def _reset_probe_cache_after_fork(**_kwargs: object) -> None:
        """A forked child must not inherit the parent's consumer snapshot.

        Celery prefork children are copies of a parent that may have probed
        minutes ago; without this the child answers from a snapshot it never
        took (review finding).
        """

        reset_celery_consumer_probe_cache()

except Exception:  # celery absent or signals unavailable
    pass


__all__ = [
    "PROBE_TIMEOUT_ENV",
    "PROBE_TTL_ENV",
    "CeleryConsumerPresence",
    "ConsumedQueues",
    "consumed_queues",
    "probe_celery_consumers",
    "reset_celery_consumer_probe_cache",
]
