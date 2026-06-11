"""Broker queue depth/age telemetry (CHAOS-2299).

Every live sync incident so far was diagnosed with per-queue ``LLEN`` against
the broker. This beat task does that automatically: one structured log line
per non-empty queue (depth + oldest message age) every minute, plus a warning
when a queue crosses the backlog thresholds, so "is <provider> stuck?" is
answerable from logs without shelling into the broker.

Age detection relies on the ``enqueued_at`` header stamped by the
``before_task_publish`` signal in workers.celery_app. Messages published
before that signal shipped (or by foreign producers) have no header, in which
case the queue is reported depth-only (``oldest_age_seconds`` is None).

This intentionally writes NO ClickHouse rows — Data Health UI wiring is a
follow-up.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.config import task_queues

logger = logging.getLogger(__name__)

# Backlog warning thresholds. Depth: a full batch fan-out of a large org tops
# out well under this in normal operation. Age: the oldest message waiting
# longer than 10 minutes means consumers are wedged or starved.
QUEUE_DEPTH_WARNING_THRESHOLD = 200
QUEUE_AGE_WARNING_SECONDS = 600


def _queue_depth(channel: Any, queue: str) -> int:
    """Best-effort message count for a queue via the kombu channel.

    Kombu virtual transports (redis/valkey — the deployed broker) implement
    ``_size``; other transports fall back to a passive queue_declare. A
    missing queue (redis deletes empty lists) counts as empty.
    """
    try:
        size = getattr(channel, "_size", None)
        if callable(size):
            return int(size(queue))
        return int(channel.queue_declare(queue=queue, passive=True).message_count)
    except Exception:
        logger.debug("queue depth probe failed for %s", queue, exc_info=True)
        return 0


def _oldest_age_seconds(channel: Any, queue: str, now: datetime) -> float | None:
    """Age of the oldest queued message, when cheaply observable.

    Redis transport only: kombu LPUSHes new messages and consumers BRPOP, so
    the oldest message sits at the tail (``LINDEX queue -1``). Its kombu
    payload carries the celery headers, including the ``enqueued_at`` stamp
    from workers.celery_app. Returns None on non-redis transports, unparseable
    payloads, or messages without the stamp (depth-only reporting).
    """
    client = getattr(channel, "client", None)
    if client is None:  # not a redis-like transport; no cheap peek available
        return None
    try:
        raw = client.lindex(queue, -1)
        if raw is None:
            return None
        payload = json.loads(raw)
        headers = payload.get("headers") or {}
        enqueued_at = headers.get("enqueued_at")
        if not enqueued_at:
            return None
        enqueued = datetime.fromisoformat(str(enqueued_at))
        if enqueued.tzinfo is None:
            enqueued = enqueued.replace(tzinfo=timezone.utc)
        return max(0.0, (now - enqueued).total_seconds())
    except Exception:
        logger.debug("queue age probe failed for %s", queue, exc_info=True)
        return None


@celery_app.task(
    bind=True,
    # Dedicated telemetry queue (consumed by both `worker` and `worker-heavy`
    # in compose.yml): if `default` floods, this monitor must keep running —
    # that is exactly the moment its output matters.
    queue="monitoring",
    name="dev_health_ops.workers.tasks.monitor_queue_depths",
)
def monitor_queue_depths(self) -> dict:
    """Log depth + oldest-message age for every declared Celery queue."""
    now = datetime.now(timezone.utc)
    observed: list[dict[str, Any]] = []

    with celery_app.connection_or_acquire() as connection:
        channel = connection.default_channel
        for queue in task_queues:
            depth = _queue_depth(channel, queue)
            if depth <= 0:
                continue
            oldest_age = _oldest_age_seconds(channel, queue, now)
            stats = {
                "queue": queue,
                "depth": depth,
                "oldest_age_seconds": oldest_age,
            }
            observed.append(stats)
            logger.info("queue_depth", extra=stats)
            if depth > QUEUE_DEPTH_WARNING_THRESHOLD or (
                oldest_age is not None and oldest_age > QUEUE_AGE_WARNING_SECONDS
            ):
                logger.warning("queue_backlog", extra=stats)

    return {"queues": observed}
