"""Liveness/lag observability for external-ingest streams (CHAOS-2693 D9).

Mirrors the existing ``workers/queue_monitor.py`` convention (structured
logs, no new table -- no Prometheus/statsd exporter exists anywhere in this
codebase today) applied to Redis Streams instead of the Celery broker:
``SCAN``s ``external-ingest:*:batches``/``external-ingest:*:dlq``, and per
stream logs depth (``XLEN``), pending-entry count, and the oldest pending
entry's idle time (``XPENDING``), warning above thresholds analogous to
``queue_monitor.py``'s ``QUEUE_DEPTH_WARNING_THRESHOLD``/
``QUEUE_AGE_WARNING_SECONDS``.

Beat-scheduled every 60s on the ``monitoring`` queue (``workers/config.py``)
-- the same dedicated telemetry queue ``monitor_queue_depths`` uses, so this
keeps reporting even if ``default``/``external-ingest`` floods.
"""

from __future__ import annotations

import logging
from typing import Any

from .streams import CONSUMER_GROUP, get_redis_client

logger = logging.getLogger(__name__)

STREAM_DEPTH_WARNING_THRESHOLD = 200
STREAM_AGE_WARNING_MS = 600_000  # 10 minutes


def _oldest_pending_idle_ms(rc: Any, stream_key: str) -> int | None:
    """Idle time (ms) of the lowest-ID (oldest-delivered) pending entry.

    A cheap proxy for "oldest pending age": ``XPENDING``'s plain summary
    form reports count/min-id/max-id/per-consumer counts but not idle time;
    the extended (range) form with ``count=1`` returns the single
    lowest-ID pending entry, which in a FIFO consumer-group PEL is also the
    entry that has been outstanding longest in the common case (its ID
    ordering is unaffected by reclaim).
    """
    try:
        oldest = rc.xpending_range(
            stream_key, CONSUMER_GROUP, min="-", max="+", count=1
        )
    except Exception:
        logger.debug("xpending_range failed for %s", stream_key, exc_info=True)
        return None
    if not oldest:
        return None
    return int(oldest[0].get("time_since_delivered", 0) or 0)


def _pending_count(rc: Any, stream_key: str) -> int:
    try:
        summary = rc.xpending(stream_key, CONSUMER_GROUP)
    except Exception:
        logger.debug("xpending failed for %s", stream_key, exc_info=True)
        return 0
    if not summary:
        return 0
    return int(summary.get("pending", 0) or 0)


def report_stream_health() -> dict[str, Any]:
    """Log depth/pending/oldest-idle for every discovered external-ingest
    stream (batches + DLQ). Returns the same data for programmatic/test use.
    """
    rc = get_redis_client()
    if rc is None:
        logger.warning("external_ingest_stream_health: Redis unavailable")
        return {"streams": []}

    observed: list[dict[str, Any]] = []
    for pattern in ("external-ingest:*:batches", "external-ingest:*:dlq"):
        is_dlq = pattern.endswith(":dlq")
        try:
            stream_keys = list(rc.scan_iter(match=pattern, _type="stream"))
        except Exception:
            logger.exception("scan_iter failed for pattern %s", pattern)
            continue

        for stream_key in stream_keys:
            try:
                depth = int(rc.xlen(stream_key))
            except Exception:
                logger.exception("xlen failed for %s", stream_key)
                continue

            pending = 0 if is_dlq else _pending_count(rc, stream_key)
            oldest_idle_ms = None if is_dlq else _oldest_pending_idle_ms(rc, stream_key)

            stats = {
                "stream": stream_key,
                "is_dlq": is_dlq,
                "depth": depth,
                "pending": pending,
                "oldest_pending_idle_ms": oldest_idle_ms,
            }
            observed.append(stats)
            logger.info("external_ingest_stream_health", extra=stats)

            if depth > STREAM_DEPTH_WARNING_THRESHOLD or (
                oldest_idle_ms is not None and oldest_idle_ms > STREAM_AGE_WARNING_MS
            ):
                logger.warning("external_ingest_stream_backlog", extra=stats)

    return {"streams": observed}


__all__ = [
    "report_stream_health",
    "STREAM_DEPTH_WARNING_THRESHOLD",
    "STREAM_AGE_WARNING_MS",
]
