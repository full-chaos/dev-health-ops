"""Shared base machinery for Redis Stream consumers.

Any background consumer that drains a Redis/Valkey Stream via a consumer group
should subclass :class:`StreamConsumer` so it inherits one resilient,
correctly-configured consume loop. This exists because two near-identical
consumers (ingest, product telemetry) diverged, and one of them crashed its
Celery task on a routine idle poll.

Two distinct defects motivated this module:

1. **Blocking-read socket timeout.** A blocking ``XREADGROUP(block=BLOCK_MS)``
   waits up to ``BLOCK_MS`` on the server. ``valkey-py``'s ``from_url`` defaults
   ``socket_timeout`` to *5 seconds* (an intentional divergence from redis-py,
   see valkey-io/valkey-py#119/#120). With ``BLOCK_MS == 5000`` the socket read
   timeout equals the block duration, so ``recv()`` raises ``socket.timeout``
   right as (or just before) the server returns the empty result, surfacing as
   ``valkey.exceptions.TimeoutError: Timeout reading from socket``. Blocking
   consumers must therefore use ``socket_timeout=None`` (see
   :func:`get_consumer_redis_client`). Writers keep their own finite-timeout
   client so HTTP request paths never hang.

2. **Unguarded loop.** A timeout (or any transient Redis error) on the blocking
   read must not escalate to a task failure. The loop wraps ``XREADGROUP`` in
   bounded exponential backoff so a flaky/unavailable broker degrades into
   retries, not crashes.

Subclasses implement :meth:`stream_patterns` and :meth:`process_entry` (or
override :meth:`handle_entries` for batch semantics). Everything else — client
acquisition, stream discovery, consumer-group creation, the backoff loop, DLQ
routing, and ACK — is provided here.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_BLOCK_MS = 5000
DEFAULT_BACKOFF_MAX_S = 30.0
# Headroom added to socket_connect_timeout; the read side is unbounded.
DEFAULT_CONNECT_TIMEOUT_S = 5
DEFAULT_HEALTH_CHECK_INTERVAL_S = 30


def get_consumer_redis_client():
    """Build a Valkey client safe for blocking ``XREADGROUP`` reads.

    Returns ``None`` when ``REDIS_URL`` is unset or the client cannot be built,
    matching the writer-side ``get_redis_client`` contract so callers can treat
    "no Redis" as a graceful no-op.

    Unlike the writer client, this sets ``socket_timeout=None`` so a blocking
    read is bounded by the server-side ``BLOCK`` rather than the socket read
    timeout. Connection establishment is still bounded by
    ``socket_connect_timeout``, and ``health_check_interval`` keeps idle
    pooled connections from going stale.
    """
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import valkey as redis

        return redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=None,
            socket_connect_timeout=DEFAULT_CONNECT_TIMEOUT_S,
            health_check_interval=DEFAULT_HEALTH_CHECK_INTERVAL_S,
        )
    except Exception:
        logger.warning("Redis unavailable for stream consumer")
        return None


class StreamConsumer:
    """Resilient base for Redis Stream consumer-group workers.

    Class attributes configure the consumer; subclasses set at minimum
    :attr:`consumer_group` and implement :meth:`stream_patterns`. The default
    :meth:`handle_entries` processes each entry via :meth:`process_entry`,
    routes failures to the DLQ, and ACKs the whole batch. Consumers that need
    batch persistence across entries (e.g. one DB write per stream) override
    :meth:`handle_entries`.
    """

    #: Consumer-group name (required).
    consumer_group: str = ""
    #: DLQ stream key. Empty disables DLQ routing in the default handler.
    dlq_stream: str = ""
    #: Prefix for auto-generated consumer names.
    consumer_name_prefix: str = "consumer"
    #: Max entries fetched per XREADGROUP.
    batch_size: int = DEFAULT_BATCH_SIZE
    #: Server-side block duration (ms) per poll.
    block_ms: int = DEFAULT_BLOCK_MS
    #: Upper bound for exponential backoff between failed polls.
    backoff_max_s: float = DEFAULT_BACKOFF_MAX_S
    #: Exceptions treated as "reject to DLQ" (logged at warning, not error).
    reject_exceptions: tuple[type[BaseException], ...] = ()

    #: Opt-in, default-off reclaim/redelivery (CHAOS-2693 D5). The base
    #: ``handle_entries()`` above has no true retry path -- every failure is
    #: DLQ'd and ACKed in the same pass, which is fine for existing
    #: best-effort internal consumers (ingest, product-telemetry) but not
    #: for a consumer with a stricter durability bar. Leaving this ``False``
    #: keeps every existing subclass byte-for-byte unaffected: ``reclaim_stale``
    #: below short-circuits before touching Redis when disabled.
    enable_reclaim: bool = False
    #: Idle time (ms) before a pending entry is eligible for reclaim. 15
    #: minutes, not the naive 60s: a consumer's own in-process retry ladder
    #: can sleep ~14s by itself, and a large batch can take well over 60s to
    #: process -- a short reclaim window risks two workers concurrently
    #: processing the same entry.
    reclaim_idle_ms: int = 900_000
    #: Delivery attempts (first read + reclaims) before an entry is treated
    #: as poison: DLQ + ACK instead of being reclaimed again.
    max_deliveries: int = 5

    def __init__(
        self,
        *,
        consumer_name: str | None = None,
        block_ms: int | None = None,
        batch_size: int | None = None,
    ) -> None:
        self.consumer_name = consumer_name
        if block_ms is not None:
            self.block_ms = block_ms
        if batch_size is not None:
            self.batch_size = batch_size

    # ------------------------------------------------------------------
    # Hooks for subclasses
    # ------------------------------------------------------------------
    def stream_patterns(self) -> list[str]:
        """Return stream keys/patterns to read. ``*`` triggers ``scan_iter``."""
        raise NotImplementedError

    def process_entry(
        self, stream_key: str, entry_id: str, data: dict[str, str]
    ) -> int:
        """Process a single stream entry; return number of units persisted.

        Used by the default :meth:`handle_entries`. Raise an exception from
        :attr:`reject_exceptions` for a poison message (routed to DLQ at
        warning level); any other exception is also routed to DLQ but logged
        at error level.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Overridable machinery (sensible defaults)
    # ------------------------------------------------------------------
    def get_client(self):
        """Acquire a blocking-safe Redis client. Override only for tests."""
        return get_consumer_redis_client()

    def discover_streams(self, rc: Any) -> dict[str, str]:
        """Resolve configured patterns into a ``{stream_key: '>'}`` mapping."""
        streams: dict[str, str] = {}
        for pattern in self.stream_patterns():
            if "*" in pattern:
                try:
                    for key in rc.scan_iter(match=pattern, _type="stream"):
                        streams[key] = ">"
                except Exception:
                    logger.exception("scan_iter failed for pattern %s", pattern)
            else:
                streams[pattern] = ">"
        return streams

    def ensure_group(self, rc: Any, stream_key: str) -> None:
        """Create the consumer group, tolerating an existing one."""
        try:
            rc.xgroup_create(stream_key, self.consumer_group, id="0", mkstream=True)
        except Exception as exc:  # noqa: BLE001
            if "BUSYGROUP" in str(exc):
                return
            logger.exception(
                "failed to ensure consumer group",
                extra={
                    "stream_key": stream_key,
                    "consumer_group": self.consumer_group,
                },
            )
            raise

    def move_to_dlq(
        self, rc: Any, stream_key: str, entry_id: str, reason: str
    ) -> bool | None:
        """Best-effort route of a poison entry to the configured DLQ stream.

        Return value is an ack-gating contract for :meth:`reclaim_stale`'s
        give-up path (added for CHAOS-2693's stricter-durability subclass;
        additive, does not change this default's own behavior): ``False``
        means the DLQ write itself failed and the caller should NOT ack the
        source entry (leave it pending for a later retry rather than losing
        it with no DLQ record). This base implementation still returns
        ``None`` (not ``False``) on failure -- existing subclasses
        (ingest, product-telemetry) keep their original best-effort/
        always-ack semantics unless they override this method to return
        ``False`` explicitly on failure, as :class:`ExternalIngestStreamConsumer`
        does.
        """
        if not self.dlq_stream:
            return None
        try:
            rc.xadd(
                self.dlq_stream,
                {
                    "original_stream": stream_key,
                    "entry_id": entry_id,
                    "reason": reason,
                    "moved_at": str(time.time()),
                },
            )
        except Exception:
            logger.exception("Failed to move entry %s to DLQ", entry_id)
        return None

    def reclaim_stale(
        self, rc: Any, stream_key: str
    ) -> list[tuple[str, dict[str, str]]]:
        """Reclaim entries idle longer than :attr:`reclaim_idle_ms`.

        No-op (returns ``[]`` without touching Redis) unless
        :attr:`enable_reclaim` is set -- existing subclasses pay no extra
        round-trip. Entries that have already reached :attr:`max_deliveries`
        are treated as poison: routed to the DLQ and ACKed (given up on, not
        reclaimed) via the same :meth:`move_to_dlq` hook the default
        :meth:`handle_entries` uses, so DLQ semantics are identical
        regardless of which path gave up on the entry.
        """
        if not self.enable_reclaim:
            return []
        try:
            pending = rc.xpending_range(
                stream_key,
                self.consumer_group,
                min="-",
                max="+",
                count=self.batch_size,
                idle=self.reclaim_idle_ms,
            )
        except Exception:
            logger.exception("xpending_range failed for %s", stream_key)
            return []

        claim_ids: list[str] = []
        for entry in pending:
            message_id = entry.get("message_id")
            if message_id is None:
                continue
            times_delivered = int(entry.get("times_delivered", 0) or 0)
            if times_delivered >= self.max_deliveries:
                moved = self.move_to_dlq(
                    rc, stream_key, message_id, "max_deliveries_exceeded"
                )
                if moved is False:
                    # DLQ write itself failed: do NOT ack -- leave the entry
                    # pending so the next reclaim_stale() poll retries the
                    # DLQ write instead of silently losing the entry with no
                    # DLQ record (adversarial-review finding).
                    logger.warning(
                        "DLQ write failed for given-up entry %s; leaving "
                        "pending for retry instead of ACKing",
                        message_id,
                    )
                    continue
                try:
                    rc.xack(stream_key, self.consumer_group, message_id)
                except Exception:
                    logger.exception(
                        "Failed to ACK reclaimed-and-given-up entry %s", message_id
                    )
            else:
                claim_ids.append(message_id)

        if not claim_ids:
            return []
        try:
            claimed = rc.xclaim(
                stream_key,
                self.consumer_group,
                self.consumer_name,
                min_idle_time=self.reclaim_idle_ms,
                message_ids=claim_ids,
            )
        except Exception:
            logger.exception("xclaim failed for %s", stream_key)
            return []
        return list(claimed)

    def handle_entries(
        self,
        rc: Any,
        stream_key: str,
        entries: list[tuple[str, dict[str, str]]],
    ) -> int:
        """Default: process each entry, DLQ failures, ACK the whole batch."""
        processed = 0
        entry_ids: list[str] = []
        for entry_id, data in entries:
            entry_ids.append(entry_id)
            try:
                processed += self.process_entry(stream_key, entry_id, data)
            except self.reject_exceptions as exc:
                logger.warning("Rejecting entry %s: %s", entry_id, exc)
                self.move_to_dlq(rc, stream_key, entry_id, str(exc))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to process entry %s", entry_id)
                self.move_to_dlq(rc, stream_key, entry_id, str(exc))
        if entry_ids:
            try:
                rc.xack(stream_key, self.consumer_group, *entry_ids)
            except Exception:
                logger.exception("Failed to ACK entries on %s", stream_key)
        return processed

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def consume(self, max_iterations: int | None = None) -> int:
        """Drain configured streams, returning total units processed.

        Runs forever when ``max_iterations`` is ``None`` (the worker case), or a
        bounded number of polls (the Celery-task / test case). A failed
        ``XREADGROUP`` never propagates: it is logged and retried with bounded
        exponential backoff.
        """
        rc = self.get_client()
        if not rc:
            logger.warning("Redis unavailable, %s cannot start", type(self).__name__)
            return 0

        if self.consumer_name is None:
            self.consumer_name = f"{self.consumer_name_prefix}-{uuid.uuid4().hex[:8]}"

        streams = self.discover_streams(rc)
        if not streams:
            return 0

        for stream_key in streams:
            self.ensure_group(rc, stream_key)

        total_processed = 0
        iterations = 0
        backoff_s = 1.0
        while max_iterations is None or iterations < max_iterations:
            iterations += 1

            if self.enable_reclaim:
                for stream_key in streams:
                    reclaimed = self.reclaim_stale(rc, stream_key)
                    if reclaimed:
                        total_processed += self.handle_entries(
                            rc, stream_key, reclaimed
                        )

            try:
                results = rc.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    streams=streams,
                    count=self.batch_size,
                    block=self.block_ms,
                )
                backoff_s = 1.0
            except Exception:
                logger.exception("XREADGROUP failed (backoff=%ss)", backoff_s)
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, self.backoff_max_s)
                continue

            if not results:
                continue

            for stream_key, entries in results:
                if not entries:
                    continue
                total_processed += self.handle_entries(rc, stream_key, entries)

        return total_processed
