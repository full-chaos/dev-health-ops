"""Consumer group + retry/reclaim + DLQ for external-ingest streams (CHAOS-2693).

Subclasses the shared :class:`~dev_health_ops.api._stream_consumer.StreamConsumer`
base rather than hand-rolling the ``XREADGROUP`` loop -- that base class
exists specifically because two independent consumers (ingest,
product-telemetry) regressed the same two production bugs (blocking-read
socket-timeout race; unguarded-loop crash). See its module docstring.

DEPLOYMENT INVARIANT (master-spec CC11, non-negotiable): exactly ONE
``worker-external-ingest`` replica must run, at Celery ``--concurrency=1``
(``compose.yml``). The reclaim design (``enable_reclaim=True`` below) relies
on there being a single logical consumer identity draining the PEL --
scaling replicas without first revisiting reclaim semantics reintroduces
the double-processing window the 15-minute ``reclaim_idle_ms`` and the
idempotent-skip guard below are built to close.

Retry/give-up policy (D5): a :class:`PermanentProcessingError` from
``process_entry`` (unsupported schema version, a structurally invalid
envelope that survived API-layer validation) is routed straight to the
DLQ and ACKed -- no retry. Any other exception is treated conservatively
as transient (connection errors, timeouts, unclassified bugs): the entry
is deliberately left un-ACKed, so it stays in the consumer group's PEL for
the shared base's ``reclaim_stale()`` to retry on a later poll, up to
``max_deliveries``. Once exhausted, the base's reclaim loop calls
:meth:`move_to_dlq` itself (the "give-up" path) -- routed here to the same
DLQ logic as an explicit permanent failure.

Idempotent-skip guard (CC11 post-critique): before (re)processing ANY
entry -- freshly read or reclaimed -- the consumer loads the batch's
current status (``api.external_ingest.status.get_batch``, CHAOS-2694) and
ACKs-and-skips without reprocessing if it is already terminal
(completed/partial/failed). This prevents double processing when an entry
is reclaimed after a slow-but-ultimately-successful run raced the
15-minute reclaim window.
"""

from __future__ import annotations

import inspect
import logging
import uuid
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.external_ingest.errors import PermanentProcessingError
from dev_health_ops.models.external_ingest import TERMINAL_STATUSES
from dev_health_ops.workers.async_runner import run_async

from .._stream_consumer import StreamConsumer
from .streams import CONSUMER_GROUP, STREAM_MAXLEN, dlq_name

logger = logging.getLogger(__name__)

# Smaller than ingest's 100: each entry now triggers a Postgres payload
# fetch + full record processing, not a cheap in-memory item append.
BATCH_SIZE = 50
BLOCK_MS = 5000
RECLAIM_IDLE_MS = 900_000  # 15 min, master-spec CC11 post-critique
MAX_DELIVERIES = 5

_TERMINAL_STATUS_VALUES = {s.value for s in TERMINAL_STATUSES}


def _processor_available() -> bool:
    """Whether CHAOS-2697's ``external_ingest.processor`` module (owning
    ``process_batch``/``mark_batch_failed``) is importable.

    Deployment-order guard (adversarial-review finding): this issue's beat
    schedule/queue wiring shipped ahead of CHAOS-2697's worker implementation
    by design (master-spec wave plan), so the consumer checks this before
    claiming any entries rather than draining the stream into a
    guaranteed-ImportError retry ladder. CHAOS-2697 has since landed — the
    guard now passes and is kept as rollback protection.
    """
    try:
        import dev_health_ops.external_ingest.processor  # noqa: F401
    except ImportError:
        return False
    return True


class ExternalIngestStreamConsumer(StreamConsumer):
    consumer_group = CONSUMER_GROUP
    consumer_name_prefix = "external-ingest-consumer"
    batch_size = BATCH_SIZE
    block_ms = BLOCK_MS
    enable_reclaim = True
    reclaim_idle_ms = RECLAIM_IDLE_MS
    max_deliveries = MAX_DELIVERIES
    reject_exceptions = (PermanentProcessingError,)

    def stream_patterns(self) -> list[str]:
        return ["external-ingest:*:batches"]

    # ------------------------------------------------------------------
    # Deployment-order guard (adversarial-review finding)
    # ------------------------------------------------------------------
    def consume(self, max_iterations: int | None = None) -> int:
        """Refuses to claim any stream entries when CHAOS-2697's
        ``external_ingest.processor`` module is unavailable.

        Without this guard, enabling this consumer (beat-scheduled every
        30s) before CHAOS-2697 lands would deliver every entry into
        ``process_entry``, which immediately raises ``ImportError``
        (treated as transient), burning through ``max_deliveries`` reclaim
        cycles for no reason and routing customer batches to the DLQ before
        a real worker implementation ever existed to attempt them. A no-op
        here leaves entries completely untouched (zero delivery attempts)
        in the stream/PEL, so they are picked up fresh and correctly once
        CHAOS-2697 ships -- no manual re-drive needed.
        """
        if not _processor_available():
            logger.warning(
                "external_ingest.processor unavailable (CHAOS-2697 not yet "
                "landed) -- %s refusing to claim any stream entries this "
                "poll; entries remain untouched for a later poll once the "
                "processor ships",
                type(self).__name__,
            )
            return 0
        return super().consume(max_iterations=max_iterations)

    # ------------------------------------------------------------------
    # DLQ routing
    # ------------------------------------------------------------------
    def _org_id_from_stream_key(self, stream_key: str) -> str:
        # "external-ingest:<org_id>:batches"
        parts = stream_key.split(":")
        return parts[1] if len(parts) >= 3 else "unknown"

    def _fetch_entry_fields(
        self, rc: Any, stream_key: str, entry_id: str
    ) -> dict[str, str]:
        """Re-read a single entry's fields by ID.

        Used only by :meth:`move_to_dlq`'s give-up path (invoked by the
        shared base's ``reclaim_stale()``, which has no field data -- only
        the message ID from ``XPENDING``). Freshly-read/reclaimed entries
        handled via :meth:`handle_entries` already carry their field dict
        and never call this.
        """
        try:
            rows = rc.xrange(stream_key, min=entry_id, max=entry_id)
        except Exception:
            logger.exception("Failed to re-read entry %s for DLQ routing", entry_id)
            return {}
        if not rows:
            return {}
        _id, data = rows[0]
        return dict(data)

    def move_to_dlq(self, rc: Any, stream_key: str, entry_id: str, reason: str) -> bool:
        """Give-up path invoked directly by the shared base's ``reclaim_stale()``
        once ``max_deliveries`` is exhausted for a still-pending entry. This
        is a SYNC call site, outside of any running event loop -- safe to
        use :func:`run_async` for the ``mark_batch_failed`` side effect.

        Returns whether the DLQ write itself succeeded (base class contract,
        adversarial-review finding): the caller (``reclaim_stale``) must NOT
        ack the source entry when this is ``False``, or a DLQ write failure
        (e.g. a transient Redis blip) would silently lose the entry with no
        DLQ record at all.
        """
        data = self._fetch_entry_fields(rc, stream_key, entry_id)
        org_id, dlq_written = self._xadd_dlq_entry(
            rc, stream_key, entry_id, reason, data
        )
        ingestion_id = data.get("ingestion_id")
        if dlq_written and ingestion_id:
            self._mark_batch_failed_best_effort(
                org_id=org_id, ingestion_id=ingestion_id, reason=reason
            )
        return dlq_written

    async def _dlq_entry_async(
        self,
        rc: Any,
        stream_key: str,
        entry_id: str,
        reason: str,
        data: dict[str, str],
    ) -> bool:
        """Async counterpart of the give-up path, for the permanent-failure
        branch inside :meth:`_handle_entries_async`, which already runs
        inside ``run_async``'s event loop -- MUST ``await`` directly rather
        than call :meth:`_mark_batch_failed_best_effort` (which itself calls
        :func:`run_async`), or ``run_async``'s own re-entrancy guard raises.

        Returns whether the source entry may be ACKed: requires BOTH the DLQ
        write to succeed (same rationale as :meth:`move_to_dlq`) AND, when an
        ingestion_id is present, the failed-status write to be safe (see
        :meth:`_mark_batch_failed_best_effort_async` -- adversarial-review
        round-2: ACKing past a raised ``mark_batch_failed`` strands the batch
        in a non-terminal status with the entry gone).
        """
        org_id, dlq_written = self._xadd_dlq_entry(
            rc, stream_key, entry_id, reason, data
        )
        ingestion_id = data.get("ingestion_id")
        if dlq_written and ingestion_id:
            marked_ok = await self._mark_batch_failed_best_effort_async(
                org_id=org_id, ingestion_id=ingestion_id, reason=reason
            )
            return dlq_written and marked_ok
        return dlq_written

    def _xadd_dlq_entry(
        self,
        rc: Any,
        stream_key: str,
        entry_id: str,
        reason: str,
        data: dict[str, str],
    ) -> tuple[str, bool]:
        """XADD to the per-org DLQ (sync -- the redis client itself is never
        async). Per-org (D1): a single bad-actor/misconfigured org flooding
        poison batches must not crowd out DLQ visibility for other orgs.
        Returns ``(org_id, succeeded)`` -- callers use ``succeeded`` to
        decide whether it is safe to ack the source entry (adversarial-review
        finding: a swallowed DLQ-write failure must not silently drop the
        entry with no DLQ record).
        """
        org_id = data.get("org_id") or self._org_id_from_stream_key(stream_key)
        dlq = dlq_name(org_id)
        try:
            rc.xadd(
                dlq,
                {
                    "original_stream": stream_key,
                    "entry_id": entry_id,
                    "reason": reason,
                    "ingestion_id": data.get("ingestion_id", ""),
                    "org_id": org_id,
                    "moved_at": datetime.now(timezone.utc).isoformat(),
                },
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
        except Exception:
            logger.exception("Failed to move entry %s to DLQ", entry_id)
            return org_id, False
        return org_id, True

    @staticmethod
    def _resolve_mark_batch_failed():
        """Give-up path calls ``external_ingest.processor.mark_batch_failed``
        (CHAOS-2697's pinned worker contract, CC23). Import-tolerant: kept
        even now that CHAOS-2697 has landed, so a rollback that removes the
        processor module degrades to the logged-warning path rather than
        raising ImportError into the consumer loop.
        """
        try:
            from dev_health_ops.external_ingest.processor import mark_batch_failed
        except ImportError:
            return None
        return mark_batch_failed

    def _mark_batch_failed_best_effort(
        self, *, org_id: str, ingestion_id: str, reason: str
    ) -> None:
        """Sync path: used by :meth:`move_to_dlq`, itself only ever called
        outside a running event loop (the shared base's synchronous
        ``reclaim_stale()``)."""
        mark_batch_failed = self._resolve_mark_batch_failed()
        if mark_batch_failed is None:
            logger.warning(
                "external_ingest.processor.mark_batch_failed unavailable "
                "(CHAOS-2697 not yet landed) -- batch %s org=%s not marked failed",
                ingestion_id,
                org_id,
            )
            return
        try:
            result = mark_batch_failed(
                ingestion_id=ingestion_id, org_id=org_id, reason=reason
            )
            if inspect.isawaitable(result):
                # mark_batch_failed's concrete return type is unknown until
                # CHAOS-2697 defines it; inspect.isawaitable narrows to the
                # broader Awaitable protocol, but run_async expects a
                # Coroutine specifically -- an async def's return value
                # always is one at runtime.
                run_async(cast(Coroutine[Any, Any, Any], result))
        except Exception:
            logger.exception(
                "mark_batch_failed failed for ingestion_id=%s org=%s",
                ingestion_id,
                org_id,
            )

    async def _mark_batch_failed_best_effort_async(
        self, *, org_id: str, ingestion_id: str, reason: str
    ) -> bool:
        """Async path: used by :meth:`_dlq_entry_async`, already inside a
        running event loop -- awaits the coroutine directly, never
        :func:`run_async`.

        Returns whether it is safe to ACK the source entry with respect to
        status marking: ``True`` when the batch was marked failed OR when the
        seam is not yet importable (pre-CHAOS-2697 there is no worker writing
        statuses to strand; the CC13 stale-accepted RETRY path remains the
        customer's recovery). ``False`` ONLY when a resolvable
        ``mark_batch_failed`` raised (e.g. transient Postgres outage): the
        caller must then leave the entry un-ACKed so a later redelivery
        retries the status write -- otherwise the customer-visible status
        never reaches ``failed`` while the entry is gone (adversarial-review
        round-2 finding). A duplicate DLQ entry on that retry is accepted
        operational noise; a silently non-terminal batch is not."""
        mark_batch_failed = self._resolve_mark_batch_failed()
        if mark_batch_failed is None:
            logger.warning(
                "external_ingest.processor.mark_batch_failed unavailable "
                "(CHAOS-2697 not yet landed) -- batch %s org=%s not marked failed",
                ingestion_id,
                org_id,
            )
            return True
        try:
            result = mark_batch_failed(
                ingestion_id=ingestion_id, org_id=org_id, reason=reason
            )
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception(
                "mark_batch_failed failed for ingestion_id=%s org=%s -- "
                "leaving source entry un-ACKed for a status-write retry",
                ingestion_id,
                org_id,
            )
            return False
        return True

    # ------------------------------------------------------------------
    # Idempotent-skip guard
    # ------------------------------------------------------------------
    async def _is_batch_terminal_async(self, org_id: str, ingestion_id: str) -> bool:
        from dev_health_ops.api.external_ingest.status import get_batch
        from dev_health_ops.db import get_postgres_session

        try:
            batch_ingestion_id = uuid.UUID(ingestion_id)
        except (ValueError, AttributeError, TypeError):
            return False

        async with get_postgres_session() as session:
            batch = await get_batch(
                session, org_id=org_id, ingestion_id=batch_ingestion_id
            )
        return batch is not None and batch.status in _TERMINAL_STATUS_VALUES

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------
    async def _process_entry_async(
        self, stream_key: str, entry_id: str, data: dict[str, str]
    ) -> int:
        from dev_health_ops.external_ingest.processor import process_batch

        # CHAOS-2697's pinned contract (CC23): async, returns items_accepted.
        return await process_batch(
            ingestion_id=data["ingestion_id"],
            org_id=data["org_id"],
            source_system=data["source_system"],
            source_instance=data["source_instance"],
            schema_version=data["schema_version"],
        )

    def process_entry(
        self, stream_key: str, entry_id: str, data: dict[str, str]
    ) -> int:
        """Standalone sync entry point (interface parity with the base
        class contract / CHAOS-2697; ad-hoc REPL verification). Not called
        internally by :meth:`handle_entries`, which awaits the async core
        directly within a single event loop for the whole batch -- see its
        docstring for why a per-entry ``run_async`` would break.
        """
        return run_async(self._process_entry_async(stream_key, entry_id, data))

    # ------------------------------------------------------------------
    # Batch handling: selective ack (D5) + idempotent-skip guard (CC11)
    # ------------------------------------------------------------------
    def handle_entries(
        self,
        rc: Any,
        stream_key: str,
        entries: list[tuple[str, dict[str, str]]],
    ) -> int:
        """Overrides the base's default (which ACKs every entry regardless
        of outcome) -- external-ingest's durability bar requires transient
        failures to stay in the PEL for reclaim, not be ACKed away.

        Runs the whole batch through ONE event loop (a single
        ``run_async`` call), not one per entry: the cached async Postgres
        engine (``db.get_postgres_engine()``) is bound to whichever event
        loop first created it, and ``run_async`` resets it before each
        call -- doing that per-entry inside a tight loop would be both
        wasteful (a connection-pool teardown/rebuild per entry) and, if
        this method's caller (the shared base's ``consume()``) already
        held a running loop, would trip ``run_async``'s own re-entrancy
        guard.
        """
        return run_async(self._handle_entries_async(rc, stream_key, entries))

    async def _handle_entries_async(
        self,
        rc: Any,
        stream_key: str,
        entries: list[tuple[str, dict[str, str]]],
    ) -> int:
        processed = 0
        to_ack: list[str] = []
        for entry_id, data in entries:
            org_id = data.get("org_id", "")
            ingestion_id = data.get("ingestion_id", "")
            try:
                if ingestion_id and await self._is_batch_terminal_async(
                    org_id, ingestion_id
                ):
                    logger.info(
                        "Skipping already-terminal batch %s (idempotent-skip guard)",
                        ingestion_id,
                    )
                    to_ack.append(entry_id)
                    continue
                processed += await self._process_entry_async(stream_key, entry_id, data)
                to_ack.append(entry_id)
            except PermanentProcessingError as exc:
                logger.warning("Permanent failure for entry %s: %s", entry_id, exc)
                dlq_written = await self._dlq_entry_async(
                    rc, stream_key, entry_id, str(exc), data
                )
                if dlq_written:
                    to_ack.append(entry_id)
                else:
                    # DLQ write failed: do NOT ack -- leave pending so a
                    # later poll retries the DLQ write instead of silently
                    # losing the entry with no DLQ record (adversarial-review
                    # finding). It will keep re-hitting PermanentProcessingError
                    # on every fresh delivery/reclaim until the DLQ write
                    # eventually succeeds.
                    logger.warning(
                        "DLQ write failed for permanently-failed entry %s; "
                        "leaving un-ACKed for retry",
                        entry_id,
                    )
            except Exception:
                logger.exception(
                    "Transient failure processing entry %s on %s; leaving "
                    "un-ACKed for reclaim_stale()",
                    entry_id,
                    stream_key,
                )
                # Deliberately NOT appended to to_ack: stays in the PEL.

        if to_ack:
            try:
                rc.xack(stream_key, self.consumer_group, *to_ack)
            except Exception:
                logger.exception("Failed to ACK entries on %s", stream_key)
        return processed


def consume_external_ingest_streams(
    max_iterations: int | None = None, consumer_name: str | None = None
) -> int:
    """Entry point for the Celery task (``workers/system_ops.py``) and
    ad-hoc/live-verification REPL calls. Returns total units processed."""
    consumer = ExternalIngestStreamConsumer(
        consumer_name=consumer_name, block_ms=BLOCK_MS, batch_size=BATCH_SIZE
    )
    return consumer.consume(max_iterations=max_iterations)


__all__ = [
    "BATCH_SIZE",
    "BLOCK_MS",
    "RECLAIM_IDLE_MS",
    "MAX_DELIVERIES",
    "ExternalIngestStreamConsumer",
    "consume_external_ingest_streams",
]
