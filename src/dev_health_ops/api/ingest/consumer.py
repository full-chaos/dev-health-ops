"""Background consumer for ingest Redis Streams.

Reads buffered payloads from Redis Streams using consumer groups,
deserializes them, and persists to the configured storage backend.

The resilient consume loop (blocking-safe client, bounded backoff, group
creation, ACK) lives in :mod:`dev_health_ops.api._stream_consumer`. This module
supplies the ingest-specific stream patterns and batch persistence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time  # noqa: F401  (retained for backward-compatible monkeypatching)

from .._stream_consumer import StreamConsumer

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "ingest-consumers"
BATCH_SIZE = 100
BLOCK_MS = 5000
MAX_RETRIES = 3


def _ensure_group(rc, stream_key: str) -> None:
    try:
        rc.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass  # Group already exists


def _process_entries(entries: list, entity_type: str) -> list[dict]:
    """Deserialize stream entries back into payload dicts.

    Each entry has {ingestion_id: ..., payload: <json string>}.
    Returns list of individual item dicts ready for storage.
    """
    items: list[dict] = []
    for entry_id, data in entries:
        try:
            payload = json.loads(data.get("payload", "{}"))
            batch_items = payload.get("items", [])
            for item in batch_items:
                item["_org_id"] = payload.get("org_id", "")
                item["_repo_url"] = payload.get("repo_url", "")
                item["_ingestion_id"] = data.get("ingestion_id", "")
            items.extend(batch_items)
        except (json.JSONDecodeError, Exception):
            logger.exception("Failed to deserialize stream entry %s", entry_id)
    return items


def _move_to_dlq(rc, stream_key: str, entry_id: str, entity_type: str) -> None:
    dlq_key = f"ingest:dlq:{entity_type}"
    try:
        rc.xadd(
            dlq_key,
            {
                "original_stream": stream_key,
                "entry_id": entry_id,
                "moved_at": str(time.time()),
            },
        )
    except Exception:
        logger.exception("Failed to move entry %s to DLQ", entry_id)


def _entity_type_from_key(stream_key: str) -> str:
    parts = (
        stream_key.split(":")
        if isinstance(stream_key, str)
        else stream_key.decode().split(":")
    )
    return parts[-1] if len(parts) >= 3 else "unknown"


class IngestStreamConsumer(StreamConsumer):
    """Drains ``ingest:<org>:<entity>`` streams and persists items in batches.

    Overrides :meth:`handle_entries` because ingest deserializes every entry in
    a stream into a single flat item list and performs one persist call per
    stream, rather than one per entry.
    """

    consumer_group = CONSUMER_GROUP
    consumer_name_prefix = "consumer"

    def __init__(self, stream_patterns: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._stream_patterns = stream_patterns

    def stream_patterns(self) -> list[str]:
        if self._stream_patterns is not None:
            return self._stream_patterns
        from .streams import ENTITY_TYPES

        return [f"ingest:*:{et}" for et in ENTITY_TYPES]

    def ensure_group(self, rc, stream_key: str) -> None:
        # Preserve ingest's swallow-all semantics (best-effort group creation).
        _ensure_group(rc, stream_key)

    def handle_entries(self, rc, stream_key, entries) -> int:
        if not entries:
            return 0

        entity_type = _entity_type_from_key(stream_key)
        items = _process_entries(entries, entity_type)

        processed = 0
        if items:
            logger.info(
                "Processed %d items from %s (%d entries)",
                len(items),
                stream_key,
                len(entries),
            )
            try:
                from .persist import persist_items

                asyncio.run(persist_items(entity_type, items))
            except Exception:
                logger.exception(
                    "Failed to persist %d items for %s",
                    len(items),
                    entity_type,
                )
            processed = len(entries)

        entry_ids = [eid for eid, _ in entries]
        try:
            rc.xack(stream_key, self.consumer_group, *entry_ids)
        except Exception:
            logger.exception("Failed to ACK entries on %s", stream_key)

        return processed


def consume_streams(
    stream_patterns: list[str] | None = None,
    max_iterations: int | None = None,
    consumer_name: str | None = None,
) -> int:
    """Read from ingest streams, deserialize, validate, and ACK entries.

    Returns total number of entries processed.
    """
    consumer = IngestStreamConsumer(
        stream_patterns=stream_patterns,
        consumer_name=consumer_name,
        block_ms=BLOCK_MS,
        batch_size=BATCH_SIZE,
    )
    return consumer.consume(max_iterations=max_iterations)
