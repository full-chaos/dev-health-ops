"""Background consumer for ingest Redis Streams.

Reads buffered payloads from Redis Streams using consumer groups,
deserializes them, and persists to the configured storage backend.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

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
                item["_org_id"] = payload.get("org_id", "default")
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


def consume_streams(
    stream_patterns: list[str] | None = None,
    max_iterations: int | None = None,
    consumer_name: str | None = None,
) -> int:
    """Read from ingest streams, deserialize, validate, and ACK entries.

    Returns total number of entries processed.
    """
    from .streams import ENTITY_TYPES, get_redis_client

    rc = get_redis_client()
    if not rc:
        logger.warning("Redis unavailable, consumer cannot start")
        return 0

    if consumer_name is None:
        consumer_name = f"consumer-{uuid.uuid4().hex[:8]}"

    if stream_patterns is None:
        stream_patterns = [f"ingest:*:{et}" for et in ENTITY_TYPES]

    all_streams: dict[str, str] = {}
    for pattern in stream_patterns:
        if "*" in pattern:
            try:
                for key in rc.scan_iter(match=pattern, _type="stream"):
                    all_streams[key] = ">"
            except Exception:
                pass
        else:
            all_streams[pattern] = ">"

    if not all_streams:
        logger.info("No streams found matching patterns")
        return 0

    for stream_key in all_streams:
        _ensure_group(rc, stream_key)

    total_processed = 0
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        try:
            results = rc.xreadgroup(
                CONSUMER_GROUP,
                consumer_name,
                streams=all_streams,
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )
        except Exception:
            logger.exception("XREADGROUP failed")
            time.sleep(1)
            continue

        if not results:
            continue

        for stream_key, entries in results:
            if not entries:
                continue

            parts = (
                stream_key.split(":")
                if isinstance(stream_key, str)
                else stream_key.decode().split(":")
            )
            entity_type = parts[-1] if len(parts) >= 3 else "unknown"

            items = _process_entries(entries, entity_type)

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
                total_processed += len(entries)

            entry_ids = [eid for eid, _ in entries]
            try:
                rc.xack(stream_key, CONSUMER_GROUP, *entry_ids)
            except Exception:
                logger.exception("Failed to ACK entries on %s", stream_key)

    return total_processed
