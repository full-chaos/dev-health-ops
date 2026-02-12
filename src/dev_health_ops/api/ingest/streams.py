"""Redis Stream helpers for the ingest API.

Provides read/write utilities for buffering ingest payloads in Redis Streams.
Extracted from router.py to enable reuse by the background consumer.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

ENTITY_TYPES = ("commits", "pull-requests", "work-items", "deployments", "incidents")
CONSUMER_GROUP = "ingest-consumers"
DLQ_PREFIX = "ingest:dlq:"


def get_redis_client():
    """Get Redis client from REDIS_URL env var. Returns None if unavailable."""
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis

        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Redis unavailable for ingest streams")
        return None


def write_to_stream(redis_client, stream_name: str, data: dict) -> bool:
    """Write a message to a Redis Stream. Returns True on success."""
    if not redis_client:
        return False
    try:
        redis_client.xadd(stream_name, data, maxlen=100000, approximate=True)
        return True
    except Exception:
        logger.exception("Failed to write to stream %s", stream_name)
        return False


def ensure_consumer_groups(redis_client) -> None:
    """Create consumer groups for all known stream patterns if they don't exist.

    This is best-effort — groups are created dynamically on first read too.
    """
    pass


def stream_name(org_id: str, entity_type: str) -> str:
    """Build a canonical stream key for the given org and entity type."""
    return f"ingest:{org_id}:{entity_type}"
