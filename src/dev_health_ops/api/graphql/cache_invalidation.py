"""Cache invalidation utilities for GraphQL queries.

Provides tag-based cache invalidation that can be triggered by
background tasks (e.g., Celery workers) when data changes.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class CacheInvalidationEvent:
    """
    Represents a cache invalidation event.

    Events can be published when data changes to trigger cache invalidation.
    """

    # Event types
    METRICS_UPDATED = "metrics_updated"
    WORK_ITEMS_SYNCED = "work_items_synced"
    TEAMS_UPDATED = "teams_updated"
    REPOS_UPDATED = "repos_updated"
    ORG_DATA_CHANGED = "org_data_changed"

    def __init__(
        self,
        event_type: str,
        org_id: str,
        tags: Optional[List[str]] = None,
        metadata: Optional[dict] = None,
    ):
        """
        Initialize a cache invalidation event.

        Args:
            event_type: Type of event (use class constants).
            org_id: Organization ID affected.
            tags: Additional cache tags to invalidate.
            metadata: Optional metadata about the event.
        """
        self.event_type = event_type
        self.org_id = org_id
        self.tags = tags or []
        self.metadata = metadata or {}

    def get_tags_to_invalidate(self) -> List[str]:
        """
        Get all cache tags that should be invalidated for this event.

        Returns:
            List of cache tag strings.
        """
        tags = [f"org:{self.org_id}"]
        tags.extend(self.tags)

        # Add event-specific tags
        if self.event_type == self.METRICS_UPDATED:
            tags.append(f"metrics:{self.org_id}")
        elif self.event_type == self.WORK_ITEMS_SYNCED:
            tags.append(f"work_items:{self.org_id}")
        elif self.event_type == self.TEAMS_UPDATED:
            tags.append(f"teams:{self.org_id}")
        elif self.event_type == self.REPOS_UPDATED:
            tags.append(f"repos:{self.org_id}")

        return tags


def invalidate_cache_for_event(
    cache: Any,
    event: CacheInvalidationEvent,
) -> int:
    """
    Invalidate cache entries based on an event.

    Args:
        cache: Cache instance (TTLCache or GraphQLCacheManager).
        event: The invalidation event.

    Returns:
        Total number of cache keys invalidated.
    """
    from dev_health_ops.api.services.cache import GraphQLCacheManager

    if isinstance(cache, GraphQLCacheManager):
        manager = cache
    else:
        manager = GraphQLCacheManager(cache)

    total_invalidated = 0
    tags = event.get_tags_to_invalidate()

    for tag in tags:
        try:
            count = manager.invalidate_by_tag(tag)
            total_invalidated += count
            logger.info(
                "Invalidated %d cache entries for tag '%s' (event: %s)",
                count,
                tag,
                event.event_type,
            )
        except Exception as e:
            logger.warning("Failed to invalidate tag '%s': %s", tag, e)

    return total_invalidated


def invalidate_on_metrics_update(cache: Any, org_id: str, day: str) -> int:
    """
    Invalidate cache when daily metrics are updated.

    Args:
        cache: Cache instance.
        org_id: Organization ID.
        day: The day that was updated (YYYY-MM-DD).

    Returns:
        Number of keys invalidated.
    """
    event = CacheInvalidationEvent(
        event_type=CacheInvalidationEvent.METRICS_UPDATED,
        org_id=org_id,
        metadata={"day": day},
    )
    return invalidate_cache_for_event(cache, event)


def invalidate_on_sync_complete(
    cache: Any,
    org_id: str,
    sync_type: str,
) -> int:
    """
    Invalidate cache when a data sync completes.

    Args:
        cache: Cache instance.
        org_id: Organization ID.
        sync_type: Type of sync (e.g., "github", "gitlab", "jira").

    Returns:
        Number of keys invalidated.
    """
    event = CacheInvalidationEvent(
        event_type=CacheInvalidationEvent.WORK_ITEMS_SYNCED,
        org_id=org_id,
        tags=[f"sync:{sync_type}"],
        metadata={"sync_type": sync_type},
    )
    return invalidate_cache_for_event(cache, event)


def invalidate_org_cache(cache: Any, org_id: str) -> int:
    """
    Invalidate all cached data for an organization.

    Use this when you need to force a complete cache refresh for an org.

    Args:
        cache: Cache instance.
        org_id: Organization ID.

    Returns:
        Number of keys invalidated.
    """
    event = CacheInvalidationEvent(
        event_type=CacheInvalidationEvent.ORG_DATA_CHANGED,
        org_id=org_id,
    )
    return invalidate_cache_for_event(cache, event)


# =============================================================================
# Redis Pub/Sub integration for distributed cache invalidation
# =============================================================================


INVALIDATION_CHANNEL = "graphql:cache:invalidate"


def publish_invalidation_event(
    redis_client: Any,
    event: CacheInvalidationEvent,
) -> None:
    """
    Publish a cache invalidation event to Redis pub/sub.

    This allows distributed instances to receive and process invalidation events.

    Args:
        redis_client: Redis client instance.
        event: The invalidation event to publish.
    """
    import json

    message = json.dumps(
        {
            "event_type": event.event_type,
            "org_id": event.org_id,
            "tags": event.tags,
            "metadata": event.metadata,
        }
    )

    try:
        redis_client.publish(INVALIDATION_CHANNEL, message)
        logger.debug("Published invalidation event: %s", event.event_type)
    except Exception as e:
        logger.warning("Failed to publish invalidation event: %s", e)


def subscribe_to_invalidation_events(
    redis_client: Any,
    cache: Any,
) -> None:
    """
    Subscribe to cache invalidation events from Redis pub/sub.

    This should be run in a background thread/task to receive events
    from other instances and invalidate the local cache.

    Args:
        redis_client: Redis client instance.
        cache: Local cache instance to invalidate.
    """
    import json

    pubsub = redis_client.pubsub()
    pubsub.subscribe(INVALIDATION_CHANNEL)

    logger.info("Subscribed to cache invalidation channel")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            event = CacheInvalidationEvent(
                event_type=data["event_type"],
                org_id=data["org_id"],
                tags=data.get("tags", []),
                metadata=data.get("metadata", {}),
            )
            invalidate_cache_for_event(cache, event)
        except Exception as e:
            logger.warning("Failed to process invalidation message: %s", e)
