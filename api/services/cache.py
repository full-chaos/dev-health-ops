from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class CacheBackend(ABC):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Set a value in the cache with TTL."""
        pass

    @abstractmethod
    def status(self) -> str:
        """Check the status of the cache backend."""
        pass


class MemoryBackend(CacheBackend):
    """In-memory cache backend (default)."""

    def __init__(self) -> None:
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if time.time() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._store[key] = (time.time() + ttl_seconds, value)

    def status(self) -> str:
        return "ok"


class RedisBackend(CacheBackend):
    """Redis-backed cache for distributed deployments."""

    def __init__(self, redis_url: str) -> None:
        try:
            import redis

            self._client = redis.from_url(redis_url, decode_responses=True)
            self._client.ping()  # Test connection
            self._available = True
            logger.info("Redis cache connected: %s", redis_url.split("@")[-1])
        except Exception as e:
            logger.warning("Redis unavailable, falling back to memory: %s", e)
            self._available = False
            self._fallback = MemoryBackend()

    def get(self, key: str) -> Optional[Any]:
        if not self._available:
            return self._fallback.get(key)
        try:
            raw = self._client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as e:
            logger.warning("Redis get failed: %s", e)
            return None

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if not self._available:
            self._fallback.set(key, value, ttl_seconds)
            return
        try:
            self._client.setex(key, ttl_seconds, json.dumps(value))
        except Exception as e:
            logger.warning("Redis set failed: %s", e)

    def status(self) -> str:
        if not self._available:
            return "down"
        try:
            self._client.ping()
            return "ok"
        except Exception:
            return "down"


class TTLCache:
    """Cache with configurable backend (memory or Redis)."""

    def __init__(
        self,
        ttl_seconds: int,
        backend: Optional[CacheBackend] = None,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self._backend = backend or MemoryBackend()

    def get(self, key: str) -> Optional[Any]:
        return self._backend.get(key)

    def set(self, key: str, value: Any) -> None:
        self._backend.set(key, value, self.ttl_seconds)

    def status(self) -> str:
        """Returns the status of the underlying backend."""
        return self._backend.status()


def create_cache(
    ttl_seconds: int,
    redis_url: Optional[str] = None,
) -> TTLCache:
    """Factory function to create a cache with the appropriate backend.

    If REDIS_URL is set in environment or provided, uses Redis.
    Otherwise falls back to in-memory cache.
    """
    url = redis_url or os.getenv("REDIS_URL")
    if url:
        backend: CacheBackend = RedisBackend(url)
    else:
        backend = MemoryBackend()
    return TTLCache(ttl_seconds=ttl_seconds, backend=backend)


# =============================================================================
# GraphQL-specific cache utilities
# =============================================================================


class GraphQLCacheManager:
    """
    Specialized cache manager for GraphQL operations.

    Provides methods for caching query results with org scoping
    and tag-based invalidation.
    """

    def __init__(self, cache: TTLCache):
        """
        Initialize the GraphQL cache manager.

        Args:
            cache: Underlying TTLCache instance.
        """
        self._cache = cache
        self._tag_prefix = "gql_tag:"

    def get_query_result(self, key: str) -> Optional[Any]:
        """
        Get a cached query result.

        Args:
            key: Cache key for the query.

        Returns:
            Cached result or None if not found/expired.
        """
        return self._cache.get(key)

    def set_query_result(
        self,
        key: str,
        value: Any,
        tags: Optional[list] = None,
    ) -> None:
        """
        Cache a query result with optional tags.

        Args:
            key: Cache key for the query.
            value: Result to cache.
            tags: Optional list of tags for invalidation grouping.
        """
        self._cache.set(key, value)
        if tags:
            for tag in tags:
                self._add_key_to_tag(tag, key)

    def _add_key_to_tag(self, tag: str, key: str) -> None:
        """Associate a cache key with a tag."""
        tag_key = f"{self._tag_prefix}{tag}"
        existing = self._cache.get(tag_key) or []
        if key not in existing:
            existing.append(key)
            self._cache.set(tag_key, existing)

    def invalidate_by_tag(self, tag: str) -> int:
        """
        Invalidate all cached items with a given tag.

        Args:
            tag: Tag to invalidate.

        Returns:
            Number of keys invalidated.
        """
        tag_key = f"{self._tag_prefix}{tag}"
        keys = self._cache.get(tag_key) or []
        count = 0
        for key in keys:
            try:
                self._cache._backend.set(key, None, 1)
                count += 1
            except Exception as e:
                logger.warning("Failed to invalidate %s: %s", key, e)
        # Clear tag set (best-effort; log but do not raise on failure)
        try:
            self._cache._backend.set(tag_key, None, 1)
        except Exception as e:
            logger.debug(
                "Failed to clear tag key %s from cache backend: %s", tag_key, e
            )
        return count

    def invalidate_org(self, org_id: str) -> int:
        """
        Invalidate all cached data for an organization.

        Args:
            org_id: Organization ID.

        Returns:
            Number of keys invalidated.
        """
        return self.invalidate_by_tag(f"org:{org_id}")

    def status(self) -> str:
        """Get cache backend status."""
        return self._cache.status()


def create_graphql_cache(ttl_seconds: int = 300) -> GraphQLCacheManager:
    """
    Create a GraphQL-specific cache manager.

    Args:
        ttl_seconds: Default TTL for cached items.

    Returns:
        GraphQLCacheManager instance.
    """
    cache = create_cache(ttl_seconds=ttl_seconds)
    return GraphQLCacheManager(cache)
