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
