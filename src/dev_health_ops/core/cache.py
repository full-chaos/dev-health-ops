"""Cache utilities — pure infrastructure, no API or framework dependencies.

Provides TTL-based caching with in-memory and Redis backends. Shared by the
API layer, Celery workers, and any other module without creating circular
imports.

Nothing here imports from dev_health_ops.api.*.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


def _safe_key_label(key: str) -> str:
    """Return a log-safe label for a cache key.

    Cache keys built by ``filter_cache_key`` embed the full serialized filter
    payload (org_id plus user-controlled scope/repo/developer filters), so the
    raw key must never reach centralized logs. We log the prefix (before the
    first ':') plus a short stable digest, which still lets repeated failures
    for the same key be correlated without leaking tenant data.
    """
    prefix = key.split(":", 1)[0]
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}:{digest}"


# =============================================================================
# Per-org cache epoch (CHAOS-4226)
# =============================================================================
#
# Filter-scoped cache entries (``home:{...}``, ``explain:{...}``) embed the
# full filter payload, so no producer can enumerate "every key for org X" to
# delete them. Instead every org has ONE epoch key; readers fold its value
# into their cache key (``epoch_cache_key``) and writers that change the
# org's data bump it (INCR). A bump makes every older entry unreachable in
# one write; the orphans age out by their own TTL.
#
# The Go native finalize (internal/cacheinvalidation) bumps the SAME key
# after a sync run commits. The two sides never hand-copy each other's
# string: tests/test_cache_epoch_contract.py regenerates
# contracts/cache-invalidation/v1/org_cache_epoch_key.json from
# ``org_cache_epoch_key`` and the Go contract test asserts against it.

ORG_CACHE_EPOCH_KEY_PREFIX = "cache_epoch:org:"

# Expiry the bumping side sets on the epoch key (INCR + EXPIRE refreshes it
# on every bump). It must exceed every epoch-scoped entry TTL by a wide
# margin: if the epoch key expired while entries stamped with epoch N were
# still alive, a reader would fall back to epoch 0 -- harmless (0 never
# collides with N >= 1), but the entries stamped N would silently keep
# serving after the NEXT bump re-created the key at 1. 30 days against a
# 60-120s entry TTL closes that window by five orders of magnitude.
ORG_CACHE_EPOCH_TTL_SECONDS = 30 * 24 * 60 * 60

# Largest entry TTL an epoch-scoped cache may use; ``epoch_cache_key``
# refuses a cache whose TTL is longer, so the margin above is enforced at
# the read site rather than trusted.
EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS = 3600
EPOCH_SCOPED_CACHE_TTL_MARGIN = 100

assert ORG_CACHE_EPOCH_TTL_SECONDS >= (
    EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS * EPOCH_SCOPED_CACHE_TTL_MARGIN
)


def epoch_scoped(cache: TTLCache) -> TTLCache:
    """Declare a cache as epoch-scoped at CONSTRUCTION time.

    Refuses an entry TTL above ``EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS`` once,
    where the TTL is decided (``api/main.py``), instead of re-checking a
    static number on every request in ``epoch_cache_key``.
    """
    if cache.ttl_seconds > EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS:
        raise ValueError(
            f"epoch-scoped cache ttl {cache.ttl_seconds}s exceeds "
            f"{EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS}s; the epoch key expiry "
            "margin would no longer hold"
        )
    return cache


def _decode_epoch(raw: Any) -> int:
    """Absent (None) reads as 0; anything that is not a non-negative int is 0
    as well -- an epoch never goes backwards from an unexpected shape."""
    if isinstance(raw, bool):
        return 0
    if isinstance(raw, int):
        return max(raw, 0)
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return 0


def org_cache_epoch_key(org_id: str) -> str:
    """The Valkey key holding an organization's cache epoch.

    Identical across MemoryBackend and RedisBackend so a Valkey outage (and
    the per-process memory fallback it triggers) never splits the scheme.
    """
    return f"{ORG_CACHE_EPOCH_KEY_PREFIX}{org_id}"


class CacheBackend(ABC):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: str) -> Any | None:
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

    def mget(self, keys: list[str]) -> list[Any | None]:
        """Batch get. Default implementation calls get() per key.

        Backends with a native multi-get primitive (e.g. Redis MGET) should
        override this to issue a single round-trip.
        """
        return [self.get(k) for k in keys]

    def incr(self, key: str, ttl_seconds: int) -> int | None:
        """Increment an integer counter (absent -> 1) and (re)set its expiry.

        Returns the new value, or None when the backend could not apply it.
        Default implementation is a non-atomic get/set; backends with a
        native INCR (Redis) override it.
        """
        current = self.get(key)
        value = (int(current) if isinstance(current, int) else 0) + 1
        self.set(key, value, ttl_seconds)
        return value

    # Reason the most recent get_epoch returned None, for the bypass log
    # line; backends that cannot fail leave it empty.
    _last_epoch_error: str = ""

    def last_epoch_error(self) -> str:
        return self._last_epoch_error

    def get_epoch(self, key: str) -> int | None:
        """Tri-state read of an integer epoch: value, 0 when ABSENT, or None
        when UNREADABLE (backend error). ``get`` collapses absent and error
        into one None; an epoch reader must not, because "unknown epoch"
        treated as 0 would let a stale epoch-0 entry serve after a bump
        (codex R2, CHAOS-4226). The memory backend cannot fail, so the
        default never returns None.
        """
        return _decode_epoch(self.get(key))


class MemoryBackend(CacheBackend):
    """In-memory cache backend (default)."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
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
            import valkey as redis

            self._client = redis.from_url(redis_url, decode_responses=True)
            self._client.ping()  # Test connection
            self._available = True
            logger.info("Redis cache connected: %s", redis_url.split("@")[-1])
        except Exception as e:
            logger.warning("Redis unavailable, falling back to memory: %s", e)
            self._available = False
            self._fallback = MemoryBackend()

    def get(self, key: str) -> Any | None:
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

    def mget(self, keys: list[str]) -> list[Any | None]:
        """Batch get via Redis MGET — a single round-trip instead of N."""
        if not keys:
            return []
        if not self._available:
            return self._fallback.mget(keys)
        try:
            raw_values = self._client.mget(keys)
        except Exception as e:
            logger.warning("Redis mget failed: %s", e)
            return [None] * len(keys)
        results: list[Any | None] = []
        for raw in raw_values:
            if raw is None:
                results.append(None)
                continue
            try:
                results.append(json.loads(raw))
            except Exception as e:
                logger.warning("Redis mget value decode failed: %s", e)
                results.append(None)
        return results

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if not self._available:
            self._fallback.set(key, value, ttl_seconds)
            return
        try:
            self._client.setex(key, ttl_seconds, json.dumps(value))
        except Exception as e:
            logger.warning(
                "Redis set failed for key=%s value_type=%s ttl=%s: %s",
                _safe_key_label(key),
                type(value).__name__,
                ttl_seconds,
                e,
                exc_info=True,
            )

    def get_epoch(self, key: str) -> int | None:
        if not self._available:
            return self._fallback.get_epoch(key)
        try:
            raw = self._client.get(key)
        except Exception as e:
            self._last_epoch_error = f"{type(e).__name__}: {e}"
            logger.warning(
                "Redis epoch get failed for key=%s: %s", _safe_key_label(key), e
            )
            return None
        if raw is None:
            return 0
        try:
            return _decode_epoch(json.loads(raw))
        except (TypeError, ValueError) as e:
            self._last_epoch_error = f"undecodable epoch value: {type(e).__name__}"
            logger.warning(
                "Redis epoch value undecodable for key=%s: %s", _safe_key_label(key), e
            )
            return None

    def incr(self, key: str, ttl_seconds: int) -> int | None:
        """INCR + EXPIRE in one pipeline -- the same two commands the Go
        finalize issues (internal/cacheinvalidation), so a Python-side bump
        and a Go-side bump are indistinguishable to readers."""
        if not self._available:
            return self._fallback.incr(key, ttl_seconds)
        try:
            pipe = self._client.pipeline()
            pipe.incr(key)
            pipe.expire(key, ttl_seconds)
            value, _ = pipe.execute()
            return int(value)
        except Exception as e:
            logger.warning(
                "Redis incr failed for key=%s ttl=%s: %s",
                _safe_key_label(key),
                ttl_seconds,
                e,
                exc_info=True,
            )
            return None

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
        backend: CacheBackend | None = None,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self._backend = backend or MemoryBackend()

    def get(self, key: str) -> Any | None:
        return self._backend.get(key)

    def mget(self, keys: list[str]) -> list[Any | None]:
        """Batch get delegating to the underlying backend."""
        return self._backend.mget(keys)

    def set(self, key: str, value: Any) -> None:
        self._backend.set(key, value, self.ttl_seconds)

    def org_epoch(self, org_id: str) -> int | None:
        """Current cache epoch for an org: ONE backend GET. Absent reads as 0
        (a fresh org, or the memory fallback) so the key is deterministic;
        UNREADABLE (a backend error) reads as None so the caller bypasses
        the cache instead of guessing an epoch."""
        return self._backend.get_epoch(org_cache_epoch_key(org_id))

    def last_epoch_error(self, org_id: str) -> str:
        """Why the most recent ``org_epoch`` read returned None (empty if it
        did not); consumed by the bypass log line."""
        del org_id  # one backend per cache; the key is implied
        return self._backend.last_epoch_error()

    def bump_org_epoch(self, org_id: str) -> int | None:
        """Invalidate every epoch-scoped entry of an org in one write."""
        if not org_id:
            return None
        return self._backend.incr(
            org_cache_epoch_key(org_id), ORG_CACHE_EPOCH_TTL_SECONDS
        )

    def status(self) -> str:
        """Returns the status of the underlying backend."""
        return self._backend.status()


def create_cache(
    ttl_seconds: int,
    redis_url: str | None = None,
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

    def get_query_result(self, key: str) -> Any | None:
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
        tags: list | None = None,
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
