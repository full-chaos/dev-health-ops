"""Base DataLoader with optional caching integration."""

from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, Sequence, TypeVar

from strawberry.dataloader import DataLoader

logger = logging.getLogger(__name__)

K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type


def make_cache_key(prefix: str, key: Any) -> str:
    """
    Generate a stable cache key from a prefix and key object.

    Args:
        prefix: Cache key prefix (e.g., 'team', 'repo').
        key: The key object to hash.

    Returns:
        A stable hash-based cache key string.
    """
    key_str = json.dumps(key, sort_keys=True, default=str)
    key_hash = hashlib.sha256(key_str.encode()).hexdigest()[:16]
    return f"loader:{prefix}:{key_hash}"


class CachedDataLoader(DataLoader[K, V], Generic[K, V], ABC):
    """
    Base DataLoader with optional external cache integration.

    Strawberry DataLoaders already provide per-request caching (deduplication).
    This class adds optional cross-request caching via an external cache backend.

    Attributes:
        cache: Optional cache backend (must implement get/set with TTL).
        cache_ttl: Cache TTL in seconds for cross-request caching.
        cache_prefix: Prefix for cache keys.
    """

    def __init__(
        self,
        cache: Optional[Any] = None,
        cache_ttl: int = 300,
        cache_prefix: str = "loader",
    ):
        """
        Initialize the cached DataLoader.

        Args:
            cache: Optional cache backend with get(key) and set(key, value) methods.
            cache_ttl: TTL for cached values in seconds.
            cache_prefix: Prefix for cache keys to avoid collisions.
        """
        super().__init__(load_fn=self._load_with_cache)
        self._external_cache = cache
        self._cache_ttl = cache_ttl
        self._cache_prefix = cache_prefix

    async def _load_with_cache(self, keys: List[K]) -> Sequence[V]:
        """
        Load values with optional cache lookup.

        First checks cache for each key, then batch loads missing keys,
        and finally caches the newly loaded values.
        """
        results: Dict[int, V] = {}
        missing_keys: List[tuple[int, K]] = []

        # Check cache for each key
        if self._external_cache:
            for idx, key in enumerate(keys):
                cache_key = make_cache_key(self._cache_prefix, key)
                try:
                    cached = self._external_cache.get(cache_key)
                    if cached is not None:
                        results[idx] = cached
                        continue
                except Exception as e:
                    logger.debug("Cache lookup failed for %s: %s", cache_key, e)
                missing_keys.append((idx, key))
        else:
            missing_keys = [(idx, key) for idx, key in enumerate(keys)]

        # Batch load missing keys
        if missing_keys:
            missing_indices, missing_key_values = (
                zip(*missing_keys) if missing_keys else ([], [])
            )
            loaded_values = await self.batch_load(list(missing_key_values))

            # Map loaded values back and cache them
            for idx, key, value in zip(
                missing_indices, missing_key_values, loaded_values
            ):
                results[idx] = value
                if self._external_cache and value is not None:
                    cache_key = make_cache_key(self._cache_prefix, key)
                    try:
                        self._external_cache.set(cache_key, value)
                    except Exception as e:
                        logger.debug("Cache set failed for %s: %s", cache_key, e)

        # Return results in original key order
        return [results[idx] for idx in range(len(keys))]

    @abstractmethod
    async def batch_load(self, keys: List[K]) -> List[V]:
        """
        Batch load values for the given keys.

        Subclasses must implement this method to perform the actual data loading.
        The returned list must have the same length as keys, with None for missing.

        Args:
            keys: List of keys to load.

        Returns:
            List of values corresponding to keys (same order and length).
        """
        raise NotImplementedError


class SimpleDataLoader(DataLoader[K, V], Generic[K, V]):
    """
    Simple DataLoader without external caching.

    Uses only Strawberry's built-in per-request deduplication.
    """

    def __init__(self):
        super().__init__(load_fn=self._batch_load_wrapper)

    async def _batch_load_wrapper(self, keys: List[K]) -> Sequence[V]:
        """Wrapper to call the abstract batch_load method."""
        return await self.batch_load(keys)

    @abstractmethod
    async def batch_load(self, keys: List[K]) -> List[V]:
        """
        Batch load values for the given keys.

        Args:
            keys: List of keys to load.

        Returns:
            List of values corresponding to keys (same order and length).
        """
        raise NotImplementedError
