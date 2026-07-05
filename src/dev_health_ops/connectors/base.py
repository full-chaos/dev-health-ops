"""
Base connector class for Git repository connectors.

This module provides an abstract base class that defines the common interface
for all Git connectors (GitHub, GitLab, local, etc.).
"""

import asyncio
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import valkey as redis

from dev_health_ops.connectors.models import (
    Repository,
    RepoStats,
)
from dev_health_ops.exceptions import RateLimitException as _RootRateLimitException

logger = logging.getLogger(__name__)


class RateLimitException(_RootRateLimitException):
    """Rate-limit error raised by the legacy Git connectors.

    Subclasses the root :class:`dev_health_ops.exceptions.RateLimitException` so a
    rate limit raised deep in a legacy connector (``connectors/gitlab.py``) is
    caught by the worker deferral branch in ``workers/sync_units.py`` and deferred
    as retryable work, instead of falling through to the generic ``Exception``
    handler and becoming a FAILED unit (the class-split bug this fixes).

    Kept as a *distinct* subclass rather than a plain alias so the
    ``retry_with_backoff(exceptions=(RateLimitException, ...))`` decorator sites
    in the legacy connectors keep referencing it by identity and retain their
    existing in-connector retry semantics. The constructor is inherited from the
    root (``message``, ``retry_after_seconds``, keyword-only ``signal``).
    """


@dataclass
class BatchResult:
    """Result of a batch repository processing operation."""

    repository: Repository
    stats: RepoStats | None = None
    error: str | None = None
    success: bool = True


class GitConnector(ABC):
    """
    Abstract base class for Git repository connectors.

    This class defines the common interface that all Git connectors
    (GitHub, GitLab, local) must implement.
    """

    def __init__(
        self,
        per_page: int = 100,
        max_workers: int = 4,
        cache: redis.Redis | None = None,
        cache_prefix: str = "git:",
        cache_ttl: int = 3600,
    ):
        """
        Initialize the base connector.

        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        :param cache: Optional Redis client for caching.
        :param cache_prefix: Prefix for Redis keys.
        :param cache_ttl: TTL for cached items in seconds.
        """
        self.per_page = per_page
        self.max_workers = max_workers
        self.cache = cache
        self.cache_prefix = cache_prefix
        self.cache_ttl = cache_ttl
        # Lazy-created so workers not in an asyncio context don't pay the cost.
        self._concurrency_semaphore: asyncio.Semaphore | None = None

    @property
    def concurrency_semaphore(self) -> asyncio.Semaphore:
        """Shared semaphore gating concurrent async calls to this connector."""
        if self._concurrency_semaphore is None:
            self._concurrency_semaphore = asyncio.Semaphore(self.max_workers)
        return self._concurrency_semaphore

    @abstractmethod
    def close(self) -> None:
        """Close the connector and cleanup resources."""
        pass

    # --- Caching Support ---

    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate a stable cache key for a method and its arguments."""
        # Sort kwargs to ensure stability
        sorted_args = sorted(kwargs.items())
        args_str = json.dumps(sorted_args, default=str)
        args_hash = hashlib.md5(args_str.encode(), usedforsecurity=False).hexdigest()
        return f"{self.cache_prefix}{method}:{args_hash}"

    def _get_cached_item(self, key: str, model_class: Any) -> Any | None:
        """Retrieve and deserialize an item from cache."""
        if not self.cache:
            return None

        try:
            data = self.cache.get(key)
            if data:
                logger.debug(f"Cache hit: {key}")
                raw_data = json.loads(data)  # type: ignore[arg-type]
                if isinstance(raw_data, list):
                    return [model_class(**item) for item in raw_data]
                return model_class(**raw_data)
        except Exception as e:
            logger.warning(f"Cache retrieval error for {key}: {e}")
        return None

    def _set_cached_item(self, key: str, item: Any) -> None:
        """Serialize and store an item in cache."""
        if not self.cache:
            return

        try:
            if isinstance(item, list):
                data = json.dumps(
                    [i.__dict__ if hasattr(i, "__dict__") else i for i in item],
                    default=str,
                )
            elif hasattr(item, "__dict__"):
                data = json.dumps(item.__dict__, default=str)
            else:
                data = json.dumps(item, default=str)

            self.cache.setex(key, self.cache_ttl, data)
            logger.debug(f"Cache store: {key}")
        except Exception as e:
            logger.warning(f"Cache storage error for {key}: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
