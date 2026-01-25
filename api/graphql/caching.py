"""GraphQL query-level caching utilities."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
from typing import Any, Callable, Dict, Optional, TypeVar

from .context import GraphQLContext

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def _serialize_arg(arg: Any) -> Any:
    """Serialize an argument for cache key generation."""
    if arg is None:
        return None
    if isinstance(arg, (str, int, float, bool)):
        return arg
    if isinstance(arg, (list, tuple)):
        return [_serialize_arg(item) for item in arg]
    if isinstance(arg, dict):
        return {k: _serialize_arg(v) for k, v in sorted(arg.items())}
    if hasattr(arg, "__dict__"):
        # Handle dataclasses and Strawberry input types
        return {k: _serialize_arg(v) for k, v in sorted(vars(arg).items())}
    if hasattr(arg, "value"):
        # Handle Enum types
        return arg.value
    return str(arg)


def build_cache_key(
    prefix: str,
    org_id: str,
    *args: Any,
    **kwargs: Any,
) -> str:
    """
    Build a cache key from function arguments.

    Args:
        prefix: Cache key prefix (e.g., resolver name).
        org_id: Organization ID for scoping.
        *args: Positional arguments to include in key.
        **kwargs: Keyword arguments to include in key.

    Returns:
        A deterministic cache key string.
    """
    key_data = {
        "prefix": prefix,
        "org_id": org_id,
        "args": [_serialize_arg(arg) for arg in args],
        "kwargs": {k: _serialize_arg(v) for k, v in sorted(kwargs.items())},
    }
    key_json = json.dumps(key_data, sort_keys=True, default=str)
    key_hash = hashlib.sha256(key_json.encode()).hexdigest()[:32]
    return f"gql:{prefix}:{org_id}:{key_hash}"


def cached_resolver(
    ttl_seconds: int = 300,
    key_prefix: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator to cache GraphQL resolver results.

    The cache is stored in the context's cache backend if available.
    Cache keys are built from the resolver name, org_id, and arguments.

    Args:
        ttl_seconds: Cache TTL in seconds (default 5 minutes).
        key_prefix: Optional custom key prefix (defaults to function name).

    Returns:
        Decorated resolver function with caching.

    Example:
        @cached_resolver(ttl_seconds=60)
        async def resolve_home(context: GraphQLContext, filters: FilterInput) -> HomeResult:
            ...
    """

    def decorator(func: F) -> F:
        prefix = key_prefix or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Extract context from args or kwargs
            context: Optional[GraphQLContext] = None
            for arg in args:
                if isinstance(arg, GraphQLContext):
                    context = arg
                    break
            if context is None:
                context = kwargs.get("context")

            # If no cache available, execute directly
            if context is None or context.cache is None:
                return await func(*args, **kwargs)

            # Build cache key
            org_id = getattr(context, "org_id", "unknown")
            cache_key = build_cache_key(prefix, org_id, *args[1:], **kwargs)

            # Check cache
            try:
                cached_value = context.cache.get(cache_key)
                if cached_value is not None:
                    logger.debug("Cache hit for %s", cache_key)
                    return cached_value
            except Exception as e:
                logger.debug("Cache get failed: %s", e)

            # Execute resolver
            result = await func(*args, **kwargs)

            # Store in cache
            try:
                # Convert result to cacheable format if needed
                cacheable = _make_cacheable(result)
                context.cache.set(cache_key, cacheable)
                logger.debug("Cached result for %s", cache_key)
            except Exception as e:
                logger.debug("Cache set failed: %s", e)

            return result

        return wrapper  # type: ignore

    return decorator


def _make_cacheable(value: Any) -> Any:
    """Convert a value to a JSON-serializable format for caching."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_make_cacheable(item) for item in value]
    if isinstance(value, dict):
        return {k: _make_cacheable(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        # Handle dataclasses and Strawberry types
        result: Dict[str, Any] = {"__type__": type(value).__name__}
        for k, v in vars(value).items():
            if not k.startswith("_"):
                result[k] = _make_cacheable(v)
        return result
    return str(value)


class CacheInvalidator:
    """
    Helper for cache invalidation by tags.

    Supports tag-based invalidation where cached items are associated with tags
    (e.g., "org:abc", "team:xyz") and can be invalidated in bulk.
    """

    TAG_PREFIX = "cache_tag:"

    def __init__(self, cache: Any):
        """
        Initialize the cache invalidator.

        Args:
            cache: TTLCache instance with get/set methods.
        """
        self._cache = cache

    def tag_key(self, cache_key: str, *tags: str) -> None:
        """
        Associate a cache key with tags for later invalidation.

        Args:
            cache_key: The cache key to tag.
            *tags: Tags to associate with the key.
        """
        for tag in tags:
            tag_set_key = f"{self.TAG_PREFIX}{tag}"
            existing = self._cache.get(tag_set_key) or []
            if cache_key not in existing:
                existing.append(cache_key)
                self._cache.set(tag_set_key, existing)

    def invalidate_tag(self, tag: str) -> int:
        """
        Invalidate all cache entries associated with a tag.

        Args:
            tag: The tag to invalidate.

        Returns:
            Number of keys invalidated.
        """
        tag_set_key = f"{self.TAG_PREFIX}{tag}"
        keys = self._cache.get(tag_set_key) or []
        count = 0
        for key in keys:
            try:
                # Set to None or use delete if available
                if hasattr(self._cache, "delete"):
                    self._cache.delete(key)
                else:
                    # Setting to empty value with short TTL effectively invalidates
                    self._cache._backend.set(key, None, 1)
                count += 1
            except Exception as e:
                logger.warning("Failed to invalidate key %s: %s", key, e)
        # Clear the tag set
        try:
            if hasattr(self._cache, "delete"):
                self._cache.delete(tag_set_key)
            else:
                self._cache._backend.set(tag_set_key, None, 1)
        except Exception as e:
            logger.warning("Failed to clear tag set %s: %s", tag_set_key, e)
        return count


def invalidate_org_cache(cache: Any, org_id: str) -> int:
    """
    Invalidate all cached data for an organization.

    Args:
        cache: TTLCache instance.
        org_id: Organization ID to invalidate.

    Returns:
        Number of keys invalidated.
    """
    invalidator = CacheInvalidator(cache)
    return invalidator.invalidate_tag(f"org:{org_id}")
