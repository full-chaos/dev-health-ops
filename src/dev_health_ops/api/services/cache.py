"""Cache service — re-exports from core.cache for backward compatibility.

The implementation has moved to dev_health_ops.core.cache.
Import from there directly for new code.
"""

from dev_health_ops.core.cache import (  # noqa: F401
    CacheBackend,
    GraphQLCacheManager,
    MemoryBackend,
    RedisBackend,
    TTLCache,
    create_cache,
    create_graphql_cache,
)
