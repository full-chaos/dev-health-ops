"""GraphQL application factory for analytics API."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from fastapi import Request
from strawberry.fastapi import GraphQLRouter

from .context import GraphQLContext, build_context
from .persisted import get_schema_version
from .schema import schema


logger = logging.getLogger(__name__)

# Global cache instance for cross-request caching
_graphql_cache: Optional[Any] = None


def _get_cache() -> Optional[Any]:
    """Get or create the shared cache instance."""
    global _graphql_cache
    if _graphql_cache is None:
        try:
            from dev_health_ops.api.services.cache import create_cache

            # 5-minute TTL for GraphQL entity caching
            _graphql_cache = create_cache(ttl_seconds=300)
            logger.info("GraphQL cache initialized")
        except Exception as e:
            logger.warning("Failed to initialize GraphQL cache: %s", e)
    return _graphql_cache


async def get_context(request: Request) -> GraphQLContext:
    """
    Build GraphQL context from FastAPI request.

    Extracts org_id from headers or query params, sets up DB connection,
    and initializes DataLoaders for the request.
    """
    logger.debug("Entering get_context")
    # Get org_id from header or query param
    org_id = request.headers.get("X-Org-Id", "")
    if not org_id:
        org_id = request.query_params.get("org_id", "")

    # Get DB URL from environment
    db_url = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL", "")

    # Check for persisted query
    persisted_query_id = request.headers.get("X-Persisted-Query-Id")

    # Get ClickHouse client
    client = None
    try:
        from dev_health_ops.api.queries.client import get_global_client
        import asyncio

        logger.debug("Getting ClickHouse client for %s", db_url)
        client = await asyncio.wait_for(get_global_client(db_url), timeout=5.0)
    except Exception as e:
        logger.warning("Failed to get ClickHouse client: %s", e)

    # Get cache for cross-request caching
    cache = _get_cache()

    # Build context with DataLoaders
    # We allow placeholder org_id here since resolvers require it as an argument
    context = build_context(
        org_id=org_id or "placeholder",  # Will be overridden by resolver
        db_url=db_url,
        persisted_query_id=persisted_query_id,
        client=client,
        cache=cache,
    )

    return context


def create_graphql_app(
    db_url: Optional[str] = None,
) -> GraphQLRouter[GraphQLContext, None]:
    """
    Create the GraphQL router for the analytics API.

    Args:
        db_url: Optional database URL override.

    Returns:
        Strawberry GraphQL router to mount in FastAPI.
    """

    async def context_getter(request: Request) -> GraphQLContext:
        """Context getter with optional db_url override."""
        context = await get_context(request)
        if db_url:
            context.db_url = db_url
        return context

    router = GraphQLRouter(
        schema=schema,
        context_getter=context_getter,
        path="",
    )

    return router


def get_graphql_info() -> dict:
    """Get information about the GraphQL API."""
    return {
        "schema_version": get_schema_version(),
        "endpoints": {
            "graphql": "/graphql",
            "graphiql": "/graphql",
            "subscriptions": "/graphql",
        },
        "features": [
            "catalog",
            "analytics",
            "timeseries",
            "breakdowns",
            "sankey",
            "persisted_queries",
            "subscriptions",
            "dataloaders",
            "caching",
        ],
        "subscription_protocol": "graphql-ws",
    }


async def init_pubsub() -> None:
    """
    Initialize the PubSub system.

    Call this during application startup to establish Redis connections.
    """
    try:
        from .pubsub import get_pubsub

        pubsub = await get_pubsub()
        logger.info("PubSub initialized, Redis available: %s", pubsub._available)
    except Exception as e:
        logger.warning("Failed to initialize PubSub: %s", e)


async def shutdown_pubsub() -> None:
    """
    Shutdown the PubSub system.

    Call this during application shutdown to close Redis connections.
    """
    try:
        from .pubsub import _pubsub

        if _pubsub:
            await _pubsub.disconnect()
            logger.info("PubSub disconnected")
    except Exception as e:
        logger.warning("Failed to shutdown PubSub: %s", e)
