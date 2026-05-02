"""GraphQL application factory for analytics API."""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import HTTPException, Request
from strawberry.fastapi import GraphQLRouter

from .context import GraphQLContext, build_context
from .persisted import get_schema_version
from .schema import schema

logger = logging.getLogger(__name__)

DEFAULT_CLICKHOUSE_URI = "clickhouse://localhost:8123/default"

# Global cache instance for cross-request caching
_graphql_cache: Any | None = None


def _get_cache() -> Any | None:
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


def _graphql_auth_required() -> bool:
    """Check whether GraphQL authentication is enforced.

    Defaults to True (enforced).  Set GRAPHQL_AUTH_REQUIRED=false to
    disable — useful for local development / GraphiQL playground.
    """
    return os.getenv("GRAPHQL_AUTH_REQUIRED", "true").lower() != "false"


async def get_context(request: Request) -> GraphQLContext:
    """
    Build GraphQL context from FastAPI request.

    Authentication is enforced by default (GRAPHQL_AUTH_REQUIRED != "false").
    org_id is resolved from the JWT, then OrgIdMiddleware contextvar,
    then query params as a last resort.
    """
    logger.debug("Entering get_context")
    from dev_health_ops.api.services.auth import (
        extract_token_from_header,
        get_auth_service,
        get_current_org_id,
    )

    db_url = os.getenv("CLICKHOUSE_URI") or DEFAULT_CLICKHOUSE_URI
    persisted_query_id = request.headers.get("X-Persisted-Query-Id")

    # --- Authenticate --------------------------------------------------------
    user = None
    auth_header = request.headers.get("Authorization")
    if auth_header:
        token = extract_token_from_header(auth_header)
        if token:
            auth_service = get_auth_service()
            user = auth_service.get_authenticated_user(token)
            logger.debug("Authenticated user: %s", user.email if user else None)

    if _graphql_auth_required() and user is None:
        raise HTTPException(status_code=401, detail="Authentication required")

    # --- Resolve org_id (prefer JWT claim, fall back to context/param) -------
    org_id = ""
    if user and user.org_id:
        org_id = user.org_id
    if not org_id:
        org_id = get_current_org_id() or request.query_params.get("org_id", "") or ""

    # --- Build context -------------------------------------------------------
    client = None
    try:
        import asyncio

        from dev_health_ops.api.queries.client import get_global_client

        logger.debug("Getting ClickHouse client for %s", db_url)
        client = await asyncio.wait_for(get_global_client(db_url), timeout=5.0)
    except Exception as e:
        logger.warning("Failed to get ClickHouse client: %s", e)
    cache = _get_cache()
    context = build_context(
        org_id=org_id,
        db_url=db_url,
        persisted_query_id=persisted_query_id,
        client=client,
        cache=cache,
        user=user,
    )
    return context


def create_graphql_app(
    db_url: str | None = None,
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

    router = GraphQLRouter[GraphQLContext, None](
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
