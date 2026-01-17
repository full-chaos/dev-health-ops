"""GraphQL application factory for analytics API."""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import Request
from strawberry.fastapi import GraphQLRouter

from .context import GraphQLContext
from .persisted import get_schema_version
from .schema import schema


logger = logging.getLogger(__name__)


async def get_context(request: Request) -> GraphQLContext:
    """
    Build GraphQL context from FastAPI request.

    Extracts org_id from headers or query params, and sets up DB connection.
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

    # Build context (will be updated in resolver with explicit org_id)
    # We allow empty org_id here since resolvers require it as an argument
    context = GraphQLContext(
        org_id=org_id or "placeholder",  # Will be overridden by resolver
        db_url=db_url,
        persisted_query_id=persisted_query_id,
    )

    # Get ClickHouse client
    try:
        from api.queries.client import get_global_client
        import asyncio

        logger.debug("Getting ClickHouse client for %s", db_url)
        context.client = await asyncio.wait_for(get_global_client(db_url), timeout=5.0)
    except Exception as e:
        logger.warning("Failed to get ClickHouse client: %s", e)
        context.client = None

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
        },
        "features": [
            "catalog",
            "analytics",
            "timeseries",
            "breakdowns",
            "sankey",
            "persisted_queries",
        ],
    }
