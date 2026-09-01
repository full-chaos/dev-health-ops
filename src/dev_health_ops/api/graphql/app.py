"""GraphQL application factory for analytics API."""

from __future__ import annotations

import logging
import os
import uuid
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, Request

from .context import GraphQLContext, build_context
from .go_api_dispatcher import GoApiDispatchRouter
from .persisted import get_schema_version
from .schema import schema
from .security import is_graphql_ide_enabled

if TYPE_CHECKING:
    from dev_health_ops.licensing.types import LicenseTier

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
        get_impersonation_context,
    )
    from dev_health_ops.db import get_postgres_session

    db_url = os.getenv("CLICKHOUSE_URI") or DEFAULT_CLICKHOUSE_URI
    persisted_query_id = request.headers.get("X-Persisted-Query-Id")

    # --- Authenticate --------------------------------------------------------
    user = None
    auth_header = request.headers.get("Authorization")
    if auth_header:
        token = extract_token_from_header(auth_header)
        if token:
            auth_service = get_auth_service()
            async with get_postgres_session() as db:
                user = await auth_service.authenticate_access_token(token, db)
            logger.debug("Authenticated user: %s", user.email if user else None)

    if _graphql_auth_required() and user is None:
        raise HTTPException(status_code=401, detail="Authentication required")

    # --- Resolve org_id ------------------------------------------------------
    org_id = ""
    imp_ctx = get_impersonation_context()
    if imp_ctx is not None and getattr(imp_ctx, "is_active", False):
        org_id = imp_ctx.target_org_id or ""
    if not org_id:
        org_id = get_current_org_id() or ""
    if not org_id and user and user.org_id:
        org_id = user.org_id
    if not org_id:
        org_id = request.query_params.get("org_id", "") or ""

    # --- Resolve envelope inputs (CHAOS-4697 prerequisite) --------------------
    # tier and licensed_features are the two inputs
    # `issue_effective_principal_envelope` needs that nothing at this seam
    # produces today. Nothing reads them yet -- no dispatcher exists (that is
    # CHAOS-4697 itself) -- so a failure here MUST stay best-effort and never
    # break an otherwise-healthy GraphQL request. Distinguishing "resolved
    # empty" from "not resolved" is still preserved: both fields stay None on
    # any failure, they are never coerced to `[]`/community.
    tier: LicenseTier | None = None
    licensed_features: list[str] | None = None
    if user is not None and org_id:
        try:
            org_uuid = uuid.UUID(org_id)
        except ValueError:
            org_uuid = None
        if org_uuid is not None:
            try:
                from dev_health_ops.api.services.licensing import (
                    resolve_licensed_features_async,
                    resolve_org_tier_async,
                )

                async with get_postgres_session() as envelope_db:
                    tier = await resolve_org_tier_async(envelope_db, org_uuid)
                    licensed_features = await resolve_licensed_features_async(
                        envelope_db, org_uuid
                    )
            except Exception as e:
                logger.warning(
                    "Failed to resolve tier/licensed_features for org_id=%s: %s",
                    org_id,
                    e,
                )
                tier = None
                licensed_features = None

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
        tier=tier,
        licensed_features=licensed_features,
    )
    return context


def create_graphql_app(
    db_url: str | None = None,
) -> GoApiDispatchRouter[GraphQLContext, None]:
    """
    Create the GraphQL router for the analytics API.

    Args:
        db_url: Optional database URL override.

    Returns:
        A :class:`GoApiDispatchRouter` (CHAOS-4697) mounted in FastAPI --
        a :class:`~strawberry.fastapi.GraphQLRouter` subclass that
        dispatches Go-eligible, Go-enabled operations to query-api before
        falling back to strawberry's own execution. Unconfigured
        (``GO_API_QUERY_API_URL``/``GO_API_SCHEMA_DIGEST`` unset), it
        behaves identically to the plain ``GraphQLRouter`` it replaces.
    """

    async def context_getter(request: Request) -> GraphQLContext:
        """Context getter with optional db_url override."""
        context = await get_context(request)
        if db_url:
            context.db_url = db_url
        return context

    router = GoApiDispatchRouter[GraphQLContext, None](
        schema=schema,
        context_getter=context_getter,
        path="",
        graphql_ide="graphiql" if is_graphql_ide_enabled() else None,
    )

    return router


def get_graphql_info() -> dict:
    """Get information about the GraphQL API."""
    return {
        "schema_version": get_schema_version(),
        "endpoints": {
            "graphql": "/graphql",
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
