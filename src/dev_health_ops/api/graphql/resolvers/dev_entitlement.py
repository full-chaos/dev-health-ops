"""Shared server-side Ask Dev entitlement boundary for GraphQL surfaces."""

from __future__ import annotations

import uuid

from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing.feature_decisions import is_org_feature_enabled_async
from dev_health_ops.licensing.registry import ASK_DEV_FEATURE

from ..errors import AuthorizationError


async def require_ask_dev_entitlement(org_id: str) -> None:
    """Require the canonical explicit-enable feature decision; fail closed."""
    try:
        canonical_org_id = uuid.UUID(org_id)
    except ValueError as exc:
        raise AuthorizationError("Ask Dev entitlement required") from exc
    try:
        async with get_postgres_session() as session:
            allowed = await is_org_feature_enabled_async(
                session, canonical_org_id, ASK_DEV_FEATURE
            )
    except Exception as exc:
        raise AuthorizationError("Ask Dev entitlement required") from exc
    if not allowed:
        raise AuthorizationError("Ask Dev entitlement required")


__all__ = ["require_ask_dev_entitlement"]
