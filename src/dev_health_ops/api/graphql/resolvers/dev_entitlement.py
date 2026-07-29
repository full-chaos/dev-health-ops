"""GraphQL adapter for the canonical Ask Dev entitlement boundary."""

from __future__ import annotations

from dev_health_ops.api.dev.entitlement import (
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from dev_health_ops.db import get_postgres_session

from ..errors import AuthorizationError


async def require_ask_dev_entitlement(org_id: str) -> None:
    """Translate canonical explicit-entitlement denial to GraphQL authz."""
    try:
        async with get_postgres_session() as session:
            await CanonicalAskDevEntitlementAuthorizer(session).require(org_id)
    except AskDevEntitlementDeniedError as exc:
        raise AuthorizationError("Ask Dev entitlement required") from exc
    except Exception as exc:
        # Session acquisition is outside the application authorizer but follows
        # the same fail-closed boundary when Postgres is unavailable.
        raise AuthorizationError("Ask Dev entitlement required") from exc


__all__ = ["require_ask_dev_entitlement"]
