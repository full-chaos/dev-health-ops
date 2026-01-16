"""Authorization utilities for GraphQL analytics API."""

from __future__ import annotations

from typing import Any, Dict

from .context import GraphQLContext
from .errors import AuthorizationError


def require_org_id(context: GraphQLContext) -> str:
    """
    Validate that org_id is present in the context.

    Args:
        context: GraphQL request context.

    Returns:
        The validated org_id.

    Raises:
        AuthorizationError: If org_id is missing or empty.
    """
    if not context.org_id:
        raise AuthorizationError("org_id is required for all analytics queries")
    return context.org_id


def enforce_org_scope(org_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inject org_id into SQL parameters to enforce org scoping.

    This function ensures that org_id is always included in query parameters,
    which is then used in WHERE clauses to scope data access.

    Args:
        org_id: The organization ID to scope queries to.
        params: Existing SQL parameters dict.

    Returns:
        Updated params dict with org_id included.

    Raises:
        AuthorizationError: If org_id is missing or empty.
    """
    if not org_id:
        raise AuthorizationError("org_id is required for org scoping")

    # Create a new dict to avoid mutating the original
    scoped_params = dict(params)
    scoped_params["org_id"] = org_id
    return scoped_params


def validate_org_access(context: GraphQLContext, requested_org_id: str) -> None:
    """
    Validate that the context org_id matches the requested org_id.

    This prevents cross-org data access by ensuring the authenticated
    org matches the requested data scope.

    Args:
        context: GraphQL request context.
        requested_org_id: The org_id being requested in the query.

    Raises:
        AuthorizationError: If org_ids don't match.
    """
    if context.org_id != requested_org_id:
        raise AuthorizationError(
            f"Access denied: cannot query data for org '{requested_org_id}'"
        )
