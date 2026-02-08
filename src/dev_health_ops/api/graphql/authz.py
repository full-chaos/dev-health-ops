"""Authorization utilities for GraphQL analytics API."""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict, TypeVar

from .context import GraphQLContext
from .errors import AuthorizationError

F = TypeVar("F", bound=Callable[..., Any])


def require_permission(
    *permissions: str, require_all: bool = False
) -> Callable[[F], F]:
    """Decorator to require permissions for GraphQL resolvers.

    Args:
        *permissions: Permission names required (e.g., "metrics:read")
        require_all: If True, user must have ALL permissions. If False, ANY suffices.

    Usage:
        @strawberry.field
        @require_permission("metrics:read")
        async def metrics(self, info: Info) -> list[Metric]:
            ...

        @strawberry.field
        @require_permission("settings:read", "settings:write", require_all=True)
        async def update_setting(self, info: Info) -> Setting:
            ...
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            info = kwargs.get("info") or (args[1] if len(args) > 1 else None)
            if info is None:
                raise AuthorizationError("Could not extract GraphQL info from resolver")

            context: GraphQLContext = info.context
            user = context.user

            if user is None:
                raise AuthorizationError("Authentication required")

            from dev_health_ops.api.services.permissions import (
                has_all_permissions,
                has_any_permission,
            )

            if require_all:
                if not has_all_permissions(user, *permissions):
                    raise AuthorizationError(
                        f"Missing required permissions: {', '.join(permissions)}"
                    )
            else:
                if not has_any_permission(user, *permissions):
                    raise AuthorizationError(
                        f"Requires one of: {', '.join(permissions)}"
                    )

            return await func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            info = kwargs.get("info") or (args[1] if len(args) > 1 else None)
            if info is None:
                raise AuthorizationError("Could not extract GraphQL info from resolver")

            context: GraphQLContext = info.context
            user = context.user

            if user is None:
                raise AuthorizationError("Authentication required")

            from dev_health_ops.api.services.permissions import (
                has_all_permissions,
                has_any_permission,
            )

            if require_all:
                if not has_all_permissions(user, *permissions):
                    raise AuthorizationError(
                        f"Missing required permissions: {', '.join(permissions)}"
                    )
            else:
                if not has_any_permission(user, *permissions):
                    raise AuthorizationError(
                        f"Requires one of: {', '.join(permissions)}"
                    )

            return func(*args, **kwargs)

        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        return sync_wrapper  # type: ignore

    return decorator


def require_auth(func: F) -> F:
    """Decorator to require authentication without specific permissions."""

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        info = kwargs.get("info") or (args[1] if len(args) > 1 else None)
        if info is None:
            raise AuthorizationError("Could not extract GraphQL info from resolver")

        context: GraphQLContext = info.context
        if context.user is None:
            raise AuthorizationError("Authentication required")

        return await func(*args, **kwargs)

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        info = kwargs.get("info") or (args[1] if len(args) > 1 else None)
        if info is None:
            raise AuthorizationError("Could not extract GraphQL info from resolver")

        context: GraphQLContext = info.context
        if context.user is None:
            raise AuthorizationError("Authentication required")

        return func(*args, **kwargs)

    import asyncio

    if asyncio.iscoroutinefunction(func):
        return async_wrapper  # type: ignore
    return sync_wrapper  # type: ignore


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


def require_feature(*features: str, require_all: bool = False) -> Callable[[F], F]:
    """Decorator to require feature access for GraphQL resolvers.

    Delegates to the JWT-backed has_feature() from the licensing gating module.

    Args:
        *features: Feature keys required (e.g., "capacity_forecast", "sso_saml")
        require_all: If True, must have ALL features. If False, ANY suffices.
    """
    from dev_health_ops.licensing.gating import has_feature

    def _check_features() -> None:
        if require_all:
            for feature_key in features:
                if not has_feature(feature_key, log_denial=True):
                    raise AuthorizationError(f"Feature '{feature_key}' not available")
        else:
            for feature_key in features:
                if has_feature(feature_key, log_denial=False):
                    return
            raise AuthorizationError(f"Requires one of: {', '.join(features)}")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            info = kwargs.get("info") or (args[1] if len(args) > 1 else None)
            if info is None:
                raise AuthorizationError("Could not extract GraphQL info from resolver")

            context: GraphQLContext = info.context

            if context.user is None:
                raise AuthorizationError("Authentication required")

            _check_features()
            return await func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            info = kwargs.get("info") or (args[1] if len(args) > 1 else None)
            if info is None:
                raise AuthorizationError("Could not extract GraphQL info from resolver")

            context: GraphQLContext = info.context

            if context.user is None:
                raise AuthorizationError("Authentication required")

            _check_features()
            return func(*args, **kwargs)

        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        return sync_wrapper  # type: ignore

    return decorator
