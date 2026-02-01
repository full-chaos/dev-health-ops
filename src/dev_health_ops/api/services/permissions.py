"""RBAC permission checking service with caching."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING

from dev_health_ops.models.users import (
    MemberRole,
    ROLE_PERMISSIONS,
    STANDARD_PERMISSIONS,
)

if TYPE_CHECKING:
    from dev_health_ops.api.services.auth import AuthenticatedUser

logger = logging.getLogger(__name__)

ROLE_HIERARCHY = [
    MemberRole.VIEWER,
    MemberRole.MEMBER,
    MemberRole.ADMIN,
    MemberRole.OWNER,
]


@lru_cache(maxsize=128)
def _get_role_permissions(role: str) -> frozenset[str]:
    """Get all permissions for a role including inherited permissions."""
    try:
        role_enum = MemberRole(role)
    except ValueError:
        logger.warning("Unknown role: %s", role)
        return frozenset()

    role_index = ROLE_HIERARCHY.index(role_enum)
    permissions: set[str] = set()

    for i, r in enumerate(ROLE_HIERARCHY):
        if i <= role_index:
            permissions.update(ROLE_PERMISSIONS.get(r, []))

    return frozenset(permissions)


@lru_cache(maxsize=1)
def _get_all_permission_names() -> frozenset[str]:
    """Get all defined permission names."""
    return frozenset(p[0] for p in STANDARD_PERMISSIONS)


def has_permission(user: "AuthenticatedUser", permission: str) -> bool:
    """Check if user has the specified permission.

    Args:
        user: Authenticated user from JWT
        permission: Permission name (e.g., "metrics:read")

    Returns:
        True if user has permission, False otherwise
    """
    if user.is_superuser:
        return True

    if permission not in _get_all_permission_names():
        logger.warning("Unknown permission requested: %s", permission)
        return False

    role_perms = _get_role_permissions(user.role)
    return permission in role_perms


def has_any_permission(user: "AuthenticatedUser", *permissions: str) -> bool:
    """Check if user has any of the specified permissions."""
    return any(has_permission(user, p) for p in permissions)


def has_all_permissions(user: "AuthenticatedUser", *permissions: str) -> bool:
    """Check if user has all of the specified permissions."""
    return all(has_permission(user, p) for p in permissions)


def get_user_permissions(user: "AuthenticatedUser") -> set[str]:
    """Get all permissions for a user."""
    if user.is_superuser:
        return set(_get_all_permission_names())
    return set(_get_role_permissions(user.role))


def clear_permission_cache() -> None:
    """Clear the permission cache. Call after role/permission changes."""
    _get_role_permissions.cache_clear()
    _get_all_permission_names.cache_clear()
