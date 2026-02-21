from __future__ import annotations

from fastapi import Depends, HTTPException

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser


async def require_admin(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AuthenticatedUser:
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


async def require_superuser(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AuthenticatedUser:
    if not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")
    return current_user


async def get_admin_org_id(
    current_user: AuthenticatedUser = Depends(require_admin),
) -> str:
    if not current_user.org_id:
        raise HTTPException(status_code=403, detail="Organization context required")
    return current_user.org_id


async def get_admin_user(
    current_user: AuthenticatedUser = Depends(require_admin),
) -> AuthenticatedUser:
    return current_user
