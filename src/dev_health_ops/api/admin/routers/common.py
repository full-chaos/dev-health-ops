from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep as get_session
from dev_health_ops.api.middleware.rate_limit import (
    ADMIN_PASSWORD_LIMIT,
    get_admin_user_key,
    limiter,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.users import MembershipService
from dev_health_ops.models.users import Membership, User


def get_user_id(
    x_user_id: Annotated[str | None, Header(alias="X-User-Id")] = None,
) -> str | None:
    return x_user_id


async def _ensure_user_in_scope(
    session: AsyncSession,
    user: User,
    org_id: str,
    current_user: AuthenticatedUser,
) -> None:
    if current_user.is_superuser:
        return
    membership_result = await session.execute(
        select(Membership.id).where(
            Membership.org_id == uuid.UUID(org_id),
            Membership.user_id == user.id,
        )
    )
    if membership_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="User not found")


async def _ensure_org_admin_access(
    session: AsyncSession,
    org_id: str,
    current_user: AuthenticatedUser,
) -> None:
    if current_user.is_superuser:
        return
    membership_svc = MembershipService(session)
    membership = await membership_svc.get_membership(org_id, current_user.user_id)
    if membership is None or membership.role not in {"owner", "admin"}:
        raise HTTPException(
            status_code=403, detail="Admin access required for organization"
        )


def _get_org_id_for_non_superuser(current_user: AuthenticatedUser) -> str:
    if current_user.is_superuser:
        return current_user.org_id or ""
    if not current_user.org_id:
        raise HTTPException(status_code=403, detail="Organization context required")
    return current_user.org_id


__all__ = [
    "ADMIN_PASSWORD_LIMIT",
    "_ensure_org_admin_access",
    "_ensure_user_in_scope",
    "_get_org_id_for_non_superuser",
    "get_admin_user_key",
    "get_session",
    "get_user_id",
    "limiter",
]
