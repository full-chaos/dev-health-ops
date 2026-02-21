from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, AsyncGenerator

from fastapi import Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session


@dataclass
class AdminContext:
    user: AuthenticatedUser
    org_id: str
    db: AsyncSession
    is_superuser: bool


async def require_admin(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    if not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")

    from dev_health_ops.api.services.auth import (
        extract_token_from_header,
        get_auth_service,
    )

    token = extract_token_from_header(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")


async def get_admin_context(
    authorization: Annotated[str | None, Header()] = None,
) -> AsyncGenerator[AdminContext, None]:
    from dev_health_ops.api.services.auth import (
        extract_token_from_header,
        get_auth_service,
    )

    token = extract_token_from_header(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if not user.org_id and not user.is_superuser:
        raise HTTPException(status_code=403, detail="No organization context")

    async with get_postgres_session() as db:
        yield AdminContext(
            user=user,
            org_id=user.org_id,
            db=db,
            is_superuser=user.is_superuser,
        )


async def require_superuser(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser:
    if not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")

    from dev_health_ops.api.services.auth import (
        extract_token_from_header,
        get_auth_service,
    )

    token = extract_token_from_header(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")
    return user
