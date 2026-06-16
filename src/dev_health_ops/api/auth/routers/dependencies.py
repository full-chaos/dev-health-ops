from __future__ import annotations

from typing import Annotated

from fastapi import Header, HTTPException

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    extract_token_from_header,
)
from dev_health_ops.api.utils.errors import error_detail


async def get_current_user(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    if not authorization:
        raise HTTPException(
            status_code=401,
            detail=error_detail("Not authenticated"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = extract_token_from_header(authorization)
    if not token:
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid authorization header"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    auth_service = get_auth_service()
    async with get_postgres_session() as db:
        user = await auth_service.authenticate_access_token(token, db)

    if not user:
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid or expired token"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def get_current_user_optional(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser | None:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    if not authorization:
        return None

    token = extract_token_from_header(authorization)
    if not token:
        return None

    auth_service = get_auth_service()
    async with get_postgres_session() as db:
        return await auth_service.authenticate_access_token(token, db)


__all__ = ["get_current_user", "get_current_user_optional"]
