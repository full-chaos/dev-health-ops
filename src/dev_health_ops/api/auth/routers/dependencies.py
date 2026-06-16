from __future__ import annotations

import logging
import uuid as uuid_mod
from typing import Annotated

from fastapi import Header, HTTPException
from sqlalchemy import select

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    extract_token_from_header,
)
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.models.users import User

logger = logging.getLogger(__name__)


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
    user = auth_service.get_authenticated_user(token)

    if not user:
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid or expired token"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user_uuid = uuid_mod.UUID(user.user_id)
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid token claims"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User.id, User.is_active, User.token_version).where(
                User.id == user_uuid
            )
        )
        db_user = result.one_or_none()

    if db_user is None:
        logger.warning("JWT valid but user not found in DB: %s", user.user_id)
        raise HTTPException(
            status_code=401,
            detail=error_detail("User no longer exists"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not db_user.is_active:
        logger.warning("JWT valid but user is deactivated: %s", user.user_id)
        raise HTTPException(
            status_code=401,
            detail=error_detail("Account is disabled"),
            headers={"WWW-Authenticate": "Bearer"},
        )

    if user.token_version != int(db_user.token_version or 0):
        logger.warning("JWT token version mismatch for user: %s", user.user_id)
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
    user = auth_service.get_authenticated_user(token)
    if not user:
        return None

    try:
        user_uuid = uuid_mod.UUID(user.user_id)
    except ValueError:
        return None

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User.id, User.is_active, User.token_version).where(
                User.id == user_uuid
            )
        )
        db_user = result.one_or_none()

    if not db_user or not db_user.is_active:
        return None
    if user.token_version != int(db_user.token_version or 0):
        return None

    return user


__all__ = ["get_current_user", "get_current_user_optional"]
