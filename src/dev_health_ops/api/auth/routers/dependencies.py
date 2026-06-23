from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Header, HTTPException
from sqlalchemy.exc import DBAPIError, OperationalError

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    extract_token_from_header,
)
from dev_health_ops.api.utils.errors import error_detail

logger = logging.getLogger(__name__)


def _db_temporarily_unavailable(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, OperationalError)):
        return True
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    original = getattr(exc, "orig", None)
    text = f"{type(exc).__name__} {exc} {type(original).__name__} {original}".lower()
    return "timeout" in text and any(
        marker in text for marker in ("connect", "connection", "asyncpg", "ssl")
    )


def _database_unavailable() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=error_detail("Database temporarily unavailable"),
    )


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
    try:
        async with get_postgres_session() as db:
            user = await auth_service.authenticate_access_token(token, db)
    except Exception as exc:
        if _db_temporarily_unavailable(exc):
            logger.warning("Database unavailable during access-token authentication")
            raise _database_unavailable() from exc
        raise

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
    try:
        async with get_postgres_session() as db:
            return await auth_service.authenticate_access_token(token, db)
    except Exception as exc:
        if _db_temporarily_unavailable(exc):
            logger.warning("Database unavailable during optional token authentication")
            raise _database_unavailable() from exc
        raise


__all__ = ["get_current_user", "get_current_user_optional"]
