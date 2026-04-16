"""Per-request org_id extraction middleware.

Sets the org_id contextvar for every HTTP request from:
  1. X-Org-Id header (authoritative - sent by frontend for all API calls)
  2. JWT org_id claim (fallback - when header is absent)

IDOR protection: the X-Org-Id header is ONLY accepted if the authenticated
user has a Membership row for that org (or the JWT org_id matches). Any
other value yields HTTP 403.

This is the SINGLE enforcement point for tenant scoping. All downstream
ClickHouse queries auto-inject org_id via query_dicts().
"""

from __future__ import annotations

import json
import logging
import uuid as uuid_mod
from collections.abc import Iterable

from sqlalchemy import select
from starlette.types import ASGIApp, Receive, Scope, Send

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    _current_org_id,
    extract_token_from_header,
    get_auth_service,
    set_current_org_id,
)

logger = logging.getLogger(__name__)


def get_authenticated_user_from_headers(
    headers: Iterable[tuple[bytes, bytes]],
) -> AuthenticatedUser | None:
    for key, value in headers:
        if key == b"authorization":
            token = extract_token_from_header(value.decode("latin-1"))
            if not token:
                return None
            return get_auth_service().get_authenticated_user(token)
    return None


async def user_is_member_of_org(user_id: str, org_id: str) -> bool:
    """Return True iff the user has an active Membership for org_id."""
    try:
        user_uuid = uuid_mod.UUID(user_id)
        org_uuid = uuid_mod.UUID(org_id)
    except (ValueError, TypeError):
        return False

    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.models.users import Membership

    async with get_postgres_session() as session:
        result = await session.execute(
            select(Membership.id)
            .where(Membership.user_id == user_uuid)
            .where(Membership.org_id == org_uuid)
            .limit(1)
        )
        return result.scalar_one_or_none() is not None


class OrgIdMiddleware:
    """Pure ASGI middleware — extracts org_id, verifies membership, sets contextvar."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        header_org_id: str | None = None
        for key, value in headers:
            if key == b"x-org-id":
                header_org_id = value.decode("latin-1").strip() or None
                break

        user = get_authenticated_user_from_headers(headers)

        resolved_org_id: str | None = None
        if header_org_id and user is not None:
            # IDOR check: authenticated user's X-Org-Id must match their JWT
            # org_id or an existing Membership. Mismatch → 403. Superusers are
            # permitted to scope to any org (intentional — admin API contract).
            if user.is_superuser:
                resolved_org_id = header_org_id
            elif header_org_id == user.org_id:
                resolved_org_id = header_org_id
            elif await user_is_member_of_org(user.user_id, header_org_id):
                resolved_org_id = header_org_id
            else:
                logger.warning(
                    "X-Org-Id rejected: user=%s tried to access org=%s",
                    user.user_id,
                    header_org_id,
                )
                await self._deny(send, "X-Org-Id not permitted for this user")
                return
        elif user is not None and user.org_id:
            resolved_org_id = user.org_id
        # Anonymous requests with an X-Org-Id header: pass through. The header
        # is not a security claim without auth — downstream endpoints that
        # require auth will 401 via their own dependencies.

        token = set_current_org_id(resolved_org_id) if resolved_org_id else None
        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                _current_org_id.reset(token)

    @staticmethod
    async def _deny(send: Send, message: str) -> None:
        body = json.dumps({"detail": message}).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 403,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


__all__ = [
    "OrgIdMiddleware",
    "get_authenticated_user_from_headers",
    "user_is_member_of_org",
]
