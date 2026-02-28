"""ImpersonationMiddleware - transparent impersonation context injection.

Checks every incoming request for an active impersonation session. If found,
overrides _current_org_id and _impersonation_ctx so all downstream code
(GraphQL, queries, audit) sees the impersonated user's context.

Middleware ordering in main.py:
  app.add_middleware(OrgIdMiddleware)         # inner - sets org_id from header/JWT
  app.add_middleware(ImpersonationMiddleware) # outer - overrides it when impersonating
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dev_health_ops.api.services.auth import (
    _current_org_id,
    _impersonation_ctx,
    extract_token_from_header,
    get_auth_service,
    set_current_org_id,
    set_impersonation_context,
)
from dev_health_ops.api.services.impersonation_cache import (
    get_active_session,
    invalidate,
)

logger = logging.getLogger(__name__)


class ImpersonationMiddleware:
    """ASGI middleware that injects impersonation context when a session exists."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        real_user = _extract_user(scope)
        if real_user is None or not getattr(real_user, "is_superuser", False):
            await self.app(scope, receive, send)
            return

        admin_user_id = str(real_user.user_id)

        try:
            session = await get_active_session(admin_user_id)
        except Exception:
            session = None

        if session is None:
            await self.app(scope, receive, send)
            return

        now = datetime.now(timezone.utc)
        expires_at = (
            session.expires_at.replace(tzinfo=timezone.utc)
            if session.expires_at.tzinfo is None
            else session.expires_at
        )
        if expires_at < now:
            try:
                await _expire_session(session, admin_user_id)
            except Exception:
                logger.debug("Failed to expire impersonation session", exc_info=True)
            await self.app(scope, receive, send)
            return

        org_token = set_current_org_id(str(session.target_org_id))
        imp_token = set_impersonation_context(
            target_user_id=str(session.target_user_id),
            target_org_id=str(session.target_org_id),
            target_role=session.target_role,
            real_user_id=admin_user_id,
        )
        wrapped_send = _make_header_send(send, str(session.target_user_id))

        try:
            await self.app(scope, receive, wrapped_send)
        finally:
            _current_org_id.reset(org_token)
            _impersonation_ctx.reset(imp_token)


def _extract_user(scope: Scope) -> Any | None:
    """Extract authenticated user from scope state or authorization header."""
    state = scope.get("state")
    if state is not None:
        user = getattr(state, "user", None)
        if user is not None:
            return user
        if isinstance(state, dict):
            user = state.get("user")
            if user is not None:
                return user

    auth_header = None
    for key, value in scope.get("headers", []):
        if key == b"authorization":
            auth_header = value.decode("latin-1")
            break

    token = extract_token_from_header(auth_header)
    if not token:
        return None
    return get_auth_service().get_authenticated_user(token)


async def _expire_session(session: Any, admin_user_id: str) -> None:
    """Mark a session as ended due to TTL expiry."""
    try:
        from dev_health_ops.db import get_postgres_session

        async with get_postgres_session() as db:
            session.ended_at = datetime.now(timezone.utc)
            db.add(session)
            await db.commit()
        invalidate(admin_user_id)
    except Exception as exc:
        logger.warning("Failed to expire impersonation session: %s", exc)


def _make_header_send(send: Send, target_user_id: str) -> Send:
    """Wrap ASGI send callable to append impersonation response headers."""

    async def wrapped_send(message: Message) -> None:
        if message["type"] == "http.response.start":
            headers = list(message.get("headers", []))
            headers.append((b"x-impersonating", b"true"))
            headers.append((b"x-impersonated-user-id", target_user_id.encode()))
            message = {**message, "headers": headers}
        await send(message)

    return wrapped_send
