"""Per-request org_id extraction middleware.

Sets the org_id contextvar for every HTTP request from:
  1. X-Org-Id header (authoritative - sent by frontend for all API calls)
  2. JWT org_id claim (fallback - when header is absent)

This is the SINGLE enforcement point for tenant scoping. All downstream
ClickHouse queries auto-inject org_id via query_dicts().
"""

from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send


class OrgIdMiddleware:
    """Pure ASGI middleware - extracts org_id and sets request-scoped contextvar."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        from dev_health_ops.api.services.auth import _current_org_id, set_current_org_id

        org_id = self._extract_org_id(scope)
        token = set_current_org_id(org_id) if org_id else None

        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                _current_org_id.reset(token)

    @staticmethod
    def _extract_org_id(scope: Scope) -> str | None:
        """Extract org_id from ASGI headers: X-Org-Id first, then JWT fallback."""
        from dev_health_ops.api.services.auth import (
            extract_token_from_header,
            get_auth_service,
        )

        org_id: str | None = None
        auth_value: str | None = None

        for key, value in scope.get("headers", []):
            if key == b"x-org-id":
                org_id = value.decode("latin-1")
            elif key == b"authorization":
                auth_value = value.decode("latin-1")

        if org_id:
            return org_id

        if auth_value:
            token_str = extract_token_from_header(auth_value)
            if token_str:
                user = get_auth_service().get_authenticated_user(token_str)
                if user and user.org_id:
                    return user.org_id

        return None


__all__ = ["OrgIdMiddleware"]
