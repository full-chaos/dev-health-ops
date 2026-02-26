from __future__ import annotations

from urllib.parse import urlparse
from typing import Iterable

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class OriginValidationMiddleware:
    _STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

    def __init__(
        self,
        app: ASGIApp,
        *,
        allowed_origins: Iterable[str],
        protected_paths: Iterable[str] | None = None,
    ) -> None:
        self.app = app
        self.allowed_origins = {
            origin
            for origin in (self._normalize_origin(value) for value in allowed_origins)
            if origin
        }
        self.allow_all_origins = "*" in self.allowed_origins
        self.protected_paths = set(protected_paths or {"/api/v1/auth/register"})

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        method = str(scope.get("method", "")).upper()
        path = str(scope.get("path", ""))
        if (
            method not in self._STATE_CHANGING_METHODS
            or path not in self.protected_paths
            or self.allow_all_origins
        ):
            await self.app(scope, receive, send)
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        origin = self._normalize_origin(headers.get("origin"))
        if origin and origin in self.allowed_origins:
            await self.app(scope, receive, send)
            return

        referer = self._normalize_origin(headers.get("referer"))
        if referer and referer in self.allowed_origins:
            await self.app(scope, receive, send)
            return

        response = JSONResponse(
            status_code=403,
            content={"detail": "Request origin validation failed"},
        )
        await response(scope, receive, send)

    @staticmethod
    def _normalize_origin(value: str | None) -> str | None:
        if not value:
            return None
        parsed = urlparse(value.strip())
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
        return None
