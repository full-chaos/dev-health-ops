"""Security-headers middleware.

Injects a conservative set of response headers on every HTTP response:

- Strict-Transport-Security: HSTS with 1y max-age and subdomain coverage
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY
- Referrer-Policy: strict-origin-when-cross-origin
- Content-Security-Policy: lock down by default (API returns JSON)

Pre-existing headers set by downstream handlers are preserved (case-insensitive
match); the middleware only adds what's missing.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_DEFAULT_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"strict-transport-security", b"max-age=31536000; includeSubDomains"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"strict-origin-when-cross-origin"),
    (
        b"content-security-policy",
        b"default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
    ),
)


class SecurityHeadersMiddleware:
    """Pure ASGI middleware that adds security headers to every response."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def _send(message: Message) -> None:
            if message["type"] == "http.response.start":
                existing = {k.lower() for k, _ in message.get("headers", [])}
                headers = list(message.get("headers", []))
                for name, value in _DEFAULT_HEADERS:
                    if name not in existing:
                        headers.append((name, value))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, _send)


__all__ = ["SecurityHeadersMiddleware"]
