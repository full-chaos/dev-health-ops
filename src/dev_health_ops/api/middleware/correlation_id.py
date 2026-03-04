"""Correlation ID middleware for request tracing.

Reads X-Request-ID from incoming requests (or generates a UUID if absent),
attaches it to the response, and exposes it via a context variable so that
log records emitted during the request can include the correlation ID.

Implementation: pure ASGI middleware (no BaseHTTPMiddleware overhead).

Usage in log records:
    from dev_health_ops.api.middleware.correlation_id import get_request_id
    logger.info("handling request", extra={"request_id": get_request_id()})
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_REQUEST_ID_VAR: ContextVar[str] = ContextVar("request_id", default="")

HEADER_NAME = "X-Request-ID"
_HEADER_NAME_BYTES = HEADER_NAME.lower().encode("latin-1")


def get_request_id() -> str:
    """Return the correlation ID for the current request context."""
    return _REQUEST_ID_VAR.get()


class CorrelationIdMiddleware:
    """Pure ASGI middleware that ensures every request has a unique correlation ID.

    - Reads X-Request-ID from the request header if present.
    - Generates a UUID4 if the header is absent.
    - Stores the ID in a ContextVar for downstream use in log records.
    - Echoes the ID back in the X-Request-ID response header.
    """

    def __init__(self, app: ASGIApp, header_name: str = HEADER_NAME) -> None:
        self.app = app
        self.header_name = header_name
        self._header_key = header_name.lower().encode("latin-1")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        # Extract or generate request ID from headers.
        request_id: str | None = None
        for key, value in scope.get("headers", []):
            if key == self._header_key:
                request_id = value.decode("latin-1")
                break
        if not request_id:
            request_id = str(uuid.uuid4())

        token = _REQUEST_ID_VAR.set(request_id)

        # Wrap send to inject X-Request-ID into response headers.
        header_name_bytes = self._header_key
        request_id_bytes = request_id.encode("latin-1")

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((header_name_bytes, request_id_bytes))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            _REQUEST_ID_VAR.reset(token)
