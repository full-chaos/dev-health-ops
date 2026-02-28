"""Correlation ID middleware for request tracing.

Reads X-Request-ID from incoming requests (or generates a UUID if absent),
attaches it to the response, and exposes it via a context variable so that
log records emitted during the request can include the correlation ID.

Usage in log records:
    from dev_health_ops.api.middleware.correlation_id import get_request_id
    logger.info("handling request", extra={"request_id": get_request_id()})
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

_REQUEST_ID_VAR: ContextVar[str] = ContextVar("request_id", default="")

HEADER_NAME = "X-Request-ID"


def get_request_id() -> str:
    """Return the correlation ID for the current request context."""
    return _REQUEST_ID_VAR.get()


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware that ensures every request has a unique correlation ID.

    - Reads X-Request-ID from the request header if present.
    - Generates a UUID4 if the header is absent.
    - Stores the ID in a ContextVar for downstream use in log records.
    - Echoes the ID back in the X-Request-ID response header.
    """

    def __init__(self, app: ASGIApp, header_name: str = HEADER_NAME) -> None:
        super().__init__(app)
        self.header_name = header_name

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = (
            request.headers.get(self.header_name)
            or str(uuid.uuid4())
        )
        token = _REQUEST_ID_VAR.set(request_id)
        try:
            response: Response = await call_next(request)
        finally:
            _REQUEST_ID_VAR.reset(token)

        response.headers[self.header_name] = request_id
        return response
