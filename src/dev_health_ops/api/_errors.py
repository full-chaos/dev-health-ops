"""Exception handlers for the FastAPI app.

Extracted from ``api.main`` so that ``main.py`` remains composition-only.
Handlers and the ``register_exception_handlers`` helper preserve the exact
behavior of the original inline registration.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

from .external_ingest.errors import (
    EXTERNAL_INGEST_PATH_PREFIX,
    external_ingest_error_body,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


def _rate_limit_handler(request: Request, exc: Exception) -> JSONResponse:
    """Return a structured 429 response when slowapi raises ``RateLimitExceeded``.

    Any other exception type is re-raised so the framework can route it to the
    appropriate handler (this preserves the original semantics of the inline
    handler).

    external-ingest routes get their own error envelope (master-spec CC16)
    instead of the app-wide ``{"detail": ...}`` shape — branching on path
    here avoids registering a second ``RateLimitExceeded`` handler (Starlette
    only dispatches one per exception type) and avoids a second ``Limiter``
    instance (master-spec CC15 requires reusing the shared singleton).
    """
    if isinstance(exc, RateLimitExceeded):
        if request.url.path.startswith(EXTERNAL_INGEST_PATH_PREFIX):
            return JSONResponse(
                status_code=429,
                content=external_ingest_error_body(
                    "rate_limited", "Rate limit exceeded. Please try again later."
                ),
            )
        return JSONResponse(
            status_code=429,
            content={
                "detail": {
                    "message": "Rate limit exceeded. Please try again later.",
                }
            },
        )
    raise exc


async def _generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all 500 handler that returns a sanitized response.

    Logs the real exception with stack trace at ERROR level so operators can
    investigate via logs/Sentry, but never leaks internals to the client.

    external-ingest routes get the customer-facing envelope here too
    (adversarial-review finding): the documented contract is one stable
    ``{"error": {...}}`` shape for every ``/api/v1/external-ingest/*``
    response, including a genuinely unexpected 500 — not just the errors
    ``ExternalIngestError`` raises deliberately. The message stays the same
    generic, sanitized text; only the envelope shape changes.
    """
    logger.error(
        "Unhandled exception on %s %s",
        request.method,
        request.url.path,
        exc_info=exc,
    )
    if request.url.path.startswith(EXTERNAL_INGEST_PATH_PREFIX):
        return JSONResponse(
            status_code=500,
            content=external_ingest_error_body(
                "internal_error", "Internal Server Error"
            ),
        )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Register all exception handlers on the given FastAPI app.

    Mirrors the original inline registration order from ``api.main``:
    ``RateLimitExceeded`` → ``Exception``. ``RequestValidationError`` is
    registered separately, in ``api.main`` itself, after this call (see that
    module's own handler for why -- CHAOS-7015/D2802).
    """
    app.add_exception_handler(RateLimitExceeded, _rate_limit_handler)
    app.add_exception_handler(Exception, _generic_exception_handler)


__all__ = [
    "_generic_exception_handler",
    "_rate_limit_handler",
    "register_exception_handlers",
]
