"""Exception handlers for the FastAPI app.

Extracted from ``api.main`` so that ``main.py`` remains composition-only.
Handlers and the ``register_exception_handlers`` helper preserve the exact
behavior of the original inline registration.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.exceptions import RequestValidationError
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


def _validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Return a structured 422 response for FastAPI request-validation errors.

    The parameter is typed as ``Exception`` to satisfy Starlette's
    ``ExceptionHandler`` signature; the runtime ``isinstance`` check is purely
    defensive because this handler is registered specifically for
    ``RequestValidationError``.
    """
    if not isinstance(exc, RequestValidationError):
        # Defensive: should never happen since FastAPI only routes
        # RequestValidationError instances here.
        raise exc
    errors = [str(error.get("msg", "Invalid value")) for error in exc.errors()]
    return JSONResponse(
        status_code=422,
        content={
            "detail": {
                "message": "Validation failed",
                "errors": errors,
            }
        },
    )


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
    ``RateLimitExceeded`` → ``RequestValidationError`` → ``Exception``.
    """
    app.add_exception_handler(RateLimitExceeded, _rate_limit_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
    app.add_exception_handler(Exception, _generic_exception_handler)


__all__ = [
    "_generic_exception_handler",
    "_rate_limit_handler",
    "_validation_error_handler",
    "register_exception_handlers",
]
