"""Customer-facing error envelope for /api/v1/external-ingest/* (CHAOS-2691 D3).

External-ingest responses are consumed by customer SDKs/CI scripts, not just
the web app, so they need a stable, documented, machine-parseable error shape
distinct from the app-wide ``{"detail": ...}`` convention (``api/_errors.py``),
which is not a documented public contract.

Canonical ``code`` vocabulary (master-spec CC16) is a fixed, cross-ticket set;
this ticket raises a subset (see docs/architecture/adr-003-external-ingest-rest-boundary.md)
and reserves the rest for sibling tickets (2694/2695/2696/2712/2699).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from fastapi import FastAPI

#: Shared with api/_errors.py so the 429 handler can select this envelope
#: shape for external-ingest routes without a second Limiter/exception
#: handler registration (master-spec CC15).
EXTERNAL_INGEST_PATH_PREFIX = "/api/v1/external-ingest"


class ExternalIngestError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        errors: list[dict] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.errors = errors or []


def external_ingest_error_body(
    code: str, message: str, errors: list[dict] | None = None
) -> dict:
    body: dict = {"error": {"code": code, "message": message}}
    if errors:
        body["error"]["errors"] = errors
    return body


async def _external_ingest_error_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    if not isinstance(exc, ExternalIngestError):
        # Defensive: should never happen since this handler is registered
        # specifically for ExternalIngestError.
        raise exc
    return JSONResponse(
        status_code=exc.status_code,
        content=external_ingest_error_body(exc.code, exc.message, exc.errors),
    )


def register_external_ingest_error_handlers(app: FastAPI) -> None:
    """Register the ExternalIngestError handler.

    Starlette dispatches on exact exception type first, so this wins over
    the generic ``Exception`` catch-all registered by
    ``register_exception_handlers`` regardless of call order — safe to call
    this anywhere after that registration in ``main.py``.
    """
    app.add_exception_handler(ExternalIngestError, _external_ingest_error_handler)


__all__ = [
    "EXTERNAL_INGEST_PATH_PREFIX",
    "ExternalIngestError",
    "external_ingest_error_body",
    "register_external_ingest_error_handlers",
]
