"""Middleware registration for the FastAPI app.

Extracted from ``api.main`` so that ``main.py`` remains composition-only.
``register_middleware`` adds every middleware in the same order as the
original inline registration.
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import TYPE_CHECKING

from fastapi.middleware.cors import CORSMiddleware
from slowapi.middleware import SlowAPIMiddleware

from dev_health_ops.api.middleware import OrgIdMiddleware
from dev_health_ops.api.middleware.correlation_id import CorrelationIdMiddleware
from dev_health_ops.api.middleware.impersonation import ImpersonationMiddleware
from dev_health_ops.api.middleware.security_headers import SecurityHeadersMiddleware

from .graphql.security import GraphQLQuerySizeLimitMiddleware

if TYPE_CHECKING:
    from fastapi import FastAPI

_DEFAULT_CORS_ORIGINS = "http://localhost:3000"
_CORS_PROTECTED_PATHS: frozenset[str] = frozenset({"/api/v1/auth/register"})


def _parse_cors_origins() -> list[str]:
    """Return the list of CORS-allowed origins from ``CORS_ALLOWED_ORIGINS``.

    The env var is comma-separated. Empty entries are ignored. If the variable
    is unset, the local-development default (``http://localhost:3000``) is used,
    matching the legacy inline behavior in ``api.main``.
    """
    raw = os.getenv("CORS_ALLOWED_ORIGINS", _DEFAULT_CORS_ORIGINS)
    return [o.strip() for o in raw.split(",") if o.strip()]


def register_middleware(app: FastAPI) -> None:
    """Register every middleware required by the API on ``app``.

    Order matches the original inline registration. Starlette applies
    middleware in reverse registration order, so the *last* call wraps the
    request first; do not reorder casually.
    """
    cors_origins = _parse_cors_origins()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Org-Id", "X-Request-ID"],
        expose_headers=["X-Request-ID"],
    )
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(GraphQLQuerySizeLimitMiddleware)

    # CSRF / Origin validation. The dynamic ``import_module`` mirrors the
    # original code, which avoids a hard import cycle at module load.
    OriginValidationMiddleware = import_module(
        "dev_health_ops.api.middleware.csrf"
    ).OriginValidationMiddleware
    app.add_middleware(
        OriginValidationMiddleware,
        allowed_origins=cors_origins,
        protected_paths=set(_CORS_PROTECTED_PATHS),
    )

    app.add_middleware(SlowAPIMiddleware)
    # ImpersonationMiddleware is registered BEFORE OrgIdMiddleware so it ends up
    # INNER on the request path (Starlette wraps last-added as outermost). On the
    # request path OrgIdMiddleware runs first and sets _current_org_id from the
    # X-Org-Id header / JWT, then ImpersonationMiddleware runs and overrides it to
    # the impersonated target org. The inner middleware gets the FINAL write to the
    # contextvar before the endpoint executes, so impersonation wins (CHAOS-2303).
    # Do NOT swap these two lines.
    app.add_middleware(ImpersonationMiddleware)
    app.add_middleware(OrgIdMiddleware)
    app.add_middleware(CorrelationIdMiddleware)


__all__ = [
    "_parse_cors_origins",
    "register_middleware",
]
