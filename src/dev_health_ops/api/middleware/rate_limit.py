from __future__ import annotations

import json
import logging
import os
import sys

from fastapi import Request

# No-op limiter for test/dev environments to avoid decorator signature issues.
try:
    from slowapi import Limiter  # type: ignore
    from slowapi.util import get_remote_address  # noqa: F401
except Exception:
    Limiter = None  # type: ignore

    def get_remote_address(request: Request) -> str:  # type: ignore[misc]
        return "unknown"


from dev_health_ops.api.services.auth import extract_token_from_header, get_auth_service

AUTH_LOGIN_LIMIT = "5/15minutes"
AUTH_LOGIN_IP_LIMIT = "20/15minutes"
AUTH_REGISTER_LIMIT = "3/hour"
AUTH_REFRESH_LIMIT = "10/15minutes"
AUTH_VALIDATE_LIMIT = "30/15minutes"
ADMIN_PASSWORD_LIMIT = "5/hour"


def _normalize_email(value: str | None) -> str:
    if not value:
        return "unknown"
    return value.strip().lower() or "unknown"


def _extract_login_email(request: Request) -> str:
    body = getattr(request, "_body", None)
    if isinstance(body, (bytes, bytearray)) and body:
        try:
            payload = json.loads(body)
            if isinstance(payload, dict):
                email_value = payload.get("email")
                if isinstance(email_value, str):
                    return _normalize_email(email_value)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
            # Malformed body is expected for non-JSON or non-login requests;
            # fall through to query param / "unknown" fallback for rate-limit keying.
            logging.getLogger(__name__).debug(
                "Could not parse request body for rate-limit email key"
            )

    email_from_query = request.query_params.get("email")
    if email_from_query:
        return _normalize_email(email_from_query)

    return "unknown"


def get_auth_key(request: Request) -> str:
    ip = get_remote_address(request) or "unknown"
    email = _extract_login_email(request)
    return f"{ip}:{email}"


def _trusted_proxies() -> frozenset[str]:
    """Return the configured set of trusted proxy IPs (fail-closed: empty if unset)."""
    raw = os.getenv("TRUSTED_PROXIES", "")
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


def get_forwarded_ip(request: Request) -> str:
    """Return real client IP via X-Forwarded-For, honoured only if the TCP peer
    is in the TRUSTED_PROXIES allowlist.

    Behind a reverse proxy (Next.js rewrite, nginx, etc.) the TCP peer is the
    proxy, not the end-user. X-Forwarded-For carries the original IP — but it
    is attacker-controlled when sent directly to the API, so we only trust it
    when the peer address is an expected proxy.
    """
    peer = (request.client.host if request.client else None) or "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded and peer in _trusted_proxies():
        return forwarded.split(",")[0].strip()
    return peer


def get_admin_user_key(request: Request) -> str:
    ip = get_remote_address(request) or "unknown"
    auth_header = request.headers.get("authorization")
    if auth_header:
        token = extract_token_from_header(auth_header)
        if token:
            user = get_auth_service().get_authenticated_user(token)
            if user and user.user_id:
                return f"admin:{user.user_id}"
    return f"admin-ip:{ip}"


_REDIS_URL = os.getenv("REDIS_URL")
_IS_PYTEST = "pytest" in sys.modules


class _NoOpLimiter:
    """Pass-through limiter when slowapi is unavailable (tests, minimal installs)."""

    def limit(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        def _decorator(func):  # type: ignore[no-untyped-def]
            return func

        return _decorator


if Limiter is not None:
    storage_uri = _REDIS_URL if _REDIS_URL and not _IS_PYTEST else "memory://"
    limiter = Limiter(
        key_func=get_remote_address,
        storage_uri=storage_uri,
    )
    if _REDIS_URL and not _IS_PYTEST:
        logging.getLogger(__name__).info(
            "Rate limiter using Redis storage: %s", _REDIS_URL[:20] + "..."
        )
    elif _REDIS_URL and _IS_PYTEST:
        logging.getLogger(__name__).info(
            "Rate limiter using in-memory storage during pytest run"
        )
else:
    limiter = _NoOpLimiter()  # type: ignore[assignment]
