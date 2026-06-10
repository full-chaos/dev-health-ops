from __future__ import annotations

import hashlib
import json
import logging
import os

from fastapi import Request

# No-op limiter for test/dev environments to avoid decorator signature issues.
try:
    from slowapi import Limiter
    from slowapi.util import get_remote_address  # noqa: F401
except Exception:
    Limiter = None  # type: ignore[misc,assignment]

    def get_remote_address(request: Request) -> str:
        return "unknown"


from dev_health_ops.api.services.auth import extract_token_from_header, get_auth_service

# AUTH_LOGIN_LIMIT ("5/15minutes") was removed — it counted successful logins and
# incorrectly triggered 429 for legitimate users. The DB-backed login_attempts
# lockout (login_attempts.py) is the correct primitive for failed-attempt limits.
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


def get_validate_key(request: Request) -> str:
    """Rate-limit key for POST /auth/validate: per-token, not per-IP.

    The web app calls /validate server-side, so every user shares the same
    TCP peer (the web container). Keying on IP collapses all users into one
    bucket and the limit becomes a deployment-wide throughput cap (CHAOS-2232).
    Key on a digest of the submitted token instead — never the raw token,
    which must not appear in limiter storage or error messages.
    """
    body = getattr(request, "_body", None)
    if isinstance(body, (bytes, bytearray)) and body:
        try:
            payload = json.loads(body)
            if isinstance(payload, dict):
                token = payload.get("token")
                if isinstance(token, str) and token:
                    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]
                    return f"validate-token:{digest}"
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
            logging.getLogger(__name__).debug(
                "Could not parse request body for validate rate-limit key"
            )
    return f"validate-ip:{get_forwarded_ip(request)}"


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


class _NoOpLimiter:
    """Pass-through limiter when slowapi is unavailable (tests, minimal installs)."""

    def limit(self, *args, **kwargs):
        def _decorator(func):
            return func

        return _decorator


def _is_dev_or_test() -> bool:
    """Return True when running in a local-development or test environment."""
    env = (
        (
            os.getenv("ENVIRONMENT")
            or os.getenv("APP_ENV")
            or os.getenv("ENV")
            or "production"
        )
        .strip()
        .lower()
    )
    return env in {"development", "dev", "local", "test", "testing"}


#: Exposed for /health endpoint — reports the active rate-limiter backend.
LIMITER_BACKEND: str = "unknown"

_log = logging.getLogger(__name__)

#: Module-level type annotation enables both Limiter and _NoOpLimiter assignments
#: in the conditional below without requiring type: ignore.
limiter: Limiter | _NoOpLimiter


if Limiter is not None:
    _storage_uri = _REDIS_URL if _REDIS_URL else "memory://"
    LIMITER_BACKEND = "redis" if _REDIS_URL else "memory"
    limiter = Limiter(
        key_func=get_forwarded_ip,
        storage_uri=_storage_uri,
    )
else:
    limiter = _NoOpLimiter()
    LIMITER_BACKEND = "noop"


def log_rate_limit_configuration() -> None:
    """Emit deferred rate-limiter startup messages for real CLI/API runs."""
    if Limiter is None:
        _log.warning(
            "slowapi not installed — rate limiting disabled (NoOp). "
            "Acceptable for local development only."
        )
        return
    if _REDIS_URL:
        _log.info("Rate limiter using Redis storage: %s", _REDIS_URL[:20] + "...")
    else:
        _log.warning(
            "REDIS_URL not set — rate limiter using in-memory storage "
            "(per-process, not cluster-wide). Acceptable for local dev only."
        )
    if not _trusted_proxies():
        _log.warning(
            "TRUSTED_PROXIES is not set — X-Forwarded-For headers will be ignored "
            "and rate limiting will key on TCP peer address only. "
            "Set TRUSTED_PROXIES (comma-separated IPs/CIDRs) when behind a load balancer."
        )


def verify_rate_limit_config() -> None:
    """Validate rate-limit configuration for the current environment.

    Must be called from the application startup (lifespan), not at module
    import time. Raises RuntimeError if the configuration is unsafe for
    production. Deferring to startup keeps test imports and type-checking
    import-clean while still enforcing the CHAOS-1554 safety contract.
    """
    if _is_dev_or_test():
        return
    redis_url = os.getenv("REDIS_URL")
    if Limiter is None:
        raise RuntimeError(
            "slowapi is not installed but is required in non-development environments. "
            "Install slowapi (dev_health_ops API dependencies) or set "
            "ENVIRONMENT=development to suppress."
        )
    if redis_url is None:
        raise RuntimeError(
            "REDIS_URL must be set in non-development environments. "
            "In-memory rate-limit storage (memory://) is per-process and "
            "ineffective across multiple replicas. "
            "Set REDIS_URL, or set ENVIRONMENT=development to suppress."
        )
