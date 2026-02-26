from __future__ import annotations

import json
import logging

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

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
            logging.getLogger(__name__).debug("Could not parse request body for rate-limit email key")

    email_from_query = request.query_params.get("email")
    if email_from_query:
        return _normalize_email(email_from_query)

    return "unknown"


def get_auth_key(request: Request) -> str:
    ip = get_remote_address(request) or "unknown"
    email = _extract_login_email(request)
    return f"{ip}:{email}"


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


limiter = Limiter(key_func=get_remote_address)
