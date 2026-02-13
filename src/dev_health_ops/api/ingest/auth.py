"""Ingest API authentication, HMAC signature verification, and idempotency.

Authentication layers:
- API Key: X-API-Key header validated against INGEST_API_KEYS env var
- HMAC Signature: Optional X-Signature-256 header (sha256=<hex>) using INGEST_SIGNING_SECRET
- Idempotency: Optional X-Idempotency-Key header with Redis-backed deduplication

All layers degrade gracefully when not configured (permissive mode for development).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request

logger = logging.getLogger(__name__)


def _get_api_keys() -> list[str]:
    """Get valid API keys from environment.

    Returns empty list if INGEST_API_KEYS is not set (auth disabled).
    """
    raw = os.getenv("INGEST_API_KEYS", "")
    if not raw.strip():
        return []
    return [k.strip() for k in raw.split(",") if k.strip()]


def _get_signing_secret() -> str | None:
    """Get HMAC signing secret from environment."""
    return os.getenv("INGEST_SIGNING_SECRET") or None


async def _get_raw_body(request: Request) -> bytes:
    """Extract raw request body for signature validation.

    FastAPI consumes the body stream, so we cache it for reuse.
    """
    if not hasattr(request.state, "raw_body"):
        request.state.raw_body = await request.body()
    return request.state.raw_body


def _verify_signature(
    body: bytes,
    signature_header: str | None,
    secret: str,
) -> bool:
    """Verify HMAC-SHA256 signature.

    Args:
        body: Raw request body bytes
        signature_header: Value of X-Signature-256 header (sha256=<hex>)
        secret: Configured signing secret

    Returns:
        True if signature is valid, False otherwise
    """
    if not signature_header:
        return False

    if not signature_header.startswith("sha256="):
        return False

    expected_signature = signature_header[7:]
    computed = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(computed, expected_signature)


async def validate_ingest_auth(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
    x_signature_256: Annotated[str | None, Header()] = None,
) -> dict:
    """FastAPI dependency to validate ingest API authentication.

    Validates API key and optional HMAC signature. Both layers degrade
    gracefully when their respective env vars are not configured.

    Returns:
        Auth context dict with validation metadata.

    Raises:
        HTTPException: 401 if authentication fails.
    """
    valid_keys = _get_api_keys()
    if valid_keys:
        if not x_api_key or x_api_key not in valid_keys:
            raise HTTPException(status_code=401, detail="Invalid API key")

    signing_secret = _get_signing_secret()
    if signing_secret:
        body = await _get_raw_body(request)
        if not _verify_signature(body, x_signature_256, signing_secret):
            raise HTTPException(status_code=401, detail="Invalid signature")

    return {
        "api_key_present": x_api_key is not None,
        "signature_verified": signing_secret is not None,
    }


async def check_idempotency(
    request: Request,
    x_idempotency_key: Annotated[str | None, Header()] = None,
) -> str | None:
    """FastAPI dependency for idempotency checking.

    If an idempotency key is provided, checks Redis for duplicates.
    Gracefully degrades if Redis is unavailable.

    Returns:
        The idempotency key if provided and not duplicate, None otherwise.

    Raises:
        HTTPException: 409 if duplicate request detected.
    """
    if not x_idempotency_key:
        return None

    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return x_idempotency_key

    try:
        import redis

        rc = redis.from_url(redis_url, decode_responses=True)
        was_set = rc.set(f"idem:{x_idempotency_key}", "1", nx=True, ex=86400)
        if not was_set:
            raise HTTPException(status_code=409, detail="Duplicate request")
        return x_idempotency_key
    except HTTPException:
        raise
    except Exception:
        logger.warning("Redis unavailable for idempotency check, skipping")
        return x_idempotency_key


IngestAuthContext = Annotated[dict, Depends(validate_ingest_auth)]
IngestIdempotencyKey = Annotated[str | None, Depends(check_idempotency)]
