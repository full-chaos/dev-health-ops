"""Webhook authentication and signature validation.

Each provider has a specific validation mechanism:
- GitHub: HMAC-SHA256 signature in X-Hub-Signature-256 header
- GitLab: Secret token in X-Gitlab-Token header
- Jira: Shared secret or IP allowlist (configurable)

These are implemented as FastAPI dependencies for clean integration.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request

logger = logging.getLogger(__name__)


def _get_github_webhook_secret() -> str | None:
    """Get GitHub webhook secret from environment."""
    return os.getenv("GITHUB_WEBHOOK_SECRET")


def _get_gitlab_webhook_token() -> str | None:
    """Get GitLab webhook token from environment."""
    return os.getenv("GITLAB_WEBHOOK_TOKEN")


def _get_jira_webhook_secret() -> str | None:
    """Get Jira webhook secret from environment."""
    return os.getenv("JIRA_WEBHOOK_SECRET")


async def _get_raw_body(request: Request) -> bytes:
    """Extract raw request body for signature validation.

    FastAPI consumes the body stream, so we cache it for reuse.
    """
    if not hasattr(request.state, "raw_body"):
        request.state.raw_body = await request.body()
    return request.state.raw_body


def verify_github_signature(
    body: bytes,
    signature_header: str | None,
    secret: str,
) -> bool:
    """Verify GitHub webhook HMAC-SHA256 signature.

    Args:
        body: Raw request body bytes
        signature_header: Value of X-Hub-Signature-256 header
        secret: Configured webhook secret

    Returns:
        True if signature is valid, False otherwise
    """
    if not signature_header:
        return False

    # GitHub signature format: sha256=<hex_digest>
    if not signature_header.startswith("sha256="):
        return False

    expected_signature = signature_header[7:]  # Remove "sha256=" prefix
    computed = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(computed, expected_signature)


def verify_gitlab_token(
    token_header: str | None,
    secret: str,
) -> bool:
    """Verify GitLab webhook token.

    Args:
        token_header: Value of X-Gitlab-Token header
        secret: Configured webhook token

    Returns:
        True if token matches, False otherwise
    """
    if not token_header:
        return False
    return hmac.compare_digest(token_header, secret)


def verify_jira_signature(
    body: bytes,
    signature_header: str | None,
    secret: str,
) -> bool:
    """Verify Jira webhook HMAC-SHA256 signature.

    For Jira Server/DC, the shared secret is used to compute an
    HMAC-SHA256 digest that must match the X-Hub-Signature header.

    Args:
        body: Raw request body bytes
        signature_header: Value of X-Hub-Signature header
        secret: Configured webhook secret

    Returns:
        True if signature is valid, False otherwise
    """
    if not signature_header:
        return False

    # Jira Server uses HMAC-SHA256
    computed = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(computed, signature_header)


async def validate_github_webhook(
    request: Request,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
) -> bytes:
    """FastAPI dependency to validate GitHub webhook signatures.

    Raises:
        HTTPException: 401 if signature validation fails
        HTTPException: 500 if secret not configured

    Returns:
        Raw request body for further processing
    """
    secret = _get_github_webhook_secret()

    if not secret:
        logger.warning("GITHUB_WEBHOOK_SECRET not configured - rejecting webhook")
        raise HTTPException(
            status_code=500,
            detail="Webhook secret not configured",
        )

    body = await _get_raw_body(request)

    if not verify_github_signature(body, x_hub_signature_256, secret):
        logger.warning("GitHub webhook signature validation failed")
        raise HTTPException(
            status_code=401,
            detail="Invalid signature",
        )

    return body


async def validate_gitlab_webhook(
    request: Request,
    x_gitlab_token: Annotated[str | None, Header()] = None,
) -> bytes:
    """FastAPI dependency to validate GitLab webhook tokens.

    Raises:
        HTTPException: 401 if token validation fails
        HTTPException: 500 if token not configured

    Returns:
        Raw request body for further processing
    """
    secret = _get_gitlab_webhook_token()

    if not secret:
        logger.warning("GITLAB_WEBHOOK_TOKEN not configured - rejecting webhook")
        raise HTTPException(
            status_code=500,
            detail="Webhook token not configured",
        )

    if not verify_gitlab_token(x_gitlab_token, secret):
        logger.warning("GitLab webhook token validation failed")
        raise HTTPException(
            status_code=401,
            detail="Invalid token",
        )

    return await _get_raw_body(request)


async def validate_jira_webhook(
    request: Request,
    x_hub_signature: Annotated[str | None, Header()] = None,
) -> bytes:
    """FastAPI dependency to validate Jira webhooks.

    For Jira Server/DC with a shared secret, the HMAC-SHA256 signature
    is sent in the X-Hub-Signature header.

    Raises:
        HTTPException: 401 if signature validation fails
        HTTPException: 500 if secret not configured

    Returns:
        Raw request body for further processing
    """
    secret = _get_jira_webhook_secret()

    if not secret:
        logger.warning("JIRA_WEBHOOK_SECRET not configured - rejecting webhook")
        raise HTTPException(
            status_code=500,
            detail="Webhook secret not configured",
        )

    body = await _get_raw_body(request)

    if not verify_jira_signature(body, x_hub_signature, secret):
        logger.warning("Jira webhook signature validation failed")
        raise HTTPException(
            status_code=401,
            detail="Invalid signature",
        )

    return body


# Type aliases for dependency injection
GitHubWebhookBody = Annotated[bytes, Depends(validate_github_webhook)]
GitLabWebhookBody = Annotated[bytes, Depends(validate_gitlab_webhook)]
JiraWebhookBody = Annotated[bytes, Depends(validate_jira_webhook)]
