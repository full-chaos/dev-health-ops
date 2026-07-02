"""GitHub App installation-token minting, standalone on httpx.

Discovery (``dev_health_ops.discovery.repos``) needs a bare installation
access token for GitHub App auth but must not depend on the frozen
``connectors.github.GitHubConnector`` (CHAOS-2786). This module mirrors the
JWT construction and installation-token exchange semantics of
``connectors.utils.github_app.GitHubAppTokenProvider`` --  RS256 JWT with
``iss``/``iat``/``exp`` claims, ``POST /app/installations/{id}/access_tokens``,
GHE base-url join, transient-5xx retry with no retry on 4xx -- using httpx
instead of requests, and with no import of ``connectors``.

This intentionally does not replicate the connector's token *caching* layer:
every discovery call site constructs a fresh credentials object and wants a
single fresh mint, so caching across calls has no observable effect there
(see repos.py's ``_github_token_from_resolved_credentials``). Callers that
need a long-lived, auto-refreshing token should not reach for this module.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx
import jwt

logger = logging.getLogger(__name__)

GITHUB_API_BASE_URL = "https://api.github.com"
JWT_TTL_SECONDS = 600
DEFAULT_TIMEOUT_SECONDS = 30.0
TOKEN_EXCHANGE_MAX_RETRIES = 3  # total attempts (initial + retries)
TOKEN_EXCHANGE_INITIAL_DELAY_SECONDS = 1.0
TOKEN_EXCHANGE_MAX_DELAY_SECONDS = 10.0
TOKEN_EXCHANGE_BACKOFF_FACTOR = 2.0


class GitHubAppAuthError(RuntimeError):
    """Raised when GitHub App authentication cannot produce an installation token."""


class GitHubAppTransientError(GitHubAppAuthError):
    """Transient GitHub App auth failure (5xx / network) that is safe to retry."""


def create_github_app_jwt(
    *,
    app_id: str,
    private_key: str,
    now: int | None = None,
) -> str:
    """Sign a GitHub App JWT with RS256.

    GitHub requires ``iss`` to be the App ID and ``exp`` no more than ten
    minutes in the future. ``iat`` is backdated slightly to tolerate clock skew.
    """
    issued_at = int(now if now is not None else time.time()) - 60
    payload = {
        "iat": issued_at,
        "exp": issued_at + JWT_TTL_SECONDS,
        "iss": str(app_id),
    }
    return str(jwt.encode(payload, private_key, algorithm="RS256"))


def _installation_access_tokens_url(base_url: str | None, installation_id: str) -> str:
    resolved = str(base_url or GITHUB_API_BASE_URL).rstrip("/")
    return f"{resolved}/app/installations/{installation_id}/access_tokens"


def mint_installation_token(
    *,
    app_id: str,
    private_key: str,
    installation_id: str,
    base_url: str | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = TOKEN_EXCHANGE_MAX_RETRIES,
    transport: httpx.BaseTransport | None = None,
) -> str:
    """Mint a fresh GitHub App installation access token via httpx.

    :param app_id: GitHub App ID (JWT ``iss`` claim).
    :param private_key: PEM-encoded RS256 private key for the App.
    :param installation_id: Installation ID to mint a token for.
    :param base_url: REST API base URL. Defaults to ``api.github.com``; for
        GitHub Enterprise pass the GHE REST base (e.g.
        ``https://ghe.example.com/api/v3``) -- joined as-is, no path rewriting.
    :param timeout: Per-request timeout in seconds.
    :param max_retries: Total attempts (initial + retries) on transient (5xx /
        network) failures. 4xx failures are never retried.
    :param transport: Optional httpx transport override (tests use
        ``httpx.MockTransport``).
    :returns: The bare installation access token string.
    :raises ValueError: Missing app_id/private_key/installation_id.
    :raises GitHubAppAuthError: Non-retryable failure (4xx, malformed response).
    :raises GitHubAppTransientError: Retries on 5xx/network exhausted.
    """
    if not app_id:
        raise ValueError("GitHub App auth requires app_id")
    if not private_key:
        raise ValueError("GitHub App auth requires private_key")
    if not installation_id:
        raise ValueError("GitHub App auth requires installation_id")

    url = _installation_access_tokens_url(base_url, installation_id)

    delay = TOKEN_EXCHANGE_INITIAL_DELAY_SECONDS
    last_exception: GitHubAppTransientError | None = None

    with httpx.Client(timeout=timeout, transport=transport) as client:
        for attempt in range(max_retries):
            app_jwt = create_github_app_jwt(app_id=app_id, private_key=private_key)
            try:
                response = client.post(
                    url,
                    headers={
                        "Accept": "application/vnd.github+json",
                        "Authorization": f"Bearer {app_jwt}",
                        "X-GitHub-Api-Version": "2022-11-28",
                    },
                )
            except httpx.HTTPError as exc:
                last_exception = GitHubAppTransientError(
                    f"GitHub App installation token exchange request failed: {exc}"
                )
            else:
                if response.status_code >= 400:
                    if 500 <= response.status_code < 600:
                        last_exception = GitHubAppTransientError(
                            "GitHub App installation token exchange failed: "
                            f"HTTP {response.status_code}"
                        )
                    else:
                        raise GitHubAppAuthError(
                            "GitHub App installation token exchange failed: "
                            f"HTTP {response.status_code}"
                        )
                else:
                    data: dict[str, Any] = response.json()
                    token = data.get("token")
                    expires_at = data.get("expires_at")
                    if not token or not expires_at:
                        raise GitHubAppAuthError(
                            "GitHub App installation token response missing "
                            "token or expires_at"
                        )
                    return str(token)

            if attempt < max_retries - 1:
                logger.warning(
                    "GitHub App installation token exchange attempt %s/%s "
                    "failed: %s. Retrying...",
                    attempt + 1,
                    max_retries,
                    last_exception,
                )
                time.sleep(min(delay, TOKEN_EXCHANGE_MAX_DELAY_SECONDS))
                delay *= TOKEN_EXCHANGE_BACKOFF_FACTOR

    assert last_exception is not None
    raise last_exception
