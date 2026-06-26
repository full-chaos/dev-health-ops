from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import jwt
import requests

from dev_health_ops.connectors.utils.retry import retry_with_backoff

GITHUB_API_BASE_URL = "https://api.github.com"
JWT_TTL_SECONDS = 600
INSTALLATION_TOKEN_REFRESH_WINDOW_SECONDS = 300
TOKEN_EXCHANGE_MAX_RETRIES = (
    3  # total attempts (initial + retries), per retry_with_backoff
)
TOKEN_EXCHANGE_INITIAL_DELAY_SECONDS = 1.0
TOKEN_EXCHANGE_MAX_DELAY_SECONDS = 10.0


class GitHubAppAuthError(RuntimeError):
    """Raised when GitHub App authentication cannot produce an access token."""


class GitHubAppTransientError(GitHubAppAuthError):
    """Transient GitHub App auth failure (5xx / network) that is safe to retry."""


@dataclass(frozen=True)
class InstallationToken:
    token: str
    expires_at: datetime


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


class GitHubAppTokenProvider:
    """Caches and refreshes GitHub App installation access tokens."""

    def __init__(
        self,
        *,
        app_id: str,
        private_key: str,
        installation_id: str,
        api_base_url: str = GITHUB_API_BASE_URL,
        timeout: int = 30,
        refresh_window_seconds: int = INSTALLATION_TOKEN_REFRESH_WINDOW_SECONDS,
    ) -> None:
        if not app_id:
            raise ValueError("GitHub App auth requires app_id")
        if not private_key:
            raise ValueError("GitHub App auth requires private_key")
        if not installation_id:
            raise ValueError("GitHub App auth requires installation_id")

        self.app_id = str(app_id)
        self.private_key = private_key
        self.installation_id = str(installation_id)
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout
        self.refresh_window_seconds = refresh_window_seconds
        self._cached: InstallationToken | None = None

    def get_token(self) -> str:
        """Return a cached installation token or refresh near expiry."""
        if self._cached and not self._expires_soon(self._cached.expires_at):
            return self._cached.token
        self._cached = self._exchange_installation_token()
        return self._cached.token

    def _expires_soon(self, expires_at: datetime) -> bool:
        now = datetime.now(timezone.utc)
        return (expires_at - now).total_seconds() <= self.refresh_window_seconds

    @retry_with_backoff(
        max_retries=TOKEN_EXCHANGE_MAX_RETRIES,
        initial_delay=TOKEN_EXCHANGE_INITIAL_DELAY_SECONDS,
        max_delay=TOKEN_EXCHANGE_MAX_DELAY_SECONDS,
        exceptions=(GitHubAppTransientError,),
    )
    def _exchange_installation_token(self) -> InstallationToken:
        app_jwt = create_github_app_jwt(
            app_id=self.app_id,
            private_key=self.private_key,
        )
        url = (
            f"{self.api_base_url}/app/installations/"
            f"{self.installation_id}/access_tokens"
        )
        try:
            response = requests.post(
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {app_jwt}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise GitHubAppTransientError(
                f"GitHub App installation token exchange request failed: {exc}"
            ) from exc
        if response.status_code >= 400:
            if 500 <= response.status_code < 600:
                raise GitHubAppTransientError(
                    f"GitHub App installation token exchange failed: HTTP {response.status_code}"
                )
            raise GitHubAppAuthError(
                f"GitHub App installation token exchange failed: HTTP {response.status_code}"
            )

        data: dict[str, Any] = response.json()
        token = data.get("token")
        expires_at_raw = data.get("expires_at")
        if not token or not expires_at_raw:
            raise GitHubAppAuthError(
                "GitHub App installation token response missing token or expires_at"
            )

        expires_at = datetime.fromisoformat(str(expires_at_raw).replace("Z", "+00:00"))
        return InstallationToken(token=str(token), expires_at=expires_at)
