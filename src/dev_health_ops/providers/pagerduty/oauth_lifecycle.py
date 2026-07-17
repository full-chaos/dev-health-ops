"""Renewal, client-credentials caching, and disconnect orchestration."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

import anyio
import httpx

from dev_health_ops.providers.pagerduty.oauth import (
    DEFAULT_RENEWAL_WINDOW,
    OAuthTokens,
    PagerDutyOAuthConfig,
    client_credentials,
    refresh_tokens,
    revoke_token,
)
from dev_health_ops.providers.pagerduty.oauth_storage import (
    OAuthRotationConflictError,
    VersionedOAuthTokens,
)


class OAuthCredentialStore(Protocol):
    """Minimal persistence seam for token renewal and disconnect."""

    async def get(self) -> VersionedOAuthTokens | None: ...

    async def rotate(self, current_version: int, tokens: OAuthTokens) -> int: ...

    async def delete(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ClientCredentialsRequest:
    scopes: frozenset[str]
    subdomain: str
    region: str


@dataclass
class ClientCredentialsTokenCache:
    """Mutable token cache protected by an async lock for one automation app."""

    tokens: OAuthTokens | None = None
    _lock: anyio.Lock = field(default_factory=anyio.Lock)


def is_renewal_due(
    tokens: OAuthTokens,
    *,
    now: datetime | None = None,
    renewal_window: timedelta = DEFAULT_RENEWAL_WINDOW,
) -> bool:
    """Return whether a token should be renewed before making a request."""
    return tokens.expires_at <= (now or datetime.now(UTC)) + renewal_window


async def get_valid_access_token(
    repository: OAuthCredentialStore,
    config: PagerDutyOAuthConfig,
    *,
    now: datetime | None = None,
    renewal_window: timedelta = DEFAULT_RENEWAL_WINDOW,
) -> str:
    """Return a usable OAuth token, atomically retaining a rotated refresh token."""
    versioned = await repository.get()
    if versioned is None:
        raise OAuthRotationConflictError("PagerDuty OAuth credential was not found")
    if not is_renewal_due(versioned.tokens, now=now, renewal_window=renewal_window):
        return versioned.tokens.access_token
    if versioned.tokens.refresh_token is None:
        raise OAuthRotationConflictError(
            "PagerDuty OAuth credential cannot be refreshed"
        )
    refreshed = await refresh_tokens(config, versioned.tokens.refresh_token)
    rotated = (
        refreshed
        if refreshed.refresh_token
        else refreshed.model_copy(
            update={"refresh_token": versioned.tokens.refresh_token}
        )
    )
    try:
        await repository.rotate(versioned.version, rotated)
        return rotated.access_token
    except OAuthRotationConflictError:
        latest = await repository.get()
        if latest is None:
            raise
        return latest.tokens.access_token


async def get_client_credentials_access_token(
    cache: ClientCredentialsTokenCache,
    config: PagerDutyOAuthConfig,
    request: ClientCredentialsRequest,
    *,
    now: datetime | None = None,
) -> str:
    """Return a cached scoped machine token, renewing it under a single lock."""
    async with cache._lock:
        if cache.tokens is None or is_renewal_due(cache.tokens, now=now):
            cache.tokens = await client_credentials(
                config,
                scopes=set(request.scopes),
                subdomain=request.subdomain,
                region=request.region,
            )
        return cache.tokens.access_token


async def disconnect(
    repository: OAuthCredentialStore,
    config: PagerDutyOAuthConfig,
) -> None:
    """Best-effort remote revocation followed by guaranteed local credential deletion."""
    versioned = await repository.get()
    if versioned is not None:
        token = versioned.tokens.refresh_token or versioned.tokens.access_token
        try:
            await revoke_token(config, token)
        except httpx.HTTPError:
            pass
    await repository.delete()
