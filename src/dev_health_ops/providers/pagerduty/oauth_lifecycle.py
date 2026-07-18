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

    async def get(self) -> VersionedOAuthTokens | None:
        """Return the current stored credential version, if any."""

    async def get_for_update(self) -> VersionedOAuthTokens | None:
        """Return the current credential version under a row lock, if any."""

    async def rotate(
        self,
        current_version: int,
        tokens: OAuthTokens,
        *,
        expected_binding_id: str,
    ) -> int:
        """Rotate the stored token to a new version, returning that version."""

    async def delete(self) -> None:
        """Delete the stored credential."""


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


ClientCredentialsCacheKey = tuple[str, str, frozenset[str], str, str, str]


@dataclass
class ClientCredentialsTokenCacheRegistry:
    """Owns isolated client-credential token caches for account-scoped keys."""

    _caches: dict[ClientCredentialsCacheKey, ClientCredentialsTokenCache] = field(
        default_factory=dict
    )
    _lock: anyio.Lock = field(default_factory=anyio.Lock)

    async def cache_for(
        self, key: ClientCredentialsCacheKey
    ) -> ClientCredentialsTokenCache:
        async with self._lock:
            cache = self._caches.get(key)
            if cache is None:
                cache = ClientCredentialsTokenCache()
                self._caches[key] = cache
            return cache


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
    locked = await repository.get_for_update()
    if locked is None:
        raise OAuthRotationConflictError("PagerDuty OAuth credential was not found")
    if not is_renewal_due(locked.tokens, now=now, renewal_window=renewal_window):
        return locked.tokens.access_token
    if locked.tokens.refresh_token is None or locked.binding_id is None:
        raise OAuthRotationConflictError(
            "PagerDuty OAuth credential cannot be refreshed"
        )
    refreshed = await refresh_tokens(config, locked.tokens.refresh_token)
    rotated = refreshed.model_copy(
        update={
            "refresh_token": refreshed.refresh_token or locked.tokens.refresh_token,
            "granted_scopes": refreshed.granted_scopes or locked.tokens.granted_scopes,
        }
    )
    try:
        await repository.rotate(
            locked.version,
            rotated,
            expected_binding_id=locked.binding_id,
        )
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


async def get_client_credentials_access_token_keyed(
    registry: ClientCredentialsTokenCacheRegistry,
    key: ClientCredentialsCacheKey,
    config: PagerDutyOAuthConfig,
    request: ClientCredentialsRequest,
    *,
    now: datetime | None = None,
) -> str:
    cache = await registry.cache_for(key)
    return await get_client_credentials_access_token(cache, config, request, now=now)


async def disconnect(
    repository: OAuthCredentialStore,
    config: PagerDutyOAuthConfig,
) -> None:
    """Best-effort remote revocation followed by guaranteed local credential deletion."""
    try:
        versioned = await repository.get()
        if versioned is not None:
            token = versioned.tokens.refresh_token or versioned.tokens.access_token
            try:
                await revoke_token(config, token)
            except httpx.HTTPError:
                # Remote revocation is best-effort; the guaranteed local delete
                # in the finally block is the authoritative disconnect action.
                pass
    finally:
        await repository.delete()
