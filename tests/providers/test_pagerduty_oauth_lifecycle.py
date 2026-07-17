from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from dev_health_ops.providers.pagerduty.auth import ApiTokenAuth, OAuthBearerAuth
from dev_health_ops.providers.pagerduty.oauth import (
    AuthorizationRequest,
    OAuthCallbackValidationError,
    OAuthTokens,
    PagerDutyOAuthConfig,
    client_credentials,
    validate_callback,
)
from dev_health_ops.providers.pagerduty.oauth_lifecycle import (
    ClientCredentialsRequest,
    ClientCredentialsTokenCache,
    disconnect,
    get_client_credentials_access_token,
    get_valid_access_token,
)
from dev_health_ops.providers.pagerduty.oauth_storage import (
    OAuthRotationConflictError,
    PagerDutyOAuthCredentialRepository,
    VersionedOAuthTokens,
)


@dataclass
class FakeRepository:
    current: VersionedOAuthTokens
    rotate_error: bool = False
    rotations: list[OAuthTokens] | None = None

    async def get(self) -> VersionedOAuthTokens:
        return self.current

    async def rotate(self, current_version: int, tokens: OAuthTokens) -> int:
        if self.rotate_error:
            self.current = VersionedOAuthTokens(
                OAuthTokens(
                    access_token="winner",
                    refresh_token="winner-refresh",
                    expires_at=datetime.now(UTC) + timedelta(hours=1),
                ),
                current_version + 1,
            )
            raise OAuthRotationConflictError("conflict")
        self.rotations = [tokens]
        self.current = VersionedOAuthTokens(tokens, current_version + 1)
        return current_version + 1

    async def delete(self) -> None:
        self.rotations = []


@pytest.mark.asyncio
async def test_refreshes_expired_token_and_preserves_rotated_refresh_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def refresh(_: PagerDutyOAuthConfig, __: str) -> OAuthTokens:
        return OAuthTokens(
            access_token="fresh",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.refresh_tokens", refresh
    )
    repo = FakeRepository(
        VersionedOAuthTokens(
            OAuthTokens(
                access_token="stale",
                refresh_token="refresh",
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
            ),
            1,
        )
    )

    token = await get_valid_access_token(
        repo, PagerDutyOAuthConfig("id", "secret", "uri")
    )

    assert token == "fresh"
    assert repo.rotations is not None
    assert repo.rotations[0].refresh_token == "refresh"


@pytest.mark.asyncio
async def test_concurrent_rotation_uses_winner_without_corrupting_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def refresh(_: PagerDutyOAuthConfig, __: str) -> OAuthTokens:
        return OAuthTokens(
            access_token="loser",
            refresh_token="loser-refresh",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.refresh_tokens", refresh
    )
    repo = FakeRepository(
        VersionedOAuthTokens(
            OAuthTokens(
                access_token="stale",
                refresh_token="refresh",
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
            ),
            4,
        ),
        rotate_error=True,
    )

    token = await get_valid_access_token(
        repo, PagerDutyOAuthConfig("id", "secret", "uri")
    )

    assert token == "winner"
    assert repo.current.tokens.access_token == "winner"


@pytest.mark.asyncio
async def test_refresh_failure_does_not_rotate_persisted_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def refresh(_: PagerDutyOAuthConfig, __: str) -> OAuthTokens:
        response = httpx.Response(400, request=httpx.Request("POST", "https://token"))
        raise httpx.HTTPStatusError(
            "refresh failed", request=response.request, response=response
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.refresh_tokens", refresh
    )
    repo = FakeRepository(
        VersionedOAuthTokens(
            OAuthTokens(
                access_token="stale",
                refresh_token="refresh",
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
            ),
            1,
        )
    )

    with pytest.raises(httpx.HTTPStatusError):
        await get_valid_access_token(repo, PagerDutyOAuthConfig("id", "secret", "uri"))
    assert repo.rotations is None


@pytest.mark.asyncio
async def test_disconnect_deletes_local_credential_when_remote_revoke_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def revoke(_: PagerDutyOAuthConfig, __: str) -> None:
        response = httpx.Response(503, request=httpx.Request("POST", "https://revoke"))
        raise httpx.HTTPStatusError(
            "unavailable", request=response.request, response=response
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.revoke_token", revoke
    )
    repo = FakeRepository(
        VersionedOAuthTokens(
            OAuthTokens(
                access_token="access",
                refresh_token="refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
            1,
        )
    )

    await disconnect(repo, PagerDutyOAuthConfig("id", "secret", "uri"))
    assert repo.rotations == []


@pytest.mark.asyncio
async def test_repository_rejects_stale_atomic_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyUpdateResult:
        def scalar_one_or_none(self) -> None:
            return None

    session = AsyncMock()
    session.execute.return_value = EmptyUpdateResult()
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_storage.encrypt_value",
        lambda _: "encrypted",
    )
    repository = PagerDutyOAuthCredentialRepository(session, "org")
    tokens = OAuthTokens(
        access_token="new",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )

    with pytest.raises(OAuthRotationConflictError):
        await repository.rotate(1, tokens)
    assert session.flush.await_count == 0


def test_callback_rejects_invalid_state_and_pkce_verifier() -> None:
    request = AuthorizationRequest("url", "state", "nonce", "verifier")

    with pytest.raises(OAuthCallbackValidationError, match="state"):
        validate_callback(request, state="other", code="code", code_verifier="verifier")
    with pytest.raises(OAuthCallbackValidationError, match="PKCE"):
        validate_callback(request, state="state", code="code", code_verifier="other")


@pytest.mark.asyncio
async def test_client_credentials_cache_renews_once_and_qualifies_account_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[set[str], str, str]] = []

    async def exchange(
        _: PagerDutyOAuthConfig, *, scopes: set[str], subdomain: str, region: str
    ) -> OAuthTokens:
        calls.append((scopes, subdomain, region))
        return OAuthTokens(
            access_token="machine",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.client_credentials",
        exchange,
    )
    cache = ClientCredentialsTokenCache()
    request = ClientCredentialsRequest(frozenset({"Users.read"}), "acme", "eu")
    config = PagerDutyOAuthConfig("id", "secret", "uri")

    assert (
        await get_client_credentials_access_token(cache, config, request) == "machine"
    )
    assert (
        await get_client_credentials_access_token(cache, config, request) == "machine"
    )
    assert calls == [({"Users.read"}, "acme", "eu")]


def test_auth_strategies_use_only_supported_header_forms() -> None:
    assert ApiTokenAuth("api").headers() == {"Authorization": "Token token=api"}
    assert OAuthBearerAuth("oauth").headers() == {"Authorization": "Bearer oauth"}


@pytest.mark.asyncio
async def test_client_credentials_posts_scoped_region_and_subdomain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    class FakeClient:
        async def post(self, _: str, *, data: dict[str, str]) -> httpx.Response:
            captured.update(data)
            return httpx.Response(
                200,
                json={"access_token": "token", "expires_in": 60},
                request=httpx.Request("POST", "https://identity.example/token"),
            )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth.httpx.AsyncClient", FakeClient
    )
    await client_credentials(
        PagerDutyOAuthConfig("id", "secret", "uri"),
        scopes={"Users.read"},
        subdomain="acme",
        region="eu",
    )

    assert captured["subdomain"] == "acme"
    assert captured["region"] == "eu"
