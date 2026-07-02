"""Tests for the standalone providers-side GitHub App token minting utility.

CHAOS-2786: discovery must mint GitHub App installation tokens without
depending on the frozen ``connectors.github.GitHubConnector``. This module
covers ``dev_health_ops.providers.github.app_auth`` in isolation: RS256 JWT
construction, the installation-token exchange over httpx (via
``httpx.MockTransport`` -- no real network), GHE base-url joining, and error
semantics parity with ``connectors.utils.github_app.GitHubAppTokenProvider``
(transient 5xx/network retries, no retry on 4xx).

The RSA keypair used to sign JWTs is generated at runtime via the
``cryptography`` library -- never a committed PEM literal -- to keep secret
scanners (gitleaks) quiet and avoid shipping a real-looking key fixture.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from dev_health_ops.providers.github.app_auth import (
    TOKEN_EXCHANGE_MAX_RETRIES,
    GitHubAppAuthError,
    GitHubAppTransientError,
    create_github_app_jwt,
    mint_installation_token,
)


@pytest.fixture(scope="module")
def rsa_keypair() -> tuple[str, rsa.RSAPrivateKey]:
    """Generate a synthetic RSA keypair for signing/verifying test JWTs."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    return pem, private_key


def _ok_response(token: str = "installation-token") -> httpx.Response:
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat()
    return httpx.Response(201, json={"token": token, "expires_at": expires_at})


# ---------------------------------------------------------------------------
# JWT construction
# ---------------------------------------------------------------------------


def test_create_github_app_jwt_signs_rs256_with_claims(rsa_keypair) -> None:
    pem, private_key = rsa_keypair
    now = 1_700_000_000

    token = create_github_app_jwt(app_id="99999", private_key=pem, now=now)

    decoded = jwt.decode(
        token,
        private_key.public_key(),
        algorithms=["RS256"],
        options={"verify_exp": False},
    )
    assert decoded["iss"] == "99999"
    assert decoded["iat"] == now - 60
    assert decoded["exp"] - decoded["iat"] == 600


# ---------------------------------------------------------------------------
# mint_installation_token -- success paths
# ---------------------------------------------------------------------------


def test_mint_installation_token_success_hits_correct_endpoint_and_headers(
    rsa_keypair,
) -> None:
    pem, _ = rsa_keypair
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["accept"] = request.headers["accept"]
        captured["api_version"] = request.headers["x-github-api-version"]
        auth_header = request.headers["authorization"]
        assert auth_header.startswith("Bearer ")
        decoded = jwt.decode(
            auth_header.removeprefix("Bearer "),
            options={"verify_signature": False},
        )
        captured["jwt_claims"] = decoded
        return _ok_response("installation-token")

    token = mint_installation_token(
        app_id="12345",
        private_key=pem,
        installation_id="67890",
        transport=httpx.MockTransport(handler),
    )

    assert token == "installation-token"
    assert (
        captured["url"]
        == "https://api.github.com/app/installations/67890/access_tokens"
    )
    assert captured["accept"] == "application/vnd.github+json"
    assert captured["api_version"] == "2022-11-28"
    assert captured["jwt_claims"]["iss"] == "12345"


def test_mint_installation_token_joins_ghe_base_url(rsa_keypair) -> None:
    pem, _ = rsa_keypair
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return _ok_response("ghe-token")

    token = mint_installation_token(
        app_id="12345",
        private_key=pem,
        installation_id="67890",
        base_url="https://ghe.example.com/api/v3/",
        transport=httpx.MockTransport(handler),
    )

    assert token == "ghe-token"
    assert seen_urls == [
        "https://ghe.example.com/api/v3/app/installations/67890/access_tokens"
    ]


def test_mint_installation_token_defaults_base_url_when_none(rsa_keypair) -> None:
    pem, _ = rsa_keypair
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return _ok_response()

    mint_installation_token(
        app_id="12345",
        private_key=pem,
        installation_id="67890",
        base_url=None,
        transport=httpx.MockTransport(handler),
    )

    assert seen_urls == ["https://api.github.com/app/installations/67890/access_tokens"]


# ---------------------------------------------------------------------------
# mint_installation_token -- error / retry semantics
# ---------------------------------------------------------------------------


def test_mint_installation_token_does_not_retry_4xx(rsa_keypair) -> None:
    pem, _ = rsa_keypair
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(401, json={"message": "Bad credentials"})

    with patch("dev_health_ops.providers.github.app_auth.time.sleep") as sleep:
        with pytest.raises(GitHubAppAuthError) as excinfo:
            mint_installation_token(
                app_id="12345",
                private_key=pem,
                installation_id="67890",
                transport=httpx.MockTransport(handler),
            )

    assert not isinstance(excinfo.value, GitHubAppTransientError)
    assert calls["count"] == 1
    sleep.assert_not_called()


def test_mint_installation_token_retries_transient_5xx_then_succeeds(
    rsa_keypair,
) -> None:
    pem, _ = rsa_keypair
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(503)
        return _ok_response("recovered-token")

    with patch("dev_health_ops.providers.github.app_auth.time.sleep") as sleep:
        token = mint_installation_token(
            app_id="12345",
            private_key=pem,
            installation_id="67890",
            transport=httpx.MockTransport(handler),
        )

    assert token == "recovered-token"
    assert calls["count"] == 2
    sleep.assert_called_once()


def test_mint_installation_token_retries_network_error_then_succeeds(
    rsa_keypair,
) -> None:
    pem, _ = rsa_keypair
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            raise httpx.ConnectError("boom", request=request)
        return _ok_response("net-recovered-token")

    with patch("dev_health_ops.providers.github.app_auth.time.sleep"):
        token = mint_installation_token(
            app_id="12345",
            private_key=pem,
            installation_id="67890",
            transport=httpx.MockTransport(handler),
        )

    assert token == "net-recovered-token"
    assert calls["count"] == 2


def test_mint_installation_token_exhausts_retries_on_persistent_5xx(
    rsa_keypair,
) -> None:
    pem, _ = rsa_keypair
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(503)

    with patch("dev_health_ops.providers.github.app_auth.time.sleep"):
        with pytest.raises(GitHubAppTransientError) as excinfo:
            mint_installation_token(
                app_id="12345",
                private_key=pem,
                installation_id="67890",
                transport=httpx.MockTransport(handler),
            )

    # Exhausted transient still surfaces as GitHubAppAuthError for callers
    # that only catch the parent type.
    assert isinstance(excinfo.value, GitHubAppAuthError)
    assert calls["count"] == TOKEN_EXCHANGE_MAX_RETRIES


def test_mint_installation_token_rejects_response_missing_expiry(rsa_keypair) -> None:
    pem, _ = rsa_keypair

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"token": "no-expiry"})

    with pytest.raises(GitHubAppAuthError):
        mint_installation_token(
            app_id="12345",
            private_key=pem,
            installation_id="67890",
            transport=httpx.MockTransport(handler),
        )


@pytest.mark.parametrize(
    ("app_id", "private_key", "installation_id"),
    [
        ("", "key", "install"),
        ("app", "", "install"),
        ("app", "key", ""),
    ],
)
def test_mint_installation_token_requires_all_credential_fields(
    app_id: str, private_key: str, installation_id: str
) -> None:
    with pytest.raises(ValueError):
        mint_installation_token(
            app_id=app_id, private_key=private_key, installation_id=installation_id
        )
