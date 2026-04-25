from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from dev_health_ops.cli import build_parser
from dev_health_ops.connectors.github import GitHubConnector
from dev_health_ops.connectors.utils.github_app import (
    GitHubAppTokenProvider,
    create_github_app_jwt,
)
from dev_health_ops.credentials import CredentialSource, GitHubCredentials
from dev_health_ops.processors.sync import _resolve_github_sync_credentials
from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient


def test_create_github_app_jwt_signs_rs256_with_app_id() -> None:
    with patch("dev_health_ops.connectors.utils.github_app.jwt.encode") as encode:
        encode.return_value = "signed.jwt"

        token = create_github_app_jwt(
            app_id="12345",
            private_key="synthetic-private-key",
            now=1_700_000_000,
        )

    assert token == "signed.jwt"
    payload = encode.call_args.args[0]
    assert payload["iss"] == "12345"
    assert payload["exp"] - payload["iat"] == 600
    encode.assert_called_once_with(payload, "synthetic-private-key", algorithm="RS256")


def test_installation_token_exchange_caches_until_refresh_window() -> None:
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat()
    response = Mock(status_code=201)
    response.json.return_value = {
        "token": "installation-token",
        "expires_at": expires_at,
    }

    with (
        patch(
            "dev_health_ops.connectors.utils.github_app.create_github_app_jwt",
            return_value="app-jwt",
        ),
        patch(
            "dev_health_ops.connectors.utils.github_app.requests.post",
            return_value=response,
        ) as post,
    ):
        provider = GitHubAppTokenProvider(
            app_id="12345",
            private_key="synthetic-private-key",
            installation_id="67890",
        )
        first = provider.get_token()
        second = provider.get_token()

    assert first == "installation-token"
    assert second == "installation-token"
    post.assert_called_once()
    assert post.call_args.args[0].endswith("/app/installations/67890/access_tokens")
    assert post.call_args.kwargs["headers"]["Authorization"] == "Bearer app-jwt"


def test_github_connector_branches_to_app_token_provider() -> None:
    credentials = GitHubCredentials(
        app_id="12345",
        private_key="synthetic-private-key",
        installation_id="67890",
        source=CredentialSource.ENVIRONMENT,
    )

    with (
        patch(
            "dev_health_ops.connectors.github.GitHubAppTokenProvider"
        ) as provider_cls,
        patch("dev_health_ops.connectors.github.Github") as github_cls,
        patch("dev_health_ops.connectors.github.GitHubGraphQLClient") as graphql_cls,
    ):
        provider = provider_cls.return_value
        provider.get_token.return_value = "installation-token"

        connector = GitHubConnector(credentials=credentials)

    assert connector.token == "installation-token"
    provider_cls.assert_called_once_with(
        app_id="12345",
        private_key="synthetic-private-key",
        installation_id="67890",
    )
    github_cls.assert_called_once()
    graphql_cls.assert_called_once()
    assert graphql_cls.call_args.kwargs["token_provider"] == provider.get_token


def test_github_work_client_branches_to_app_token_provider() -> None:
    auth = GitHubAuth(
        app_id="12345",
        private_key="synthetic-private-key",
        installation_id="67890",
    )

    with (
        patch(
            "dev_health_ops.providers.github.client.GitHubAppTokenProvider"
        ) as provider_cls,
        patch("github.Github") as github_cls,
        patch(
            "dev_health_ops.providers.github.client.GitHubGraphQLClient"
        ) as graphql_cls,
    ):
        provider = provider_cls.return_value
        provider.get_token.return_value = "installation-token"

        client = GitHubWorkClient(auth=auth)

    assert client.auth.is_app_auth is True
    provider_cls.assert_called_once_with(
        app_id="12345",
        private_key="synthetic-private-key",
        installation_id="67890",
    )
    github_cls.assert_called_once()
    graphql_cls.assert_called_once()
    assert graphql_cls.call_args.kwargs["token_provider"] == provider.get_token


def test_sync_git_cli_parses_github_app_flags_and_resolves_credentials(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key_path = tmp_path / "github-app.pem"
    key_path.write_text("synthetic-private-key", encoding="utf-8")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_APP_ID", raising=False)
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

    ns = build_parser().parse_args(
        [
            "sync",
            "git",
            "--provider",
            "github",
            "--github-app-id",
            "12345",
            "--github-app-key-path",
            str(key_path),
            "--github-app-installation-id",
            "67890",
            "--owner",
            "org",
            "--repo",
            "repo",
        ]
    )

    credentials = _resolve_github_sync_credentials(ns)

    assert credentials.is_app_auth is True
    assert credentials.app_id == "12345"
    assert credentials.private_key == "synthetic-private-key"
    assert credentials.installation_id == "67890"


def test_sync_git_cli_rejects_pat_and_app_flags(tmp_path) -> None:
    key_path = tmp_path / "github-app.pem"
    key_path.write_text("synthetic-private-key", encoding="utf-8")
    ns = build_parser().parse_args(
        [
            "sync",
            "git",
            "--provider",
            "github",
            "--auth",
            "ghp_pat",
            "--github-app-id",
            "12345",
            "--github-app-key-path",
            str(key_path),
            "--github-app-installation-id",
            "67890",
            "--owner",
            "org",
            "--repo",
            "repo",
        ]
    )

    with pytest.raises(SystemExit, match="exactly one mode"):
        _resolve_github_sync_credentials(ns)
