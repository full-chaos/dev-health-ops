"""CHAOS-2254: GitHub work-items sync must honor config-resolved credentials.

These tests pin the contract that the work-items GitHub client is built from
the organization-scoped, database-resolved credential (PAT or GitHub App auth)
and never via a ``GITHUB_TOKEN`` environment side-channel.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from dev_health_ops.credentials import CredentialSource, GitHubCredentials
from dev_health_ops.metrics.job_work_items import _build_github_work_client

_ORG_ID = "11111111-1111-1111-1111-111111111111"


def test_build_github_work_client_uses_db_pat_without_env(
    monkeypatch,
) -> None:
    """A database-stored PAT is threaded into the client with no env mutation."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    creds = GitHubCredentials(token="db-pat", source=CredentialSource.DATABASE)

    with (
        patch(
            "dev_health_ops.credentials.resolver.resolve_credentials_sync",
            return_value=creds,
        ) as resolve,
        patch("github.Github"),
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
    ):
        client = _build_github_work_client(org_id=_ORG_ID)

    assert client.auth.token == "db-pat"
    assert client.auth.is_app_auth is False
    resolve.assert_called_once_with("github", org_id=_ORG_ID, allow_env_fallback=True)
    # No os.environ side-channel: GITHUB_TOKEN must not have been set.
    assert "GITHUB_TOKEN" not in os.environ


def test_build_github_work_client_uses_db_app_auth_without_env(
    monkeypatch,
) -> None:
    """GitHub App auth (no PAT) is threaded into the client with no env mutation."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    creds = GitHubCredentials(
        app_id="12345",
        private_key="synthetic-private-key",
        installation_id="67890",
        source=CredentialSource.DATABASE,
    )

    with (
        patch(
            "dev_health_ops.credentials.resolver.resolve_credentials_sync",
            return_value=creds,
        ) as resolve,
        patch(
            "dev_health_ops.providers.github.client.GitHubAppTokenProvider"
        ) as provider_cls,
        patch("github.Github"),
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
    ):
        provider_cls.return_value.get_token.return_value = "installation-token"
        client = _build_github_work_client(org_id=_ORG_ID)

    assert client.auth.is_app_auth is True
    assert client.auth.app_id == "12345"
    assert client.auth.token is None
    resolve.assert_called_once_with("github", org_id=_ORG_ID, allow_env_fallback=True)
    assert "GITHUB_TOKEN" not in os.environ


def test_build_github_work_client_without_org_falls_back_to_from_env() -> None:
    """With no organization scope, construction falls back to ``from_env``."""
    sentinel = object()
    with patch(
        "dev_health_ops.providers.github.client.GitHubWorkClient.from_env",
        return_value=sentinel,
    ) as from_env:
        client = _build_github_work_client(org_id="")

    assert client is sentinel
    from_env.assert_called_once_with()
