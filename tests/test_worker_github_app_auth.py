"""Regression tests for GitHub App auth support in the sync worker path.

Background sync previously extracted only the ``token`` key from decrypted
credentials and raised when it was absent, so GitHub App-auth sync configs
failed in the Celery worker (CHAOS-2234). These tests lock the fix: the worker
and repo-discovery paths build a typed :class:`GitHubCredentials` (PAT or App)
via ``github_credentials_from_mapping`` and mint an installation token for App
auth.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from dev_health_ops.credentials import CredentialSource
from dev_health_ops.credentials.resolver import github_credentials_from_mapping

# ---------------------------------------------------------------------------
# github_credentials_from_mapping
# ---------------------------------------------------------------------------


def test_mapping_builds_pat_credentials() -> None:
    creds = github_credentials_from_mapping({"token": "ghp_secret"})

    assert creds is not None
    assert creds.is_app_auth is False
    assert creds.token == "ghp_secret"
    assert creds.source == CredentialSource.DATABASE


def test_mapping_builds_app_credentials() -> None:
    creds = github_credentials_from_mapping(
        {
            "app_id": "12345",
            "private_key": "synthetic-private-key",
            "installation_id": "67890",
        }
    )

    assert creds is not None
    assert creds.is_app_auth is True
    assert creds.app_id == "12345"
    assert creds.private_key == "synthetic-private-key"
    assert creds.installation_id == "67890"


def test_mapping_resolves_private_key_path(tmp_path) -> None:
    key_path = tmp_path / "app.pem"
    key_path.write_text("pem-contents", encoding="utf-8")

    creds = github_credentials_from_mapping(
        {
            "app_id": "12345",
            "private_key_path": str(key_path),
            "installation_id": "67890",
        }
    )

    assert creds is not None
    assert creds.is_app_auth is True
    assert creds.private_key == "pem-contents"


def test_mapping_returns_none_when_empty() -> None:
    assert github_credentials_from_mapping({}) is None


def test_mapping_returns_none_when_app_incomplete() -> None:
    # Missing installation_id => not a complete App triple and no token.
    assert (
        github_credentials_from_mapping({"app_id": "12345", "private_key": "k"}) is None
    )


def test_mapping_returns_none_when_token_and_app_conflict() -> None:
    # token + App fields is rejected by GitHubCredentials validation.
    assert (
        github_credentials_from_mapping(
            {
                "token": "ghp_secret",
                "app_id": "12345",
                "private_key": "k",
                "installation_id": "67890",
            }
        )
        is None
    )


def test_mapping_ignores_none_values() -> None:
    creds = github_credentials_from_mapping(
        {"token": "ghp_secret", "app_id": None, "installation_id": None}
    )

    assert creds is not None
    assert creds.is_app_auth is False
    assert creds.token == "ghp_secret"


# ---------------------------------------------------------------------------
# discover_repos_for_config (repo discovery)
# ---------------------------------------------------------------------------


def _make_config(provider: str, sync_options: dict | None = None):
    return SimpleNamespace(provider=provider, sync_options=sync_options or {})


def test_discover_github_app_auth_mints_token_via_connector() -> None:
    from dev_health_ops.discovery import repos as repos_mod

    app_credentials = {
        "app_id": "12345",
        "private_key": "synthetic-private-key",
        "installation_id": "67890",
    }
    config = _make_config("github", {"search": "my-org/*"})

    with (
        patch.object(repos_mod, "discover_github_repos") as discover,
        patch("dev_health_ops.connectors.github.GitHubConnector") as connector_cls,
    ):
        connector_cls.return_value = SimpleNamespace(token="installation-token")
        discover.return_value = [("my-org", "api")]

        result = repos_mod.discover_repos_for_config(config, app_credentials)

    assert result == [("my-org", "api")]
    # App credentials were converted to a typed credential and the connector
    # minted an installation token from them.
    assert connector_cls.call_args.kwargs["credentials"].is_app_auth is True
    # discover_github_repos receives the minted token string, not raw app fields.
    assert discover.call_args.args[1] == "installation-token"


def test_discover_github_pat_passes_token_through() -> None:
    from dev_health_ops.discovery import repos as repos_mod

    config = _make_config("github", {"search": "my-org/*"})

    with patch.object(repos_mod, "discover_github_repos") as discover:
        discover.return_value = [("my-org", "api")]
        result = repos_mod.discover_repos_for_config(config, {"token": "ghp_x"})

    assert result == [("my-org", "api")]
    assert discover.call_args.args[1] == "ghp_x"


def test_discover_github_returns_empty_without_credentials() -> None:
    from dev_health_ops.discovery import repos as repos_mod

    config = _make_config("github", {"search": "my-org/*"})

    with patch.object(repos_mod, "discover_github_repos") as discover:
        result = repos_mod.discover_repos_for_config(config, {})

    assert result == []
    discover.assert_not_called()
