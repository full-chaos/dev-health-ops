from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base  # noqa: E402
from dev_health_ops.models.settings import (  # noqa: E402
    SyncConfiguration,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    yield session


def _make_config(
    provider: str = "github",
    sync_options: dict | None = None,
    sync_targets: list | None = None,
    is_active: bool = True,
    name: str = "test-config",
    org_id: str = "default",
) -> SyncConfiguration:
    config = SyncConfiguration(
        name=name,
        provider=provider,
        org_id=org_id,
        sync_targets=sync_targets or ["git", "prs"],
        sync_options=sync_options or {},
        is_active=is_active,
    )
    return config


class TestDiscoverReposForConfig:
    @patch("dev_health_ops.discovery.repos.discover_github_repos")
    def test_github_delegates_to_github_discovery(self, mock_gh):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_gh.return_value = [("my-org", "repo-a"), ("my-org", "repo-b")]
        config = _make_config(
            provider="github",
            sync_options={"search": "my-org/*"},
        )
        result = discover_repos_for_config(config, {"token": "ghp_test"})
        assert result == [("my-org", "repo-a"), ("my-org", "repo-b")]
        mock_gh.assert_called_once()

    @patch("requests.get")
    @patch("dev_health_ops.providers.github.app_auth.mint_installation_token")
    def test_github_app_all_repos_lists_installation_repositories(
        self, mock_mint_token, mock_get
    ):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_mint_token.return_value = "installation-token"
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = {
            "repositories": [
                {"name": "api", "owner": {"login": "orgA"}},
                {"name": "web", "owner": {"login": "orgA"}},
                {"name": "api-worker", "owner": {"login": "orgA"}},
                {"name": "api", "owner": {"login": "orgB"}},
            ]
        }
        mock_get.return_value = mock_response
        config = _make_config(
            provider="github",
            sync_options={"all_repos": True, "search": "orgA/api*"},
        )

        result = discover_repos_for_config(
            config,
            {
                "app_id": "123",
                "private_key": "private-key",
                "installation_id": "456",
                "base_url": "https://api.github.test",
            },
        )

        assert result == [("orgA", "api"), ("orgA", "api-worker")]
        # Installation token is minted via the standalone providers.github
        # utility (CHAOS-2786), not the connectors-side GitHubAppTokenProvider.
        mock_mint_token.assert_called_once_with(
            app_id="123",
            private_key="private-key",
            installation_id="456",
            base_url="https://api.github.test",
        )
        mock_get.assert_called_once()
        assert mock_get.call_args.args[0] == (
            "https://api.github.test/installation/repositories"
        )
        assert mock_get.call_args.kwargs["headers"]["Authorization"] == (
            "Bearer installation-token"
        )

    @patch("requests.get")
    @patch("dev_health_ops.providers.github.app_auth.mint_installation_token")
    def test_github_app_all_repos_installation_api_failure_raises(
        self, mock_mint_token, mock_get
    ):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_mint_token.return_value = "installation-token"
        mock_get.return_value = MagicMock(status_code=401, text="bad credentials")
        config = _make_config(
            provider="github",
            sync_options={"all_repos": True, "search": "orgA/*"},
        )

        with pytest.raises(RuntimeError, match="HTTP 401"):
            discover_repos_for_config(
                config,
                {
                    "app_id": "123",
                    "private_key": "private-key",
                    "installation_id": "456",
                },
            )

    @patch("requests.get")
    @patch("dev_health_ops.providers.github.app_auth.mint_installation_token")
    def test_github_app_all_repos_defaults_base_url_and_propagates_mint_error(
        self, mock_mint_token, mock_get
    ):
        """The ``all_repos`` App-auth path (Codex-flagged CHAOS-2786 gap) --

        (a) defaults ``base_url`` to ``api.github.com`` through the new
        minter exactly like the non-``all_repos`` path, and (b) propagates
        whatever the minter raises (``providers.github.app_auth`` errors,
        not a connectors-side exception type) without ever calling the
        installation-listing endpoint.
        """
        from dev_health_ops.discovery.repos import discover_repos_for_config
        from dev_health_ops.providers.github.app_auth import GitHubAppAuthError

        mock_mint_token.side_effect = GitHubAppAuthError("boom")
        config = _make_config(
            provider="github",
            sync_options={"all_repos": True, "search": "orgA/*"},
        )

        with pytest.raises(GitHubAppAuthError, match="boom"):
            discover_repos_for_config(
                config,
                {
                    "app_id": "123",
                    "private_key": "private-key",
                    "installation_id": "456",
                },
            )

        mock_mint_token.assert_called_once_with(
            app_id="123",
            private_key="private-key",
            installation_id="456",
            base_url="https://api.github.com",
        )
        mock_get.assert_not_called()

    @patch("dev_health_ops.discovery.repos.discover_gitlab_repos")
    def test_gitlab_delegates_to_gitlab_discovery(self, mock_gl):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_gl.return_value = [("123", "grp/proj-a"), ("456", "grp/proj-b")]
        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
        )
        result = discover_repos_for_config(config, {"token": "glpat_test"})
        assert result == [("123", "grp/proj-a"), ("456", "grp/proj-b")]
        mock_gl.assert_called_once()

    def test_unsupported_provider_returns_empty(self):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        config = _make_config(provider="jira")
        result = discover_repos_for_config(config, {"token": "test"})
        assert result == []


class TestDiscoverGithubRepos:
    @patch("github.Github")
    def test_lists_org_repos_filtered_by_pattern(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        repo_a = SimpleNamespace(name="api-service")
        repo_b = SimpleNamespace(name="web-app")
        repo_c = SimpleNamespace(name="docs")

        mock_org = MagicMock()
        mock_org.get_repos.return_value = [repo_a, repo_b, repo_c]
        mock_github_cls.return_value.get_organization.return_value = mock_org

        result = discover_github_repos({"search": "my-org/api-*"}, "ghp_token")
        assert result == [("my-org", "api-service")]

    @patch("github.Github")
    def test_wildcard_star_matches_all(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        repos = [SimpleNamespace(name=f"repo-{i}") for i in range(3)]
        mock_org = MagicMock()
        mock_org.get_repos.return_value = repos
        mock_github_cls.return_value.get_organization.return_value = mock_org

        result = discover_github_repos({"search": "org/*"}, "token")
        assert len(result) == 3

    @patch("github.Github")
    def test_bare_org_search_lists_all_org_repos(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        repos = [SimpleNamespace(name="api"), SimpleNamespace(name="web")]
        mock_org = MagicMock()
        mock_org.get_repos.return_value = repos
        mock_github_cls.return_value.get_organization.return_value = mock_org

        result = discover_github_repos({"search": "full-chaos"}, "token")

        mock_github_cls.return_value.get_organization.assert_called_once_with(
            "full-chaos"
        )
        assert result == [("full-chaos", "api"), ("full-chaos", "web")]

    @patch("github.Github")
    def test_falls_back_to_user_repos_on_org_error(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        mock_g = mock_github_cls.return_value
        mock_g.get_organization.side_effect = Exception("Not an org")
        mock_user = MagicMock()
        mock_user.get_repos.return_value = [SimpleNamespace(name="my-repo")]
        mock_g.get_user.return_value = mock_user

        result = discover_github_repos({"search": "user/*"}, "token")
        assert result == [("user", "my-repo")]

    @patch("github.Github")
    def test_all_repos_lists_authenticated_repos_without_owner(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        repos = [
            SimpleNamespace(name="api", owner=SimpleNamespace(login="org-a")),
            SimpleNamespace(name="web", owner=SimpleNamespace(login="org-b")),
        ]
        mock_user = MagicMock()
        mock_user.get_repos.return_value = repos
        mock_g = mock_github_cls.return_value
        mock_g.get_user.return_value = mock_user

        result = discover_github_repos({"all_repos": True, "search": ""}, "token")

        mock_g.get_user.assert_called_once_with()
        mock_g.get_organization.assert_not_called()
        assert result == [("org-a", "api"), ("org-b", "web")]

    @patch("github.Github")
    def test_all_repos_filters_and_uses_full_name_owner_fallback(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        repos = [
            SimpleNamespace(name="api-service", owner=SimpleNamespace(login="org-a")),
            SimpleNamespace(name="web-app", owner=SimpleNamespace(login="org-b")),
            SimpleNamespace(
                name="api-worker", owner=None, full_name="org-c/api-worker"
            ),
        ]
        mock_user = MagicMock()
        mock_user.get_repos.return_value = repos
        mock_github_cls.return_value.get_user.return_value = mock_user

        result = discover_github_repos({"all_repos": True, "search": "api-*"}, "token")

        assert result == [("org-a", "api-service"), ("org-c", "api-worker")]

    @patch("github.Github")
    def test_all_repos_search_namespace_limits_authenticated_repos(
        self, mock_github_cls
    ):
        from dev_health_ops.discovery.repos import discover_github_repos

        repos = [
            SimpleNamespace(name="api", owner=SimpleNamespace(login="orgA")),
            SimpleNamespace(name="web", owner=SimpleNamespace(login="orgA")),
            SimpleNamespace(name="api", owner=SimpleNamespace(login="orgB")),
        ]
        mock_user = MagicMock()
        mock_user.get_repos.return_value = repos
        mock_github_cls.return_value.get_user.return_value = mock_user

        result = discover_github_repos({"all_repos": True, "search": "orgA/*"}, "token")

        assert result == [("orgA", "api"), ("orgA", "web")]

    @patch("github.Github")
    def test_all_repos_owner_limits_authenticated_repos(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        repos = [
            SimpleNamespace(name="api", owner=SimpleNamespace(login="orgA")),
            SimpleNamespace(name="web", owner=SimpleNamespace(login="orgA")),
            SimpleNamespace(name="api", owner=SimpleNamespace(login="orgB")),
        ]
        mock_user = MagicMock()
        mock_user.get_repos.return_value = repos
        mock_github_cls.return_value.get_user.return_value = mock_user

        result = discover_github_repos({"all_repos": True, "owner": "orgA"}, "token")

        assert result == [("orgA", "api"), ("orgA", "web")]

    @patch("github.Github")
    def test_all_repos_user_listing_failure_raises(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        mock_github_cls.return_value.get_user.side_effect = RuntimeError("rate limited")

        with pytest.raises(RuntimeError, match="rate limited"):
            discover_github_repos({"all_repos": True, "search": "orgA/*"}, "token")

    @patch("github.Github")
    def test_returns_empty_on_total_failure(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        mock_g = mock_github_cls.return_value
        mock_g.get_organization.side_effect = Exception("fail")
        mock_g.get_user.side_effect = Exception("fail")

        result = discover_github_repos({"search": "org/*"}, "token")
        assert result == []

    def test_returns_empty_without_owner_when_all_repos_false(self):
        from dev_health_ops.discovery.repos import discover_github_repos

        result = discover_github_repos({"search": ""}, "token")
        assert result == []


class TestDiscoverGitlabRepos:
    @patch("gitlab.Gitlab")
    def test_lists_group_projects_filtered(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        proj_a = SimpleNamespace(name="api", id=100)
        proj_b = SimpleNamespace(name="web", id=200)
        proj_c = SimpleNamespace(name="docs", id=300)

        mock_grp = MagicMock()
        mock_grp.projects.list.return_value = [proj_a, proj_b, proj_c]
        mock_gitlab_cls.return_value.groups.get.return_value = mock_grp

        result = discover_gitlab_repos({"search": "my-group/api*"}, "glpat_token")
        assert result == [("100", "my-group/api")]

    @patch("gitlab.Gitlab")
    def test_group_project_scope_uses_path_slug_not_display_name(self, mock_gitlab_cls):
        """CHAOS-2450: when a group project lacks path_with_namespace, the scope
        must use the URL path slug (canonical, matches the stored repo full_name)
        not the display name, which can diverge and silently fail work-items
        scoping.
        """
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        proj = SimpleNamespace(name="API Service", path="api-service", id=100)
        mock_grp = MagicMock()
        mock_grp.projects.list.return_value = [proj]
        mock_gitlab_cls.return_value.groups.get.return_value = mock_grp

        result = discover_gitlab_repos({"search": "my-group/*"}, "glpat_token")
        assert result == [("100", "my-group/api-service")]

    @patch("gitlab.Gitlab")
    def test_bare_group_search_lists_all_group_projects(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        proj_a = SimpleNamespace(name="api", id=100)
        proj_b = SimpleNamespace(name="web", id=200)
        mock_grp = MagicMock()
        mock_grp.projects.list.return_value = [proj_a, proj_b]
        mock_gitlab_cls.return_value.groups.get.return_value = mock_grp

        result = discover_gitlab_repos({"search": "my-group"}, "glpat_token")

        mock_gitlab_cls.return_value.groups.get.assert_called_once_with("my-group")
        assert result == [("100", "my-group/api"), ("200", "my-group/web")]

    @patch("gitlab.Gitlab")
    def test_returns_empty_on_group_error(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        mock_gitlab_cls.return_value.groups.get.side_effect = Exception("nope")
        result = discover_gitlab_repos({"search": "group/*"}, "token")
        assert result == []

    @patch("gitlab.Gitlab")
    def test_all_repos_lists_membership_projects_without_group(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        projects = [
            SimpleNamespace(name="api", id=100, path_with_namespace="ns/api"),
            SimpleNamespace(name="web", id=200, path_with_namespace="ns/web"),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos({"all_repos": True, "search": ""}, "token")

        mock_gitlab_cls.return_value.projects.list.assert_called_once_with(
            all=True, membership=True
        )
        mock_gitlab_cls.return_value.groups.get.assert_not_called()
        assert result == [("100", "ns/api"), ("200", "ns/web")]

    @patch("gitlab.Gitlab")
    def test_all_repos_filters_membership_projects(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        projects = [
            SimpleNamespace(name="api", id=100, path_with_namespace="ns/api"),
            SimpleNamespace(name="web", id=200, path_with_namespace="ns/web"),
            SimpleNamespace(
                name="api-worker", id=300, path_with_namespace="ns/api-worker"
            ),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos({"all_repos": True, "search": "api*"}, "token")

        assert result == [("100", "ns/api"), ("300", "ns/api-worker")]

    @patch("gitlab.Gitlab")
    def test_all_repos_group_limits_membership_projects(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        projects = [
            SimpleNamespace(name="api", id=100, path_with_namespace="grpA/api"),
            SimpleNamespace(name="web", id=200, path_with_namespace="grpA/sub/web"),
            SimpleNamespace(name="api", id=300, path_with_namespace="grpB/api"),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos({"all_repos": True, "group": "grpA"}, "token")

        assert result == [("100", "grpA/api"), ("200", "grpA/sub/web")]

    @patch("gitlab.Gitlab")
    def test_all_repos_owner_limits_membership_projects(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        projects = [
            SimpleNamespace(name="api", id=100, path_with_namespace="grpA/api"),
            SimpleNamespace(name="web", id=200, path_with_namespace="grpA/sub/web"),
            SimpleNamespace(name="api", id=300, path_with_namespace="grpB/api"),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos({"all_repos": True, "owner": "grpA"}, "token")

        assert result == [("100", "grpA/api"), ("200", "grpA/sub/web")]

    @patch("gitlab.Gitlab")
    def test_all_repos_nested_subgroup_search(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        projects = [
            SimpleNamespace(name="api", id=100, path_with_namespace="grpA/sub/api"),
            SimpleNamespace(name="web", id=200, path_with_namespace="grpA/other/web"),
            SimpleNamespace(name="db", id=300, path_with_namespace="elsewhere/db"),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos(
            {"all_repos": True, "search": "grpA/sub/*"}, "token"
        )

        assert result == [("100", "grpA/sub/api")]

    @patch("gitlab.Gitlab")
    def test_all_repos_membership_listing_failure_raises(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        mock_gitlab_cls.return_value.projects.list.side_effect = RuntimeError(
            "token revoked"
        )

        with pytest.raises(RuntimeError, match="token revoked"):
            discover_gitlab_repos({"all_repos": True, "search": "grpA/*"}, "token")

    def test_returns_empty_without_group_when_all_repos_false(self):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        result = discover_gitlab_repos({"search": ""}, "token")
        assert result == []


class TestGitLabCredentialsFromMapping:
    def test_token_only_defaults_to_gitlab_com(self):
        from dev_health_ops.credentials.resolver import gitlab_credentials_from_mapping

        creds = gitlab_credentials_from_mapping({"token": "glpat_secret"})
        assert creds is not None
        assert creds.token == "glpat_secret"
        assert creds.base_url == "https://gitlab.com"

    def test_gitlab_url_key_is_used(self):
        from dev_health_ops.credentials.resolver import gitlab_credentials_from_mapping

        creds = gitlab_credentials_from_mapping(
            {"token": "glpat_secret", "gitlab_url": "https://gitlab.example.com"}
        )
        assert creds is not None
        assert creds.base_url == "https://gitlab.example.com"

    def test_url_key_is_used(self):
        from dev_health_ops.credentials.resolver import gitlab_credentials_from_mapping

        creds = gitlab_credentials_from_mapping(
            {"token": "glpat_secret", "url": "https://gl.corp.internal"}
        )
        assert creds is not None
        assert creds.base_url == "https://gl.corp.internal"

    def test_base_url_key_is_used(self):
        from dev_health_ops.credentials.resolver import gitlab_credentials_from_mapping

        creds = gitlab_credentials_from_mapping(
            {"token": "glpat_secret", "base_url": "https://gl.env.internal"}
        )
        assert creds is not None
        assert creds.base_url == "https://gl.env.internal"

    def test_gitlab_url_takes_precedence_over_url_and_base_url(self):
        from dev_health_ops.credentials.resolver import gitlab_credentials_from_mapping

        creds = gitlab_credentials_from_mapping(
            {
                "token": "glpat_secret",
                "gitlab_url": "https://first.example.com",
                "url": "https://second.example.com",
                "base_url": "https://third.example.com",
            }
        )
        assert creds is not None
        assert creds.base_url == "https://first.example.com"

    def test_missing_token_returns_none(self):
        from dev_health_ops.credentials.resolver import gitlab_credentials_from_mapping

        assert gitlab_credentials_from_mapping({}) is None
        assert (
            gitlab_credentials_from_mapping({"url": "https://gl.example.com"}) is None
        )
        assert gitlab_credentials_from_mapping({"token": None}) is None


class TestResolveGitlabUrl:
    def test_sync_options_take_precedence_over_credentials(self):
        from dev_health_ops.credentials.resolver import (
            gitlab_credentials_from_mapping,
            resolve_gitlab_url,
        )

        creds = gitlab_credentials_from_mapping(
            {"token": "t", "url": "https://cred.example.com"}
        )
        assert (
            resolve_gitlab_url({"gitlab_url": "https://opt.example.com"}, creds)
            == "https://opt.example.com"
        )

    def test_falls_back_to_credential_url(self):
        from dev_health_ops.credentials.resolver import (
            gitlab_credentials_from_mapping,
            resolve_gitlab_url,
        )

        creds = gitlab_credentials_from_mapping(
            {"token": "t", "url": "https://cred.example.com"}
        )
        assert resolve_gitlab_url({}, creds) == "https://cred.example.com"

    def test_defaults_to_gitlab_com(self):
        from dev_health_ops.credentials.resolver import resolve_gitlab_url

        assert resolve_gitlab_url({}, None) == "https://gitlab.com"


class TestSyncQueueForProvider:
    """Per-provider queue routing helper (CHAOS-2299), gated by the
    PROVIDER_SYNC_QUEUES_ENABLED env flag (default OFF) so consumers with
    expanded -Q lists deploy before producers start routing — a producer on
    the new code with old-`-Q` workers would otherwise strand syncs on queues
    nothing consumes. To be absorbed by SyncDispatchPolicy (CHAOS-2284)."""

    @pytest.mark.parametrize(
        "provider", ["github", "gitlab", "linear", "jira", "launchdarkly"]
    )
    def test_known_providers_get_dedicated_queue_when_enabled(
        self, provider, monkeypatch
    ):
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider(provider) == f"sync.{provider}"

    @pytest.mark.parametrize(
        "provider", ["github", "gitlab", "linear", "jira", "launchdarkly", "unknown"]
    )
    def test_flag_unset_routes_everything_to_shared_queue(self, provider, monkeypatch):
        """Default-off: mixed deploys (producers upgraded, consumers not yet)
        must keep every sync on the legacy shared queue."""
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.delenv("PROVIDER_SYNC_QUEUES_ENABLED", raising=False)
        assert sync_queue_for_provider(provider) == "sync"

    @pytest.mark.parametrize("value", ["false", "0", "no", "", "off", "bogus"])
    def test_falsy_flag_values_route_to_shared_queue(self, value, monkeypatch):
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", value)
        assert sync_queue_for_provider("github") == "sync"

    @pytest.mark.parametrize("value", ["1", "true", "yes", "TRUE", " True "])
    def test_truthy_flag_values_enable_provider_queues(self, value, monkeypatch):
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", value)
        assert sync_queue_for_provider("github") == "sync.github"

    def test_flag_is_read_at_call_time_not_import_time(self, monkeypatch):
        """Ops must be able to flip the flag without import-order pain: the
        same already-imported function changes behavior with the env."""
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.delenv("PROVIDER_SYNC_QUEUES_ENABLED", raising=False)
        assert sync_queue_for_provider("linear") == "sync"
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider("linear") == "sync.linear"
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "false")
        assert sync_queue_for_provider("linear") == "sync"

    @pytest.mark.parametrize("provider", ["", "local", "bitbucket", "unknown"])
    def test_unknown_providers_fall_back_to_shared_queue(self, provider, monkeypatch):
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider(provider) == "sync"

    def test_normalizes_case_and_whitespace(self, monkeypatch):
        from dev_health_ops.workers.queues import sync_queue_for_provider

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider(" GitHub ") == "sync.github"

    def test_every_routable_queue_is_declared_in_task_queues(self, monkeypatch):
        """Routing to an undeclared queue would strand messages: every queue
        the helper can return must exist in workers.config.task_queues (which
        the compose -Q coverage test then ties to a consumer)."""
        from dev_health_ops.workers.config import task_queues
        from dev_health_ops.workers.queues import (
            SYNC_QUEUE_PROVIDERS,
            sync_queue_for_provider,
        )

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        for provider in SYNC_QUEUE_PROVIDERS:
            assert sync_queue_for_provider(provider) in task_queues
        assert sync_queue_for_provider("unknown") in task_queues
        monkeypatch.delenv("PROVIDER_SYNC_QUEUES_ENABLED")
        assert sync_queue_for_provider("github") in task_queues


class TestInjectProviderToken:
    def test_linear_sets_env(self, monkeypatch):
        from dev_health_ops.workers.task_utils import _inject_provider_token

        monkeypatch.delenv("LINEAR_API_KEY", raising=False)
        _inject_provider_token("linear", "lin_api_test123")
        assert os.environ["LINEAR_API_KEY"] == "lin_api_test123"
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

    def test_github_sets_env(self, monkeypatch):
        from dev_health_ops.workers.task_utils import _inject_provider_token

        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        _inject_provider_token("github", "ghp_test123")
        assert os.environ["GITHUB_TOKEN"] == "ghp_test123"
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    def test_gitlab_sets_env(self, monkeypatch):
        from dev_health_ops.workers.task_utils import _inject_provider_token

        monkeypatch.delenv("GITLAB_TOKEN", raising=False)
        _inject_provider_token("gitlab", "glpat-test123")
        assert os.environ["GITLAB_TOKEN"] == "glpat-test123"
        monkeypatch.delenv("GITLAB_TOKEN", raising=False)

    def test_empty_token_does_not_set_env(self, monkeypatch):
        from dev_health_ops.workers.task_utils import _inject_provider_token

        monkeypatch.delenv("LINEAR_API_KEY", raising=False)
        _inject_provider_token("linear", "")
        assert "LINEAR_API_KEY" not in os.environ

    def test_unknown_provider_does_nothing(self, monkeypatch):
        from dev_health_ops.workers.task_utils import _inject_provider_token

        _inject_provider_token("unknown", "some_token")


class TestExtractProviderToken:
    def test_linear_apiKey(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert (
            _extract_provider_token("linear", {"apiKey": "lin_api_xxx"})
            == "lin_api_xxx"
        )

    def test_linear_api_key(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert (
            _extract_provider_token("linear", {"api_key": "lin_api_yyy"})
            == "lin_api_yyy"
        )

    def test_linear_prefers_api_key_over_apiKey(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        result = _extract_provider_token(
            "linear", {"api_key": "snake", "apiKey": "camel"}
        )
        assert result == "snake"

    def test_linear_empty_credentials(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert _extract_provider_token("linear", {}) == ""

    def test_github_uses_token(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert _extract_provider_token("github", {"token": "ghp_xxx"}) == "ghp_xxx"

    def test_gitlab_uses_token(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert _extract_provider_token("gitlab", {"token": "glpat-xxx"}) == "glpat-xxx"

    def test_jira_uses_api_token(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert _extract_provider_token("jira", {"api_token": "jira_xxx"}) == "jira_xxx"

    def test_jira_uses_apiToken(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert _extract_provider_token("jira", {"apiToken": "jira_yyy"}) == "jira_yyy"

    def test_unknown_provider_falls_back_to_token(self):
        from dev_health_ops.workers.task_utils import _extract_provider_token

        assert _extract_provider_token("unknown", {"token": "tok"}) == "tok"


class TestNormalizeCredentialKeys:
    def test_linear_apiKey_to_api_key(self):
        from dev_health_ops.api.services.configuration._helpers import (
            _normalize_credential_keys,
        )

        result = _normalize_credential_keys("linear", {"apiKey": "lin_api_xxx"})
        assert result == {"api_key": "lin_api_xxx"}

    def test_already_snake_case_unchanged(self):
        from dev_health_ops.api.services.configuration._helpers import (
            _normalize_credential_keys,
        )

        result = _normalize_credential_keys("linear", {"api_key": "lin_api_xxx"})
        assert result == {"api_key": "lin_api_xxx"}

    def test_jira_normalizes_multiple_keys(self):
        from dev_health_ops.api.services.configuration._helpers import (
            _normalize_credential_keys,
        )

        result = _normalize_credential_keys(
            "jira",
            {
                "apiToken": "tok",
                "baseUrl": "https://jira.example.com",
                "email": "a@b.com",
            },
        )
        assert result == {
            "api_token": "tok",
            "base_url": "https://jira.example.com",
            "email": "a@b.com",
        }

    def test_unknown_provider_no_change(self):
        from dev_health_ops.api.services.configuration._helpers import (
            _normalize_credential_keys,
        )

        creds = {"someKey": "val"}
        result = _normalize_credential_keys("unknown", creds)
        assert result == {"someKey": "val"}

    def test_github_normalizes_baseUrl(self):
        from dev_health_ops.api.services.configuration._helpers import (
            _normalize_credential_keys,
        )

        result = _normalize_credential_keys(
            "github", {"token": "ghp_xxx", "baseUrl": "https://gh.example.com"}
        )
        assert result == {"token": "ghp_xxx", "base_url": "https://gh.example.com"}


class TestCredentialMapping:
    """credential.config must be visible to the credential resolvers without
    ever shadowing decrypted secrets (CHAOS-2282 review follow-up)."""

    def test_config_only_values_are_exposed(self):
        from dev_health_ops.workers.task_utils import _credential_mapping

        credential = SimpleNamespace(
            credentials_encrypted=None,
            config={"url": "https://gitlab.example.com"},
        )
        assert _credential_mapping(credential) == {"url": "https://gitlab.example.com"}

    def test_decrypted_wins_on_key_collision(self):
        from dev_health_ops.workers.task_utils import _credential_mapping

        credential = SimpleNamespace(
            credentials_encrypted="ciphertext",
            config={"url": "https://config.example.com", "group": "my-group"},
        )
        with patch(
            "dev_health_ops.workers.task_utils._decrypt_credential_sync",
            return_value={
                "token": "glpat_secret",
                "url": "https://decrypted.example.com",
            },
        ):
            mapping = _credential_mapping(credential)

        assert mapping == {
            "token": "glpat_secret",
            "url": "https://decrypted.example.com",
            "group": "my-group",
        }

    def test_missing_or_invalid_config_returns_decrypted_only(self):
        from typing import Any

        from dev_health_ops.workers.task_utils import _credential_mapping

        configs: tuple[Any, ...] = (None, {}, "not-a-dict")
        for config in configs:
            credential = SimpleNamespace(credentials_encrypted=None, config=config)
            assert _credential_mapping(credential) == {}


class TestWorkItemsProviderCredentialIsolation:
    @pytest.mark.parametrize(
        ("provider", "source_kwargs", "message_fragment"),
        [
            ("github", {"repo_name": None}, "github work-item unit had no source"),
            ("linear", {"repo_name": "   "}, "linear work-item unit had no source"),
            ("gitlab", {"repo_name": ""}, "gitlab work-item unit had no source"),
            (
                "jira",
                {"jira_project_keys": ["   "]},
                "jira work-item unit had no source",
            ),
        ],
    )
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    def test_require_source_missing_source_fails_before_org_wide_discovery(
        self,
        mock_sink_class,
        provider,
        source_kwargs,
        message_fragment,
        caplog,
    ):
        from dev_health_ops.metrics.job_work_items import (
            WorkItemUnitMissingSource,
            run_work_items_sync_job,
        )

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink
        caplog.set_level("ERROR", logger="dev_health_ops.metrics.job_work_items")

        with patch("dev_health_ops.metrics.job_work_items._discover_repos") as discover:
            with pytest.raises(WorkItemUnitMissingSource, match=message_fragment):
                run_work_items_sync_job(
                    db_url="clickhouse://localhost/dev",
                    day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                    backfill_days=1,
                    provider=provider,
                    org_id="00000000-0000-0000-0000-000000000001",
                    require_source=True,
                    **source_kwargs,
                )

        discover.assert_not_called()
        assert message_fragment in caplog.text
        assert "refusing org-wide fan-out" in caplog.text

    @patch.dict(os.environ, {}, clear=True)
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    @patch("dev_health_ops.providers.linear.client.LinearClient.from_env")
    def test_linear_work_items_use_explicit_credentials_not_env(
        self, mock_from_env, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
        from dev_health_ops.providers.linear.client import LinearClient

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink

        captured_clients: list[object] = []

        def _fake_iter_ingest(self, ctx):
            client = self._make_client()
            captured_clients.append(client)
            return iter(())

        with patch(
            "dev_health_ops.providers.linear.provider.LinearProvider.iter_ingest",
            new=_fake_iter_ingest,
        ):
            run_work_items_sync_job(
                db_url="clickhouse://localhost/dev",
                day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                backfill_days=1,
                provider="linear",
                org_id="00000000-0000-0000-0000-000000000001",
                credentials={"api_key": "lin_explicit"},
            )

        mock_from_env.assert_not_called()
        assert captured_clients
        assert isinstance(captured_clients[0], LinearClient)
        assert captured_clients[0].auth.api_key == "lin_explicit"

    @patch.dict(os.environ, {}, clear=True)
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    @patch("dev_health_ops.providers.linear.client.LinearClient.from_env")
    def test_linear_work_items_empty_source_raises(
        self, mock_from_env, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink

        with pytest.raises(ValueError, match="non-empty source team key"):
            run_work_items_sync_job(
                db_url="clickhouse://localhost/dev",
                day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                backfill_days=1,
                provider="linear",
                org_id="00000000-0000-0000-0000-000000000001",
                credentials={"api_key": "lin_explicit"},
                repo_name="   ",
            )

    @patch.dict(os.environ, {}, clear=True)
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    def test_linear_work_items_without_required_source_keeps_org_wide_path(
        self, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
        from dev_health_ops.providers.linear.client import LinearClient

        sink = MagicMock()
        sink.query_dicts.return_value = []
        sink.client.query.return_value = SimpleNamespace(result_rows=[])
        mock_sink_class.return_value = sink

        captured_repos: list[object] = []

        def _fake_iter_ingest(self, ctx):
            client = self._make_client()
            assert isinstance(client, LinearClient)
            captured_repos.append(ctx.repo)
            return iter(())

        with patch(
            "dev_health_ops.providers.linear.provider.LinearProvider.iter_ingest",
            new=_fake_iter_ingest,
        ):
            run_work_items_sync_job(
                db_url="clickhouse://localhost/dev",
                day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                backfill_days=1,
                provider="linear",
                org_id="00000000-0000-0000-0000-000000000001",
                credentials={"api_key": "lin_explicit"},
            )

        assert captured_repos == [None]

    @patch.dict(os.environ, {}, clear=True)
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    @patch("dev_health_ops.providers.linear.client.LinearClient.from_env")
    def test_linear_work_items_threads_source_into_ingestion_repo(
        self, mock_from_env, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink

        captured_repos: list[object] = []

        def _fake_iter_ingest(self, ctx):
            captured_repos.append(ctx.repo)
            return iter(())

        with patch(
            "dev_health_ops.providers.linear.provider.LinearProvider.iter_ingest",
            new=_fake_iter_ingest,
        ):
            run_work_items_sync_job(
                db_url="clickhouse://localhost/dev",
                day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                backfill_days=1,
                provider="linear",
                org_id="00000000-0000-0000-0000-000000000001",
                credentials={"api_key": "lin_explicit"},
                repo_name="ENG",
            )

        assert captured_repos == ["ENG"]

    @patch.dict(os.environ, {}, clear=True)
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    @patch("dev_health_ops.providers.linear.client.LinearClient.from_env")
    def test_linear_work_items_org_wide_placeholder_reaches_unscoped_ingest(
        self, mock_from_env, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink

        captured_repos: list[object] = []

        def _fake_iter_ingest(self, ctx):
            captured_repos.append(ctx.repo)
            return iter(())

        with patch(
            "dev_health_ops.providers.linear.provider.LinearProvider.iter_ingest",
            new=_fake_iter_ingest,
        ):
            run_work_items_sync_job(
                db_url="clickhouse://localhost/dev",
                day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                backfill_days=1,
                provider="linear",
                org_id="00000000-0000-0000-0000-000000000001",
                credentials={"api_key": "lin_explicit"},
                repo_name=None,
                require_source=False,
            )

        assert captured_repos == [None]

    @patch.dict(os.environ, {}, clear=True)
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    @patch("dev_health_ops.providers.linear.client.LinearClient.from_env")
    def test_linear_work_items_scoped_provider_name_still_fails_visibly(
        self, mock_from_env, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink

        captured_repos: list[object] = []

        def _fake_iter_ingest(self, ctx):
            captured_repos.append(ctx.repo)
            raise ValueError("Linear team 'linear' not found")

        with patch(
            "dev_health_ops.providers.linear.provider.LinearProvider.iter_ingest",
            new=_fake_iter_ingest,
        ):
            with pytest.raises(ValueError, match="Linear team 'linear' not found"):
                run_work_items_sync_job(
                    db_url="clickhouse://localhost/dev",
                    day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                    backfill_days=1,
                    provider="linear",
                    org_id="00000000-0000-0000-0000-000000000001",
                    credentials={"api_key": "lin_explicit"},
                    repo_name="linear",
                    require_source=True,
                )

        assert captured_repos == ["linear"]

    @patch.dict(
        os.environ,
        {"JIRA_JQL": "project = ENV", "JIRA_PROJECT_KEYS": "ENV"},
        clear=True,
    )
    @patch("dev_health_ops.metrics.job_work_items.ClickHouseMetricsSink")
    @patch("dev_health_ops.providers.jira.client.JiraClient.from_env")
    def test_jira_work_items_use_explicit_credentials_not_env(
        self, mock_from_env, mock_sink_class
    ):
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        sink = MagicMock()
        sink.query_dicts.return_value = []
        mock_sink_class.return_value = sink

        captured_clients: list[object] = []
        captured_jqls: list[str] = []

        class FakeJiraClient:
            def iter_issues(self, **kwargs):
                captured_clients.append(self)
                captured_jqls.append(kwargs["jql"])
                return iter(())

            def close(self):
                pass

        with patch(
            "dev_health_ops.providers.jira.client.JiraClient",
            return_value=FakeJiraClient(),
        ) as mock_jira_client:
            run_work_items_sync_job(
                db_url="clickhouse://localhost/dev",
                day=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                backfill_days=1,
                provider="jira",
                org_id="00000000-0000-0000-0000-000000000001",
                credentials={
                    "base_url": "https://tenant.atlassian.net",
                    "email": "tenant@example.com",
                    "api_token": "jira_explicit",
                },
            )

        mock_from_env.assert_not_called()
        assert captured_clients
        kwargs = mock_jira_client.call_args.kwargs
        assert isinstance(captured_clients[0], FakeJiraClient)
        assert captured_jqls
        assert all("ENV" not in jql for jql in captured_jqls)
        assert kwargs["auth"].base_url == "https://tenant.atlassian.net"
        assert kwargs["auth"].email == "tenant@example.com"
        assert kwargs["auth"].api_token == "jira_explicit"
