from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestIsBatchEligible:
    def test_github_with_wildcard_search(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"search": "my-org/*"},
        )
        assert _is_batch_eligible(config) is True

    def test_github_with_question_mark_wildcard(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"search": "my-org/repo-?"},
        )
        assert _is_batch_eligible(config) is True

    def test_github_with_discover_flag(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"discover": True, "owner": "my-org"},
        )
        assert _is_batch_eligible(config) is True

    def test_github_with_all_repos_flag(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"all_repos": True},
        )
        assert _is_batch_eligible(config) is True

    def test_github_all_repos_with_concrete_repo_not_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"all_repos": True, "owner": "org", "repo": "api"},
        )
        assert _is_batch_eligible(config) is False

    def test_github_bare_org_search_is_batch_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(provider="github", sync_options={"search": "full-chaos"})
        assert _is_batch_eligible(config) is True

    def test_github_owner_without_repo_is_batch_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(provider="github", sync_options={"owner": "full-chaos"})
        assert _is_batch_eligible(config) is True

    def test_github_owner_repo_remains_single_config(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"search": "full-chaos/dev-health"},
        )
        assert _is_batch_eligible(config) is False

    def test_github_without_wildcard_not_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "specific-repo"},
        )
        assert _is_batch_eligible(config) is False

    def test_gitlab_with_wildcard_search(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
        )
        assert _is_batch_eligible(config) is True

    def test_gitlab_with_all_repos_flag(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="gitlab",
            sync_options={"all_repos": True},
        )
        assert _is_batch_eligible(config) is True

    def test_gitlab_all_repos_with_concrete_project_not_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        for sync_options in (
            {"all_repos": True, "project_id": 100},
            {"all_repos": True, "project": "api"},
            {"all_repos": True, "repo": "api"},
        ):
            config = _make_config(provider="gitlab", sync_options=sync_options)
            assert _is_batch_eligible(config) is False

    def test_jira_never_batch_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="jira",
            sync_options={"search": "project/*"},
        )
        assert _is_batch_eligible(config) is False

    def test_local_provider_not_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="local",
            sync_options={"search": "org/*"},
        )
        assert _is_batch_eligible(config) is False

    def test_discover_false_not_eligible(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"discover": False},
        )
        assert _is_batch_eligible(config) is False


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
    @patch("dev_health_ops.connectors.utils.github_app.GitHubAppTokenProvider")
    def test_github_app_all_repos_lists_installation_repositories(
        self, mock_token_provider_cls, mock_get
    ):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_token_provider_cls.return_value.get_token.return_value = (
            "installation-token"
        )
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
        mock_token_provider_cls.assert_called_once_with(
            app_id="123",
            private_key="private-key",
            installation_id="456",
            api_base_url="https://api.github.test",
        )
        mock_get.assert_called_once()
        assert mock_get.call_args.args[0] == (
            "https://api.github.test/installation/repositories"
        )
        assert mock_get.call_args.kwargs["headers"]["Authorization"] == (
            "Bearer installation-token"
        )

    @patch("requests.get")
    @patch("dev_health_ops.connectors.utils.github_app.GitHubAppTokenProvider")
    def test_github_app_all_repos_installation_api_failure_raises(
        self, mock_token_provider_cls, mock_get
    ):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_token_provider_cls.return_value.get_token.return_value = (
            "installation-token"
        )
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

    @patch("dev_health_ops.discovery.repos.discover_gitlab_repos")
    def test_gitlab_delegates_to_gitlab_discovery(self, mock_gl):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_gl.return_value = [("123",), ("456",)]
        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
        )
        result = discover_repos_for_config(config, {"token": "glpat_test"})
        assert result == [("123",), ("456",)]
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
        assert result == [("100",)]

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
        assert result == [("100",), ("200",)]

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
            SimpleNamespace(name="api", id=100),
            SimpleNamespace(name="web", id=200),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos({"all_repos": True, "search": ""}, "token")

        mock_gitlab_cls.return_value.projects.list.assert_called_once_with(
            all=True, membership=True
        )
        mock_gitlab_cls.return_value.groups.get.assert_not_called()
        assert result == [("100",), ("200",)]

    @patch("gitlab.Gitlab")
    def test_all_repos_filters_membership_projects(self, mock_gitlab_cls):
        from dev_health_ops.discovery.repos import discover_gitlab_repos

        projects = [
            SimpleNamespace(name="api", id=100),
            SimpleNamespace(name="web", id=200),
            SimpleNamespace(name="api-worker", id=300),
        ]
        mock_gitlab_cls.return_value.projects.list.return_value = projects

        result = discover_gitlab_repos({"all_repos": True, "search": "api*"}, "token")

        assert result == [("100",), ("300",)]

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

        assert result == [("100",), ("200",)]

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

        assert result == [("100",), ("200",)]

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


class TestGetBatchSize:
    def test_from_sync_options(self):
        from dev_health_ops.workers.sync_batch import _get_batch_size

        assert _get_batch_size({"batch_size": 10}) == 10

    def test_from_env_var(self, monkeypatch):
        from dev_health_ops.workers.sync_batch import _get_batch_size

        monkeypatch.setenv("SYNC_BATCH_SIZE", "8")
        assert _get_batch_size({}) == 8

    def test_default_is_five(self, monkeypatch):
        from dev_health_ops.workers.sync_batch import _get_batch_size

        monkeypatch.delenv("SYNC_BATCH_SIZE", raising=False)
        assert _get_batch_size({}) == 5

    def test_sync_options_takes_precedence_over_env(self, monkeypatch):
        from dev_health_ops.workers.sync_batch import _get_batch_size

        monkeypatch.setenv("SYNC_BATCH_SIZE", "8")
        assert _get_batch_size({"batch_size": 3}) == 3


class TestDispatchBatchSync:
    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_dispatches_correct_number_of_child_tasks(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(
            provider="github",
            sync_options={"search": "org/*"},
            sync_targets=["git", "prs"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [
            ("org", "repo-1"),
            ("org", "repo-2"),
            ("org", "repo-3"),
        ]

        mock_chord_instance = MagicMock()
        mock_chord.return_value = mock_chord_instance

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-1")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        assert result["total_repos"] == 3
        mock_chord.assert_called_once()
        mock_chord_instance.assert_called_once()

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_empty_repo_list_returns_no_repos(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(
            provider="github",
            sync_options={"search": "org/nonexistent-*"},
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = []

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-2")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "no_repos"
        assert result["total_repos"] == 0
        mock_chord.assert_not_called()

    @patch("dev_health_ops.workers.sync_batch.run_sync_config")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_discovery_failure_falls_back_to_single_dispatch(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_run_sync,
        db_session,
        monkeypatch,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        config = _make_config(
            provider="github",
            sync_options={"search": "org/*"},
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.side_effect = RuntimeError("API rate limited")

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-3")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "fallback_single"
        assert "API rate limited" in result["reason"]
        mock_run_sync.apply_async.assert_called_once()
        # CHAOS-2299: the fallback dispatch stays on the provider's queue.
        assert mock_run_sync.apply_async.call_args.kwargs["queue"] == "sync.github"

    @patch("dev_health_ops.workers.sync_batch.run_sync_config")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_all_repos_discovery_failure_marks_error_without_fallback(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_run_sync,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(
            provider="github",
            sync_options={"all_repos": True, "search": "org/*"},
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.side_effect = RuntimeError("API rate limited")

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-all-repos-failure")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "error"
        assert "API rate limited" in result["error"]
        mock_run_sync.apply_async.assert_not_called()

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_chord_callback_is_batch_sync_callback(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.tasks import (
            _batch_sync_callback,
            dispatch_batch_sync,
        )

        config = _make_config(
            provider="github",
            sync_options={"search": "org/*"},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", "repo-1")]

        mock_chord_instance = MagicMock()
        mock_chord.return_value = mock_chord_instance

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-4")
        try:
            task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        args, kwargs = mock_chord.call_args
        callback = args[1]
        assert callback.task == _batch_sync_callback.name

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_respects_custom_batch_size(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(
            provider="github",
            sync_options={"search": "org/*", "batch_size": 2},
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", f"repo-{i}") for i in range(7)]

        mock_chord_instance = MagicMock()
        mock_chord.return_value = mock_chord_instance

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-5")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["total_repos"] == 7
        assert result["batch_count"] == 4

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_staggers_batches_with_countdown(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import (
            BATCH_STAGGER_SECONDS,
            _batch_sync_callback,
            dispatch_batch_sync,
        )

        config = _make_config(
            provider="github",
            sync_options={"search": "org/*", "batch_size": 2},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", f"repo-{i}") for i in range(5)]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-stagger")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        assert result["batch_count"] == 3

        args, _ = mock_chord.call_args
        header_group, callback = args[0], args[1]
        signatures = list(header_group.tasks)

        # Total dispatched count unchanged: one task per repo.
        assert len(signatures) == 5

        # Batch 0 starts immediately (no countdown); batch N is staggered.
        expected_countdowns = [
            None,
            None,
            BATCH_STAGGER_SECONDS,
            BATCH_STAGGER_SECONDS,
            2 * BATCH_STAGGER_SECONDS,
        ]
        actual_countdowns = [sig.options.get("countdown") for sig in signatures]
        assert actual_countdowns == expected_countdowns

        # Chord callback still attached with unchanged kwargs (config_id
        # added by CHAOS-2267/#852 so the callback can stamp last_sync_*).
        assert callback.task == _batch_sync_callback.name
        assert callback.kwargs == {
            "provider": "github",
            "sync_targets": ["git"],
            "org_id": "default",
            "run_id": None,
            "config_id": str(config.id),
        }

    @patch("dev_health_ops.workers.sync_batch._run_sync_for_repo")
    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_github_child_work_items_search_is_scoped_to_repo(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        mock_run_for_repo,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        captured_kwargs = []

        def _capture_signature(**kwargs):
            captured_kwargs.append(kwargs)
            return MagicMock()

        mock_run_for_repo.s.side_effect = _capture_signature
        config = _make_config(
            provider="github",
            sync_options={"search": "org/*", "all_repos": True},
            sync_targets=["git", "work-items"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", "repo-1")]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-work-items")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        assert captured_kwargs[0]["sync_options_override"]["search"] == "org/repo-1"
        assert "all_repos" not in captured_kwargs[0]["sync_options_override"]


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


def _run_child_sync_task(
    db_session,
    *,
    provider: str,
    sync_options_override: dict,
    credentials: dict,
    sync_targets: list[str],
    processor_path: str,
):
    """Execute _run_sync_for_repo with the store/processor layers mocked out.

    Returns (task result, processor AsyncMock) so tests can assert on the
    kwargs forwarded to process_github_repo / process_gitlab_project.
    """
    import uuid as _uuid

    import dev_health_ops.connectors  # noqa: F401  (avoid circular-import on first load)
    import dev_health_ops.processors.github  # noqa: F401  (make patch targets resolvable)
    import dev_health_ops.processors.gitlab  # noqa: F401
    from dev_health_ops.workers.sync_batch import _run_sync_for_repo

    async def _fake_run_with_store(db_url, db_type, handler, org_id=None):
        await handler(MagicMock())

    processor_mock = AsyncMock()
    with (
        patch(
            "dev_health_ops.workers.sync_batch._get_db_url",
            return_value="clickhouse://localhost/dev",
        ),
        patch("dev_health_ops.storage.resolve_db_type", return_value="clickhouse"),
        patch("dev_health_ops.storage.run_with_store", new=_fake_run_with_store),
        patch(processor_path, processor_mock),
        patch(
            "dev_health_ops.db.get_postgres_session_sync",
            side_effect=lambda *a, **k: _fake_session_ctx(db_session),
        ),
    ):
        task = _run_sync_for_repo
        task.push_request(id=f"child-{_uuid.uuid4()}", retries=0)
        try:
            result = task(
                config_id=str(_uuid.uuid4()),
                org_id="default",
                triggered_by="manual",
                provider=provider,
                sync_targets=sync_targets,
                sync_options_override=sync_options_override,
                credentials=credentials,
                config_name="batch-config",
            )
        finally:
            task.pop_request()
    return result, processor_mock


def _seed_watermarks(db_session, repo_id: str, targets_to_ts: dict[str, datetime]):
    from dev_health_ops.models.settings import SyncWatermark

    for target, ts in targets_to_ts.items():
        db_session.add(
            SyncWatermark(
                org_id="default",
                repo_id=repo_id,
                target=target,
                last_synced_at=ts,
            )
        )
    db_session.flush()


class TestGitLabDispatchBatchSync:
    """GitLab batch dispatch mirrors GitHub: child kwargs, gitlab_url threading,
    watermark read/stamp (CHAOS-2281), and credential URL resolution (CHAOS-2282).
    """

    @patch("dev_health_ops.workers.sync_batch._run_sync_for_repo")
    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_dispatches_per_project_children_with_gitlab_url_threading(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        mock_run_for_repo,
        db_session,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        captured_kwargs = []

        def _capture_signature(**kwargs):
            captured_kwargs.append(kwargs)
            return MagicMock()

        mock_run_for_repo.s.side_effect = _capture_signature
        config = _make_config(
            provider="gitlab",
            sync_options={
                "search": "my-group/*",
                "all_repos": True,
                "gitlab_url": "https://gitlab.example.com",
            },
            sync_targets=["git", "prs"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "glpat_test"}
        mock_discover.return_value = [("100",), ("200",)]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="gl-batch-dispatch-1")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        assert result["total_repos"] == 2
        assert len(captured_kwargs) == 2

        first = captured_kwargs[0]
        assert first["provider"] == "gitlab"
        assert first["sync_targets"] == ["git", "prs"]
        assert first["credentials"] == {"token": "glpat_test"}

        override = first["sync_options_override"]
        assert override["project_id"] == 100
        assert override["gitlab_url"] == "https://gitlab.example.com"
        assert "all_repos" not in override
        assert "search" not in override
        assert "group" not in override

        assert captured_kwargs[1]["sync_options_override"]["project_id"] == 200

    @patch("dev_health_ops.discovery.repos.discover_gitlab_repos")
    def test_discovery_resolves_gitlab_url_from_credentials(self, mock_gl):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_gl.return_value = [("100",)]
        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
        )
        result = discover_repos_for_config(
            config, {"token": "glpat_test", "url": "https://gl.corp.internal"}
        )
        assert result == [("100",)]
        args, kwargs = mock_gl.call_args
        assert args[1] == "glpat_test"
        assert kwargs["gitlab_url"] == "https://gl.corp.internal"

    @patch("dev_health_ops.discovery.repos.discover_gitlab_repos")
    def test_discovery_sync_options_url_beats_credential_url(self, mock_gl):
        from dev_health_ops.discovery.repos import discover_repos_for_config

        mock_gl.return_value = []
        config = _make_config(
            provider="gitlab",
            sync_options={
                "search": "my-group/*",
                "gitlab_url": "https://opt.example.com",
            },
        )
        discover_repos_for_config(
            config, {"token": "glpat_test", "url": "https://cred.example.com"}
        )
        assert mock_gl.call_args.kwargs["gitlab_url"] == "https://opt.example.com"

    def test_batch_eligibility_unchanged_for_gitlab(self):
        from dev_health_ops.workers.sync_batch import _is_batch_eligible

        eligible = _make_config(
            provider="gitlab",
            sync_options={
                "group": "my-group",
                "gitlab_url": "https://gitlab.example.com",
            },
        )
        assert _is_batch_eligible(eligible) is True

        single = _make_config(
            provider="gitlab",
            sync_options={
                "group": "my-group",
                "project_id": 100,
                "gitlab_url": "https://gitlab.example.com",
            },
        )
        assert _is_batch_eligible(single) is False


class TestRunSyncForRepoWatermarks:
    """Batch children must honour and stamp sync watermarks (CHAOS-2281),
    mirroring run_sync_config: since=min(watermarks) only when every target
    has one, no since on partial coverage or full_resync, stamp on success.
    """

    GITLAB_PROCESSOR = "dev_health_ops.processors.gitlab.process_gitlab_project"
    GITHUB_PROCESSOR = "dev_health_ops.processors.github.process_github_repo"

    def test_gitlab_child_passes_min_watermark_as_since(self, db_session):
        older = datetime(2026, 1, 1, 12, 0, 0)
        newer = datetime(2026, 1, 5, 12, 0, 0)
        _seed_watermarks(db_session, "100", {"git": newer, "prs": older})

        result, processor = _run_child_sync_task(
            db_session,
            provider="gitlab",
            sync_options_override={"project_id": 100},
            credentials={"token": "glpat_test"},
            sync_targets=["git", "prs"],
            processor_path=self.GITLAB_PROCESSOR,
        )

        assert result["status"] == "success"
        since = processor.await_args.kwargs["since"]
        assert since is not None
        assert since.replace(tzinfo=None) == older

    def test_gitlab_child_partial_watermarks_means_no_since(self, db_session):
        _seed_watermarks(db_session, "100", {"git": datetime(2026, 1, 1)})

        result, processor = _run_child_sync_task(
            db_session,
            provider="gitlab",
            sync_options_override={"project_id": 100},
            credentials={"token": "glpat_test"},
            sync_targets=["git", "prs"],
            processor_path=self.GITLAB_PROCESSOR,
        )

        assert result["status"] == "success"
        assert processor.await_args.kwargs["since"] is None

    def test_gitlab_child_full_resync_ignores_watermarks(self, db_session):
        ts = datetime(2026, 1, 1)
        _seed_watermarks(db_session, "100", {"git": ts, "prs": ts})

        result, processor = _run_child_sync_task(
            db_session,
            provider="gitlab",
            sync_options_override={"project_id": 100, "full_resync": True},
            credentials={"token": "glpat_test"},
            sync_targets=["git", "prs"],
            processor_path=self.GITLAB_PROCESSOR,
        )

        assert result["status"] == "success"
        assert processor.await_args.kwargs["since"] is None

    def test_gitlab_child_stamps_watermarks_on_success(self, db_session):
        from dev_health_ops.models.settings import SyncWatermark

        result, _ = _run_child_sync_task(
            db_session,
            provider="gitlab",
            sync_options_override={"project_id": 100},
            credentials={"token": "glpat_test"},
            sync_targets=["git", "prs"],
            processor_path=self.GITLAB_PROCESSOR,
        )

        assert result["status"] == "success"
        rows = (
            db_session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == "default",
                SyncWatermark.repo_id == "100",
            )
            .all()
        )
        assert {row.target for row in rows} == {"git", "prs"}
        assert all(row.last_synced_at is not None for row in rows)

    def test_gitlab_child_failure_does_not_stamp_watermarks(self, db_session):
        import uuid as _uuid

        from dev_health_ops.models.settings import SyncWatermark
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        with (
            patch(
                "dev_health_ops.workers.sync_batch._get_db_url",
                return_value="clickhouse://localhost/dev",
            ),
            patch("dev_health_ops.storage.resolve_db_type", return_value="clickhouse"),
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                side_effect=lambda *a, **k: _fake_session_ctx(db_session),
            ),
        ):
            task = _run_sync_for_repo
            task.push_request(id=f"child-{_uuid.uuid4()}", retries=0)
            try:
                # Missing token => ValueError inside the gitlab branch.
                with pytest.raises(Exception):
                    task(
                        config_id=str(_uuid.uuid4()),
                        org_id="default",
                        triggered_by="manual",
                        provider="gitlab",
                        sync_targets=["git"],
                        sync_options_override={"project_id": 100},
                        credentials={},
                        config_name="batch-config",
                    )
            finally:
                task.pop_request()

        assert db_session.query(SyncWatermark).count() == 0

    def test_gitlab_child_resolves_url_from_credentials(self, db_session):
        result, processor = _run_child_sync_task(
            db_session,
            provider="gitlab",
            sync_options_override={"project_id": 100},
            credentials={"token": "glpat_test", "url": "https://gl.corp.internal"},
            sync_targets=["git"],
            processor_path=self.GITLAB_PROCESSOR,
        )

        assert result["status"] == "success"
        assert processor.await_args.kwargs["gitlab_url"] == "https://gl.corp.internal"
        assert result["result"]["gitlab_url"] == "https://gl.corp.internal"

    def test_gitlab_child_sync_options_url_beats_credential_url(self, db_session):
        result, processor = _run_child_sync_task(
            db_session,
            provider="gitlab",
            sync_options_override={
                "project_id": 100,
                "gitlab_url": "https://opt.example.com",
            },
            credentials={"token": "glpat_test", "url": "https://cred.example.com"},
            sync_targets=["git"],
            processor_path=self.GITLAB_PROCESSOR,
        )

        assert result["status"] == "success"
        assert processor.await_args.kwargs["gitlab_url"] == "https://opt.example.com"

    def test_github_child_passes_since_and_stamps_watermarks(self, db_session):
        from dev_health_ops.models.settings import SyncWatermark

        older = datetime(2026, 2, 1)
        newer = datetime(2026, 2, 3)
        _seed_watermarks(db_session, "org/repo-1", {"git": older, "prs": newer})

        result, processor = _run_child_sync_task(
            db_session,
            provider="github",
            sync_options_override={"owner": "org", "repo": "repo-1"},
            credentials={"token": "ghp_test"},
            sync_targets=["git", "prs"],
            processor_path=self.GITHUB_PROCESSOR,
        )

        assert result["status"] == "success"
        since = processor.await_args.kwargs["since"]
        assert since is not None
        assert since.replace(tzinfo=None) == older

        rows = (
            db_session.query(SyncWatermark)
            .filter(SyncWatermark.repo_id == "org/repo-1")
            .all()
        )
        assert {row.target for row in rows} == {"git", "prs"}
        # Stamped at task start time: strictly newer than the seeded values.
        assert all(row.last_synced_at.replace(tzinfo=None) > newer for row in rows)


class TestBatchSyncCallback:
    @patch("dev_health_ops.workers.sync_batch._dispatch_post_sync_tasks")
    def test_fires_post_sync_tasks(self, mock_post_sync):
        from dev_health_ops.workers.sync_batch import _batch_sync_callback

        task = _batch_sync_callback
        task.push_request(id="callback-1")
        try:
            result = task(
                [{"status": "success"}, {"status": "success"}],
                provider="github",
                sync_targets=["git", "prs"],
                org_id="default",
            )
        finally:
            task.pop_request()

        assert result["status"] == "post_sync_dispatched"
        assert result["child_results"] == 2
        mock_post_sync.assert_called_once_with(
            provider="github",
            sync_targets=["git", "prs"],
            org_id="default",
        )


class TestBatchRunStateManagement:
    """dispatch_batch_sync must manage the pending JobRun state (CHAOS-2267).

    A PENDING JobRun is persisted at Sync Now trigger time (CHAOS-2255) and
    passed via pending_run_id. Every path through dispatch_batch_sync must
    leave that run in a sensible state instead of stuck PENDING forever.
    """

    @staticmethod
    def _make_pending_run(db_session):
        import uuid as _uuid

        from dev_health_ops.models.settings import JobRun

        run = JobRun(job_id=_uuid.uuid4(), triggered_by="manual")
        db_session.add(run)
        db_session.flush()
        return run

    @staticmethod
    def _sessions(mock_get_session, db_session):
        """Each call to get_postgres_session_sync gets a fresh context."""
        mock_get_session.side_effect = lambda *a, **k: _fake_session_ctx(db_session)

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_marks_pending_run_running_on_dispatch(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.models.settings import JobRunStatus
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(provider="github", sync_options={"search": "org/*"})
        db_session.add(config)
        run = self._make_pending_run(db_session)
        self._sessions(mock_get_session, db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", "repo-1")]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="batch-run-state-1")
        try:
            result = task(
                config_id=str(config.id),
                org_id="default",
                pending_run_id=str(run.id),
            )
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        db_session.refresh(run)
        assert run.status == JobRunStatus.RUNNING.value
        assert run.started_at is not None

        # The chord callback must receive both run_id and config_id so it can
        # resolve the run and stamp the config on success.
        callback = mock_chord.call_args[0][1]
        assert callback.kwargs["run_id"] == str(run.id)
        assert callback.kwargs["config_id"] == str(config.id)

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_no_repos_resolves_run_success_and_stamps_config(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.models.settings import JobRunStatus
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(
            provider="github", sync_options={"search": "org/nonexistent-*"}
        )
        db_session.add(config)
        run = self._make_pending_run(db_session)
        self._sessions(mock_get_session, db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = []

        task = dispatch_batch_sync
        task.push_request(id="batch-run-state-2")
        try:
            result = task(
                config_id=str(config.id),
                org_id="default",
                pending_run_id=str(run.id),
            )
        finally:
            task.pop_request()

        assert result["status"] == "no_repos"
        mock_chord.assert_not_called()
        db_session.refresh(run)
        assert run.status == JobRunStatus.SUCCESS.value
        assert run.completed_at is not None
        assert run.result == {"child_results": 0}
        db_session.refresh(config)
        assert config.last_sync_at is not None
        assert config.last_sync_success is True
        assert config.last_sync_error is None

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_exception_marks_run_failed_and_stamps_config(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.models.settings import JobRunStatus
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(provider="github", sync_options={"search": "org/*"})
        db_session.add(config)
        run = self._make_pending_run(db_session)
        self._sessions(mock_get_session, db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", "repo-1")]
        mock_chord.side_effect = RuntimeError("broker unavailable")

        task = dispatch_batch_sync
        task.push_request(id="batch-run-state-3")
        try:
            result = task(
                config_id=str(config.id),
                org_id="default",
                pending_run_id=str(run.id),
            )
        finally:
            task.pop_request()

        assert result["status"] == "error"
        db_session.refresh(run)
        assert run.status == JobRunStatus.FAILED.value
        assert run.completed_at is not None
        assert "broker unavailable" in (run.error or "")
        db_session.refresh(config)
        assert config.last_sync_at is not None
        assert config.last_sync_success is False
        assert "broker unavailable" in (config.last_sync_error or "")

    @patch("dev_health_ops.workers.sync_batch.run_sync_config")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_fallback_forwards_pending_run_id_without_resolving(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_run_sync,
        db_session,
    ):
        from dev_health_ops.models.settings import JobRunStatus
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        config = _make_config(provider="github", sync_options={"search": "org/*"})
        db_session.add(config)
        run = self._make_pending_run(db_session)
        self._sessions(mock_get_session, db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.side_effect = RuntimeError("API rate limited")

        task = dispatch_batch_sync
        task.push_request(id="batch-run-state-4")
        try:
            result = task(
                config_id=str(config.id),
                org_id="default",
                pending_run_id=str(run.id),
            )
        finally:
            task.pop_request()

        assert result["status"] == "fallback_single"
        call_kwargs = mock_run_sync.apply_async.call_args.kwargs["kwargs"]
        assert call_kwargs["pending_run_id"] == str(run.id)
        # The run must NOT be resolved here: run_sync_config owns it from now
        # on (it tolerates an already-RUNNING run and re-marks it RUNNING).
        db_session.refresh(run)
        assert run.status == JobRunStatus.RUNNING.value
        assert run.completed_at is None

    @patch("dev_health_ops.workers.sync_batch._dispatch_post_sync_tasks")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_callback_marks_run_success_and_stamps_config(
        self, mock_get_session, mock_post_sync, db_session
    ):
        from dev_health_ops.models.settings import JobRunStatus
        from dev_health_ops.workers.sync_batch import _batch_sync_callback

        config = _make_config(provider="github", sync_options={"search": "org/*"})
        db_session.add(config)
        run = self._make_pending_run(db_session)
        self._sessions(mock_get_session, db_session)

        task = _batch_sync_callback
        task.push_request(id="batch-run-state-5")
        try:
            result = task(
                [{"status": "success"}, {"status": "success"}],
                provider="github",
                sync_targets=["git"],
                org_id="default",
                run_id=str(run.id),
                config_id=str(config.id),
            )
        finally:
            task.pop_request()

        assert result["status"] == "post_sync_dispatched"
        db_session.refresh(run)
        assert run.status == JobRunStatus.SUCCESS.value
        assert run.completed_at is not None
        assert run.result == {"child_results": 2}
        db_session.refresh(config)
        assert config.last_sync_at is not None
        assert config.last_sync_success is True
        assert config.last_sync_stats == {"child_results": 2}


def _setup_croniter_mock(monkeypatch: pytest.MonkeyPatch):
    mock_cron_instance = MagicMock()
    mock_cron_instance.get_next.return_value = datetime(2000, 1, 1, tzinfo=timezone.utc)
    mock_cron_cls = MagicMock(return_value=mock_cron_instance)
    monkeypatch.setitem(
        sys.modules, "croniter", SimpleNamespace(croniter=mock_cron_cls)
    )
    return mock_cron_cls


class TestDispatchScheduledSyncsRouting:
    @patch("dev_health_ops.workers.sync_scheduler.dispatch_batch_sync")
    @patch("dev_health_ops.workers.sync_scheduler.run_sync_config")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_routes_batch_eligible_to_dispatch_batch_sync(
        self, mock_get_session, mock_run_sync, mock_batch_sync, db_session, monkeypatch
    ):
        from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

        _setup_croniter_mock(monkeypatch)

        config = _make_config(
            provider="github",
            sync_options={"search": "org/*", "schedule_cron": "0 * * * *"},
            name="batch-config",
        )
        config.last_sync_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)

        task = dispatch_scheduled_syncs
        task.push_request(id="sched-1")
        try:
            result = task()
        finally:
            task.pop_request()

        assert str(config.id) in result["dispatched"]
        mock_batch_sync.apply_async.assert_called_once()
        mock_run_sync.apply_async.assert_not_called()

    @patch("dev_health_ops.workers.sync_scheduler.dispatch_batch_sync")
    @patch("dev_health_ops.workers.sync_scheduler.run_sync_config")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_routes_normal_config_to_run_sync_config(
        self, mock_get_session, mock_run_sync, mock_batch_sync, db_session, monkeypatch
    ):
        from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

        _setup_croniter_mock(monkeypatch)

        config = _make_config(
            provider="github",
            sync_options={
                "owner": "org",
                "repo": "specific-repo",
                "schedule_cron": "0 * * * *",
            },
            name="normal-config",
        )
        config.last_sync_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)

        task = dispatch_scheduled_syncs
        task.push_request(id="sched-2")
        try:
            result = task()
        finally:
            task.pop_request()

        assert str(config.id) in result["dispatched"]
        mock_run_sync.apply_async.assert_called_once()
        mock_batch_sync.apply_async.assert_not_called()

    @patch("dev_health_ops.workers.sync_scheduler.dispatch_batch_sync")
    @patch("dev_health_ops.workers.sync_scheduler.run_sync_config")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_mixed_configs_route_correctly(
        self, mock_get_session, mock_run_sync, mock_batch_sync, db_session, monkeypatch
    ):
        from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

        _setup_croniter_mock(monkeypatch)

        batch_config = _make_config(
            provider="github",
            sync_options={"search": "org/*", "schedule_cron": "0 * * * *"},
            name="batch",
        )
        batch_config.last_sync_at = datetime(2020, 1, 1, tzinfo=timezone.utc)

        normal_config = _make_config(
            provider="github",
            sync_options={
                "owner": "org",
                "repo": "repo",
                "schedule_cron": "0 * * * *",
            },
            name="normal",
        )
        normal_config.last_sync_at = datetime(2020, 1, 1, tzinfo=timezone.utc)

        db_session.add_all([batch_config, normal_config])
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)

        task = dispatch_scheduled_syncs
        task.push_request(id="sched-3")
        try:
            result = task()
        finally:
            task.pop_request()

        assert len(result["dispatched"]) == 2
        mock_batch_sync.apply_async.assert_called_once()
        mock_run_sync.apply_async.assert_called_once()


class TestTaskRegistration:
    def test_new_tasks_have_celery_attributes(self):
        from dev_health_ops.workers.tasks import (
            _batch_sync_callback,
            _run_sync_for_repo,
            dispatch_batch_sync,
        )

        for task in [dispatch_batch_sync, _batch_sync_callback, _run_sync_for_repo]:
            assert hasattr(task, "apply_async")
            assert hasattr(task, "delay")

    def test_dispatch_batch_sync_queue(self):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        assert dispatch_batch_sync.queue == "sync"

    def test_dispatch_batch_sync_rate_limit(self):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        assert dispatch_batch_sync.rate_limit == "5/m"

    def test_run_sync_for_repo_queue(self):
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        assert _run_sync_for_repo.queue == "sync"

    def test_run_sync_for_repo_rate_limit(self):
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        assert _run_sync_for_repo.rate_limit == "30/m"


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


class TestBatchChildrenQueueRouting:
    """Batch chord children and callback carry the provider queue (CHAOS-2299)."""

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_children_and_callback_signatures_carry_provider_queue(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
        monkeypatch,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        config = _make_config(
            provider="github",
            sync_options={"search": "org/*", "batch_size": 2},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", f"repo-{i}") for i in range(3)]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-queue")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        args, _ = mock_chord.call_args
        header_group, callback = args[0], args[1]

        for sig in header_group.tasks:
            assert sig.options.get("queue") == "sync.github"
        assert callback.options.get("queue") == "sync.github"

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_gitlab_children_carry_gitlab_queue(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
        monkeypatch,
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "glpat_test"}
        mock_discover.return_value = [("100",)]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="gl-batch-dispatch-queue")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        args, _ = mock_chord.call_args
        header_group, callback = args[0], args[1]
        assert [sig.options.get("queue") for sig in header_group.tasks] == [
            "sync.gitlab"
        ]
        assert callback.options.get("queue") == "sync.gitlab"

    @patch("dev_health_ops.workers.sync_batch.chord")
    @patch("dev_health_ops.discovery.repos.discover_repos_for_config")
    @patch("dev_health_ops.workers.sync_batch._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_flag_off_children_stay_on_shared_queue(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
        monkeypatch,
    ):
        """With PROVIDER_SYNC_QUEUES_ENABLED unset (the default) the chord
        children and callback stay on the legacy shared `sync` queue."""
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        monkeypatch.delenv("PROVIDER_SYNC_QUEUES_ENABLED", raising=False)
        config = _make_config(
            provider="github",
            sync_options={"search": "org/*"},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        mock_get_session.return_value = _fake_session_ctx(db_session)
        mock_resolve_creds.return_value = {"token": "ghp_test"}
        mock_discover.return_value = [("org", "repo-1")]
        mock_chord.return_value = MagicMock()

        task = dispatch_batch_sync
        task.push_request(id="batch-dispatch-flag-off")
        try:
            result = task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        args, _ = mock_chord.call_args
        header_group, callback = args[0], args[1]
        for sig in header_group.tasks:
            assert sig.options.get("queue") == "sync"
        assert callback.options.get("queue") == "sync"


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
        from dev_health_ops.api.services.configuration import _normalize_credential_keys

        result = _normalize_credential_keys("linear", {"apiKey": "lin_api_xxx"})
        assert result == {"api_key": "lin_api_xxx"}

    def test_already_snake_case_unchanged(self):
        from dev_health_ops.api.services.configuration import _normalize_credential_keys

        result = _normalize_credential_keys("linear", {"api_key": "lin_api_xxx"})
        assert result == {"api_key": "lin_api_xxx"}

    def test_jira_normalizes_multiple_keys(self):
        from dev_health_ops.api.services.configuration import _normalize_credential_keys

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
        from dev_health_ops.api.services.configuration import _normalize_credential_keys

        creds = {"someKey": "val"}
        result = _normalize_credential_keys("unknown", creds)
        assert result == {"someKey": "val"}

    def test_github_normalizes_baseUrl(self):
        from dev_health_ops.api.services.configuration import _normalize_credential_keys

        result = _normalize_credential_keys(
            "github", {"token": "ghp_xxx", "baseUrl": "https://gh.example.com"}
        )
        assert result == {"token": "ghp_xxx", "base_url": "https://gh.example.com"}


from dev_health_ops.metrics.job_work_items import (  # noqa: E402
    run_work_items_sync_job as _real_run_work_items_sync_job,
)


class TestRunSyncForRepoCredentials:
    """_run_sync_for_repo must thread credentials into the work-items job.

    Regression tests for CHAOS-2292: the work-items chunk used to rely on
    env-var injection only, which resolve_credentials_sync ignores once
    DATABASE_URI is configured — so GitHub App credentials (no "token" key)
    and PATs alike died in GitHubWorkClient.from_env().
    """

    @patch.dict(os.environ, {}, clear=False)
    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_batch._get_db_url")
    def test_work_items_job_receives_credentials(self, mock_get_db_url, mock_run_job):
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        mock_get_db_url.return_value = "clickhouse://ch:ch@clickhouse:8123/default"
        credentials = {"token": "ghp_batch_child", "org": "full-chaos"}

        task = _run_sync_for_repo
        task.push_request(id="repo-sync-creds")
        try:
            result = task(
                config_id="cfg-1",
                org_id="org-1",
                triggered_by="schedule",
                provider="github",
                sync_targets=["work-items"],
                sync_options_override={"owner": "full-chaos", "repo": "threads-cli"},
                credentials=credentials,
                config_name="chaos-all",
            )
        finally:
            task.pop_request()

        assert result["status"] == "success"
        mock_run_job.assert_called_once()
        kwargs = mock_run_job.call_args.kwargs
        assert kwargs["credentials"] == credentials
        assert kwargs["org_id"] == "org-1"

    @patch.dict(os.environ, {}, clear=False)
    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_batch._get_db_url")
    def test_work_items_job_kwargs_match_signature(self, mock_get_db_url, mock_run_job):
        """Producer-side contract: every kwarg exists on the job signature."""
        import inspect

        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        mock_get_db_url.return_value = "clickhouse://ch:ch@clickhouse:8123/default"

        task = _run_sync_for_repo
        task.push_request(id="repo-sync-contract")
        try:
            task(
                config_id="cfg-1",
                org_id="org-1",
                triggered_by="schedule",
                provider="github",
                sync_targets=["work-items"],
                sync_options_override={"owner": "full-chaos", "repo": "threads-cli"},
                credentials={"token": "ghp_contract"},
                config_name="chaos-all",
            )
        finally:
            task.pop_request()

        sig_params = set(inspect.signature(_real_run_work_items_sync_job).parameters)
        passed = set(mock_run_job.call_args.kwargs)
        assert passed <= sig_params, f"kwargs drifted: {passed - sig_params}"

    @patch.dict(os.environ, {}, clear=False)
    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_batch._get_db_url")
    def test_empty_credentials_passed_as_none(self, mock_get_db_url, mock_run_job):
        """An empty credentials dict must not masquerade as real credentials."""
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        mock_get_db_url.return_value = "clickhouse://ch:ch@clickhouse:8123/default"

        task = _run_sync_for_repo
        task.push_request(id="repo-sync-empty-creds")
        try:
            task(
                config_id="cfg-1",
                org_id="org-1",
                triggered_by="schedule",
                provider="github",
                sync_targets=["work-items"],
                sync_options_override={"owner": "full-chaos", "repo": "threads-cli"},
                credentials={},
                config_name="chaos-all",
            )
        finally:
            task.pop_request()

        assert mock_run_job.call_args.kwargs["credentials"] is None


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


class TestDispatchBatchSyncCredentialConfig:
    """A self-hosted GitLab url stored on credential.config (not in the
    encrypted secrets) must reach discovery and the batch children."""

    def _dispatch(self, db_session, *, decrypted: dict, config: dict):
        import uuid as _uuid

        from dev_health_ops.models.settings import IntegrationCredential
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

        credential = IntegrationCredential(
            org_id="default",
            provider="gitlab",
            name="default",
            credentials_encrypted="ciphertext",
            config=config,
        )
        db_session.add(credential)
        db_session.flush()

        sync_config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
            sync_targets=["git", "prs"],
        )
        sync_config.credential_id = credential.id
        db_session.add(sync_config)
        db_session.flush()

        captured_kwargs: list[dict] = []

        def _capture_signature(**kwargs):
            captured_kwargs.append(kwargs)
            return MagicMock()

        with (
            patch(
                "dev_health_ops.workers.sync_batch._run_sync_for_repo"
            ) as mock_run_for_repo,
            patch("dev_health_ops.workers.sync_batch.chord", return_value=MagicMock()),
            patch(
                "dev_health_ops.discovery.repos.discover_repos_for_config"
            ) as mock_discover,
            patch(
                "dev_health_ops.workers.task_utils._decrypt_credential_sync",
                return_value=decrypted,
            ),
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                side_effect=lambda *a, **k: _fake_session_ctx(db_session),
            ),
        ):
            mock_run_for_repo.s.side_effect = _capture_signature
            mock_discover.return_value = [("100",)]

            task = dispatch_batch_sync
            task.push_request(id=f"gl-cred-config-{_uuid.uuid4()}")
            try:
                result = task(config_id=str(sync_config.id), org_id="default")
            finally:
                task.pop_request()

        return result, mock_discover, captured_kwargs

    def test_config_url_threads_to_discovery_and_children(self, db_session):
        result, mock_discover, captured = self._dispatch(
            db_session,
            decrypted={"token": "glpat_secret"},
            config={"url": "https://gitlab.example.com"},
        )

        assert result["status"] == "dispatched"
        expected = {"url": "https://gitlab.example.com", "token": "glpat_secret"}
        # Discovery sees the merged mapping (self-hosted instance honoured).
        assert mock_discover.call_args.args[1] == expected
        # Children inherit the merged mapping, so resolve_gitlab_url in the
        # child resolves the self-hosted url too.
        assert len(captured) == 1
        assert captured[0]["credentials"] == expected

    def test_decrypted_url_wins_over_config_url(self, db_session):
        _, mock_discover, captured = self._dispatch(
            db_session,
            decrypted={"token": "glpat_secret", "url": "https://decrypted.example.com"},
            config={"url": "https://config.example.com"},
        )

        expected = {"token": "glpat_secret", "url": "https://decrypted.example.com"}
        assert mock_discover.call_args.args[1] == expected
        assert captured[0]["credentials"] == expected


class TestRunSyncConfigGitLabCredentialConfig:
    """run_sync_config's gitlab branch must honour credential.config URLs:
    sync_options.gitlab_url -> decrypted url -> credential.config url ->
    https://gitlab.com (CHAOS-2282 review follow-up)."""

    GITLAB_PROCESSOR = "dev_health_ops.processors.gitlab.process_gitlab_project"

    def _run(
        self,
        db_session,
        *,
        decrypted: dict,
        config: dict,
        sync_options: dict | None = None,
    ):
        import uuid as _uuid

        import dev_health_ops.connectors  # noqa: F401  (avoid circular-import on first load)
        import dev_health_ops.processors.gitlab  # noqa: F401  (make patch target resolvable)
        from dev_health_ops.models.settings import IntegrationCredential
        from dev_health_ops.workers.sync_runtime import run_sync_config

        credential = IntegrationCredential(
            org_id="default",
            provider="gitlab",
            name="default",
            credentials_encrypted="ciphertext",
            config=config,
        )
        db_session.add(credential)
        db_session.flush()

        sync_config = _make_config(
            provider="gitlab",
            sync_options={"project_id": 100, **(sync_options or {})},
            sync_targets=["git"],
        )
        sync_config.credential_id = credential.id
        db_session.add(sync_config)
        db_session.flush()

        async def _fake_run_with_store(db_url, db_type, handler, org_id=None):
            await handler(MagicMock())

        processor_mock = AsyncMock()
        with (
            patch(
                "dev_health_ops.workers.sync_runtime._get_db_url",
                return_value="clickhouse://localhost/dev",
            ),
            patch("dev_health_ops.storage.resolve_db_type", return_value="clickhouse"),
            patch("dev_health_ops.storage.run_with_store", new=_fake_run_with_store),
            patch(self.GITLAB_PROCESSOR, processor_mock),
            patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks"),
            patch(
                "dev_health_ops.workers.task_utils._decrypt_credential_sync",
                return_value=decrypted,
            ),
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                side_effect=lambda *a, **k: _fake_session_ctx(db_session),
            ),
        ):
            task = run_sync_config
            task.push_request(id=f"gl-runtime-cred-{_uuid.uuid4()}", retries=0)
            try:
                result = task(config_id=str(sync_config.id), org_id="default")
            finally:
                task.pop_request()
        return result, processor_mock

    def test_config_only_url_reaches_connector(self, db_session):
        result, processor = self._run(
            db_session,
            decrypted={"token": "glpat_secret"},
            config={"url": "https://gitlab.example.com"},
        )

        assert result["status"] == "success"
        kwargs = processor.await_args.kwargs
        assert kwargs["gitlab_url"] == "https://gitlab.example.com"
        assert kwargs["token"] == "glpat_secret"

    def test_decrypted_url_wins_over_config_url(self, db_session):
        result, processor = self._run(
            db_session,
            decrypted={"token": "glpat_secret", "url": "https://decrypted.example.com"},
            config={"url": "https://config.example.com"},
        )

        assert result["status"] == "success"
        assert (
            processor.await_args.kwargs["gitlab_url"] == "https://decrypted.example.com"
        )

    def test_sync_options_url_wins_over_everything(self, db_session):
        result, processor = self._run(
            db_session,
            decrypted={"token": "glpat_secret", "url": "https://decrypted.example.com"},
            config={"url": "https://config.example.com"},
            sync_options={"gitlab_url": "https://opt.example.com"},
        )

        assert result["status"] == "success"
        assert processor.await_args.kwargs["gitlab_url"] == "https://opt.example.com"


class TestGitLabWorkItemsUrlInjection:
    """Self-hosted GitLab work-items must reach the configured instance.

    fetch_gitlab_work_items builds GitLabWorkClient.from_env() (GITLAB_URL), so
    _run_sync_for_repo injects the resolved URL before the work-items chunk
    (post-rebase Codex finding on CHAOS-2282).
    """

    @patch.dict(os.environ, {}, clear=False)
    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_batch._get_db_url")
    def test_gitlab_url_env_injected_from_credentials(
        self, mock_get_db_url, mock_run_job
    ):
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        mock_get_db_url.return_value = "clickhouse://ch:ch@clickhouse:8123/default"
        os.environ.pop("GITLAB_URL", None)

        task = _run_sync_for_repo
        task.push_request(id="repo-sync-gl-url")
        try:
            task(
                config_id="cfg-1",
                org_id="org-1",
                triggered_by="schedule",
                provider="gitlab",
                sync_targets=["work-items"],
                sync_options_override={"project_id": 42},
                credentials={
                    "token": "glpat-child",
                    "url": "https://gitlab.example.com",
                },
                config_name="gl-batch",
            )
        finally:
            task.pop_request()

        assert os.environ.get("GITLAB_URL") == "https://gitlab.example.com"
        assert mock_run_job.call_args.kwargs["provider"] == "gitlab"

    @patch.dict(os.environ, {}, clear=False)
    @patch("dev_health_ops.sync.watermarks.set_watermark")
    @patch("dev_health_ops.metrics.job_work_items.run_work_items_sync_job")
    @patch("dev_health_ops.workers.sync_batch._get_db_url")
    def test_gitlab_work_items_only_child_stamps_no_watermark(
        self, mock_get_db_url, mock_run_job, mock_set_watermark
    ):
        from dev_health_ops.workers.sync_batch import _run_sync_for_repo

        mock_get_db_url.return_value = "clickhouse://ch:ch@clickhouse:8123/default"

        task = _run_sync_for_repo
        task.push_request(id="repo-sync-gl-wm")
        try:
            task(
                config_id="cfg-1",
                org_id="org-1",
                triggered_by="schedule",
                provider="gitlab",
                sync_targets=["work-items"],
                sync_options_override={"project_id": 42},
                credentials={"token": "glpat-child"},
                config_name="gl-batch",
            )
        finally:
            task.pop_request()

        mock_set_watermark.assert_not_called()
