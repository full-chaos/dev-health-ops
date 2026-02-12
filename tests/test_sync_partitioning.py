from __future__ import annotations

import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

_fake_croniter_mod = MagicMock()
if "croniter" not in sys.modules:
    sys.modules["croniter"] = _fake_croniter_mod

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
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"search": "my-org/*"},
        )
        assert _is_batch_eligible(config) is True

    def test_github_with_question_mark_wildcard(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"search": "my-org/repo-?"},
        )
        assert _is_batch_eligible(config) is True

    def test_github_with_discover_flag(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"discover": True, "owner": "my-org"},
        )
        assert _is_batch_eligible(config) is True

    def test_github_without_wildcard_not_eligible(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"owner": "my-org", "repo": "specific-repo"},
        )
        assert _is_batch_eligible(config) is False

    def test_gitlab_with_wildcard_search(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
        )
        assert _is_batch_eligible(config) is True

    def test_jira_never_batch_eligible(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="jira",
            sync_options={"search": "project/*"},
        )
        assert _is_batch_eligible(config) is False

    def test_local_provider_not_eligible(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="local",
            sync_options={"search": "org/*"},
        )
        assert _is_batch_eligible(config) is False

    def test_discover_false_not_eligible(self):
        from dev_health_ops.workers.tasks import _is_batch_eligible

        config = _make_config(
            provider="github",
            sync_options={"discover": False},
        )
        assert _is_batch_eligible(config) is False


class TestDiscoverReposForConfig:
    @patch("dev_health_ops.workers.tasks._discover_github_repos")
    def test_github_delegates_to_github_discovery(self, mock_gh):
        from dev_health_ops.workers.tasks import _discover_repos_for_config

        mock_gh.return_value = [("my-org", "repo-a"), ("my-org", "repo-b")]
        config = _make_config(
            provider="github",
            sync_options={"search": "my-org/*"},
        )
        result = _discover_repos_for_config(config, {"token": "ghp_test"})
        assert result == [("my-org", "repo-a"), ("my-org", "repo-b")]
        mock_gh.assert_called_once()

    @patch("dev_health_ops.workers.tasks._discover_gitlab_repos")
    def test_gitlab_delegates_to_gitlab_discovery(self, mock_gl):
        from dev_health_ops.workers.tasks import _discover_repos_for_config

        mock_gl.return_value = [("123",), ("456",)]
        config = _make_config(
            provider="gitlab",
            sync_options={"search": "my-group/*"},
        )
        result = _discover_repos_for_config(config, {"token": "glpat_test"})
        assert result == [("123",), ("456",)]
        mock_gl.assert_called_once()

    def test_unsupported_provider_returns_empty(self):
        from dev_health_ops.workers.tasks import _discover_repos_for_config

        config = _make_config(provider="jira")
        result = _discover_repos_for_config(config, {"token": "test"})
        assert result == []


class TestDiscoverGithubRepos:
    @patch("github.Github")
    def test_lists_org_repos_filtered_by_pattern(self, mock_github_cls):
        from dev_health_ops.workers.tasks import _discover_github_repos

        repo_a = SimpleNamespace(name="api-service")
        repo_b = SimpleNamespace(name="web-app")
        repo_c = SimpleNamespace(name="docs")

        mock_org = MagicMock()
        mock_org.get_repos.return_value = [repo_a, repo_b, repo_c]
        mock_github_cls.return_value.get_organization.return_value = mock_org

        result = _discover_github_repos({"search": "my-org/api-*"}, "ghp_token")
        assert result == [("my-org", "api-service")]

    @patch("github.Github")
    def test_wildcard_star_matches_all(self, mock_github_cls):
        from dev_health_ops.workers.tasks import _discover_github_repos

        repos = [SimpleNamespace(name=f"repo-{i}") for i in range(3)]
        mock_org = MagicMock()
        mock_org.get_repos.return_value = repos
        mock_github_cls.return_value.get_organization.return_value = mock_org

        result = _discover_github_repos({"search": "org/*"}, "token")
        assert len(result) == 3

    @patch("github.Github")
    def test_falls_back_to_user_repos_on_org_error(self, mock_github_cls):
        from dev_health_ops.workers.tasks import _discover_github_repos

        mock_g = mock_github_cls.return_value
        mock_g.get_organization.side_effect = Exception("Not an org")
        mock_user = MagicMock()
        mock_user.get_repos.return_value = [SimpleNamespace(name="my-repo")]
        mock_g.get_user.return_value = mock_user

        result = _discover_github_repos({"search": "user/*"}, "token")
        assert result == [("user", "my-repo")]

    @patch("github.Github")
    def test_returns_empty_on_total_failure(self, mock_github_cls):
        from dev_health_ops.workers.tasks import _discover_github_repos

        mock_g = mock_github_cls.return_value
        mock_g.get_organization.side_effect = Exception("fail")
        mock_g.get_user.side_effect = Exception("fail")

        result = _discover_github_repos({"search": "org/*"}, "token")
        assert result == []

    def test_returns_empty_without_owner(self):
        from dev_health_ops.workers.tasks import _discover_github_repos

        result = _discover_github_repos({"search": ""}, "token")
        assert result == []


class TestDiscoverGitlabRepos:
    @patch("gitlab.Gitlab")
    def test_lists_group_projects_filtered(self, mock_gitlab_cls):
        from dev_health_ops.workers.tasks import _discover_gitlab_repos

        proj_a = SimpleNamespace(name="api", id=100)
        proj_b = SimpleNamespace(name="web", id=200)
        proj_c = SimpleNamespace(name="docs", id=300)

        mock_grp = MagicMock()
        mock_grp.projects.list.return_value = [proj_a, proj_b, proj_c]
        mock_gitlab_cls.return_value.groups.get.return_value = mock_grp

        result = _discover_gitlab_repos({"search": "my-group/api*"}, "glpat_token")
        assert result == [("100",)]

    @patch("gitlab.Gitlab")
    def test_returns_empty_on_group_error(self, mock_gitlab_cls):
        from dev_health_ops.workers.tasks import _discover_gitlab_repos

        mock_gitlab_cls.return_value.groups.get.side_effect = Exception("nope")
        result = _discover_gitlab_repos({"search": "group/*"}, "token")
        assert result == []

    def test_returns_empty_without_group(self):
        from dev_health_ops.workers.tasks import _discover_gitlab_repos

        result = _discover_gitlab_repos({"search": ""}, "token")
        assert result == []


class TestGetBatchSize:
    def test_from_sync_options(self):
        from dev_health_ops.workers.tasks import _get_batch_size

        assert _get_batch_size({"batch_size": 10}) == 10

    def test_from_env_var(self, monkeypatch):
        from dev_health_ops.workers.tasks import _get_batch_size

        monkeypatch.setenv("SYNC_BATCH_SIZE", "8")
        assert _get_batch_size({}) == 8

    def test_default_is_five(self, monkeypatch):
        from dev_health_ops.workers.tasks import _get_batch_size

        monkeypatch.delenv("SYNC_BATCH_SIZE", raising=False)
        assert _get_batch_size({}) == 5

    def test_sync_options_takes_precedence_over_env(self, monkeypatch):
        from dev_health_ops.workers.tasks import _get_batch_size

        monkeypatch.setenv("SYNC_BATCH_SIZE", "8")
        assert _get_batch_size({"batch_size": 3}) == 3


class TestDispatchBatchSync:
    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.workers.tasks._discover_repos_for_config")
    @patch("dev_health_ops.workers.tasks._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_dispatches_correct_number_of_child_tasks(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.tasks import dispatch_batch_sync

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

    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.workers.tasks._discover_repos_for_config")
    @patch("dev_health_ops.workers.tasks._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_empty_repo_list_returns_no_repos(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.tasks import dispatch_batch_sync

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

    @patch("dev_health_ops.workers.tasks.run_sync_config")
    @patch("dev_health_ops.workers.tasks._discover_repos_for_config")
    @patch("dev_health_ops.workers.tasks._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_discovery_failure_falls_back_to_single_dispatch(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_run_sync,
        db_session,
    ):
        from dev_health_ops.workers.tasks import dispatch_batch_sync

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

    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.workers.tasks._discover_repos_for_config")
    @patch("dev_health_ops.workers.tasks._resolve_env_credentials")
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

    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.workers.tasks._discover_repos_for_config")
    @patch("dev_health_ops.workers.tasks._resolve_env_credentials")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_respects_custom_batch_size(
        self,
        mock_get_session,
        mock_resolve_creds,
        mock_discover,
        mock_chord,
        db_session,
    ):
        from dev_health_ops.workers.tasks import dispatch_batch_sync

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


class TestBatchSyncCallback:
    @patch("dev_health_ops.workers.tasks._dispatch_post_sync_tasks")
    def test_fires_post_sync_tasks(self, mock_post_sync):
        from dev_health_ops.workers.tasks import _batch_sync_callback

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


def _setup_croniter_mock():
    mock_cron_instance = MagicMock()
    mock_cron_instance.get_next.return_value = datetime(2000, 1, 1, tzinfo=timezone.utc)
    mock_cron_cls = MagicMock(return_value=mock_cron_instance)
    _fake_croniter_mod.croniter = mock_cron_cls
    return mock_cron_cls


class TestDispatchScheduledSyncsRouting:
    @patch("dev_health_ops.workers.tasks.dispatch_batch_sync")
    @patch("dev_health_ops.workers.tasks.run_sync_config")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_routes_batch_eligible_to_dispatch_batch_sync(
        self, mock_get_session, mock_run_sync, mock_batch_sync, db_session
    ):
        from dev_health_ops.workers.tasks import dispatch_scheduled_syncs

        _setup_croniter_mock()

        config = _make_config(
            provider="github",
            sync_options={"search": "org/*"},
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

    @patch("dev_health_ops.workers.tasks.dispatch_batch_sync")
    @patch("dev_health_ops.workers.tasks.run_sync_config")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_routes_normal_config_to_run_sync_config(
        self, mock_get_session, mock_run_sync, mock_batch_sync, db_session
    ):
        from dev_health_ops.workers.tasks import dispatch_scheduled_syncs

        _setup_croniter_mock()

        config = _make_config(
            provider="github",
            sync_options={"owner": "org", "repo": "specific-repo"},
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

    @patch("dev_health_ops.workers.tasks.dispatch_batch_sync")
    @patch("dev_health_ops.workers.tasks.run_sync_config")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_mixed_configs_route_correctly(
        self, mock_get_session, mock_run_sync, mock_batch_sync, db_session
    ):
        from dev_health_ops.workers.tasks import dispatch_scheduled_syncs

        _setup_croniter_mock()

        batch_config = _make_config(
            provider="github",
            sync_options={"search": "org/*"},
            name="batch",
        )
        batch_config.last_sync_at = datetime(2020, 1, 1, tzinfo=timezone.utc)

        normal_config = _make_config(
            provider="github",
            sync_options={"owner": "org", "repo": "repo"},
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
        from dev_health_ops.workers.tasks import dispatch_batch_sync

        assert dispatch_batch_sync.queue == "sync"

    def test_dispatch_batch_sync_rate_limit(self):
        from dev_health_ops.workers.tasks import dispatch_batch_sync

        assert dispatch_batch_sync.rate_limit == "5/m"

    def test_run_sync_for_repo_queue(self):
        from dev_health_ops.workers.tasks import _run_sync_for_repo

        assert _run_sync_for_repo.queue == "sync"
