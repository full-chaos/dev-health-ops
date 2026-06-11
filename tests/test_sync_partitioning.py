from __future__ import annotations

import os
import sys
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
    def test_returns_empty_on_total_failure(self, mock_github_cls):
        from dev_health_ops.discovery.repos import discover_github_repos

        mock_g = mock_github_cls.return_value
        mock_g.get_organization.side_effect = Exception("fail")
        mock_g.get_user.side_effect = Exception("fail")

        result = discover_github_repos({"search": "org/*"}, "token")
        assert result == []

    def test_returns_empty_without_owner(self):
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

    def test_returns_empty_without_group(self):
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
    ):
        from dev_health_ops.workers.sync_batch import dispatch_batch_sync

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
            sync_options={"search": "org/*"},
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
