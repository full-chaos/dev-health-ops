"""Tests for GitLab API-based file-content backfill (no local checkout).

Mirrors tests/test_github_content_backfill.py: covers the batched GraphQL
blob fetch on the connector, the scanner-driven path selection in
``processors.gitlab._fetch_scannable_contents``, and the paths-only repo
upgrade through ``_backfill_gitlab_missing_data``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Initialize the connectors package before processors.gitlab to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.metrics.job_complexity_db as job
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.processors import gitlab as gitlab_processor
from dev_health_ops.processors.gitlab import (
    _backfill_gitlab_missing_data,
    _fetch_scannable_contents,
    _sync_gitlab_mrs_to_store,
)
from dev_health_ops.providers.gitlab.code_client import (
    GitLabBlameRange,
    GitLabCommitData,
    GitLabCommitStatsData,
    GitLabFileBlame,
)
from tests._complexity_readiness_fixtures import (
    ComplexityReadinessClient,
    ComplexityReadinessSink,
)


class _FakeGitLabCodeClientForFiles:
    """Minimal instrumented GitLabCodeClient stand-in for the file-content /
    blame backfill branches (CHAOS-2815/CS14). The real backfill fetches
    through ``GitLabCodeClient.get_file_contents`` /
    ``GitLabCodeClient.get_file_blame``, never the frozen
    ``connector.get_file_contents`` / ``connector.rest_client.get_file_blame``.
    """

    def __init__(
        self,
        *,
        contents=None,
        contents_error=None,
        blame_by_path=None,
        blame_error_paths=None,
        tree_items=None,
        observations=None,
        latest_commit_sha="resolved-sha",
    ):
        self.contents = contents or {}
        self.contents_error = contents_error
        self.blame_by_path = blame_by_path or {}
        self.blame_error_paths = blame_error_paths or {}
        self.tree_items = tree_items if tree_items is not None else []
        self.observations = observations or []
        self.latest_commit_sha = latest_commit_sha
        self.get_latest_commit_sha_calls: list[Any] = []
        self.get_file_contents_calls: list[Any] = []
        self.get_file_blame_calls: list[Any] = []
        self.list_repository_tree_calls: list[Any] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get_file_contents(self, project_full_path, paths, *, ref, max_bytes=None):
        self.get_file_contents_calls.append(
            (project_full_path, tuple(paths), ref, max_bytes)
        )
        if self.contents_error is not None:
            raise self.contents_error
        return self.contents

    async def get_file_blame(self, project_id, file_path, *, ref):
        self.get_file_blame_calls.append((project_id, file_path, ref))
        if file_path in self.blame_error_paths:
            raise self.blame_error_paths[file_path]
        return self.blame_by_path.get(file_path, GitLabFileBlame(file_path=file_path))

    async def get_latest_commit_sha(self, project_id, *, ref, until):
        self.get_latest_commit_sha_calls.append((project_id, ref, until))
        return self.latest_commit_sha

    async def list_repository_tree(
        self, project_id, *, ref, per_page=100, max_items=1_000_000
    ):
        self.list_repository_tree_calls.append((project_id, ref, per_page, max_items))
        return self.tree_items

    def drain_usage_observations(self):
        observations = list(self.observations)
        self.observations.clear()
        return observations


class TestFetchScannableContents:
    @pytest.mark.asyncio
    async def test_filters_by_scanner_globs(self, monkeypatch):
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={"src/app.py": "x = 1\n"},
            observations=[{"route_family": "project"}],
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )
        connector = Mock()
        usage_sink: list[dict[str, Any]] = []

        result = await _fetch_scannable_contents(
            connector,
            "group/proj",
            "main",
            ["src/app.py", "README.md", "pkg/__init__.py"],
            usage_sink=usage_sink,
        )

        assert result == {"src/app.py": "x = 1\n"}
        assert fake_client.get_file_contents_calls == [
            ("group/proj", ("src/app.py",), "main", 1_000_000)
        ]
        assert usage_sink == [{"route_family": "project"}]

    @pytest.mark.asyncio
    async def test_api_error_degrades_to_empty(self, monkeypatch):
        fake_client = _FakeGitLabCodeClientForFiles(contents_error=RuntimeError("boom"))
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )
        connector = Mock()

        result = await _fetch_scannable_contents(
            connector, "group/proj", "main", ["src/app.py"]
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_rate_limit_exception_propagates(self, monkeypatch):
        from dev_health_ops.exceptions import RateLimitException

        fake_client = _FakeGitLabCodeClientForFiles(
            contents_error=RateLimitException("rate limited")
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )
        connector = Mock()

        with pytest.raises(RateLimitException):
            await _fetch_scannable_contents(
                connector, "group/proj", "main", ["src/app.py"]
            )


class TestGitLabBackfillContents:
    @pytest.mark.asyncio
    async def test_backfill_writes_contents_for_paths_only_repos(self, monkeypatch):
        """Projects with paths-only git_files rows get upgraded."""
        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=False)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=True)

        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)

        project = Mock()
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/app.py"},
            {"type": "blob", "path": "README.md"},
            {"type": "tree", "path": "src"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={"src/app.py": "x = 1\n"}, tree_items=tree_items
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        by_path = {f.path: f.contents for f in written}
        assert by_path == {"src/app.py": "x = 1\n", "README.md": None}

    @pytest.mark.asyncio
    async def test_backfill_skips_when_contents_already_present(self):
        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=True)

        connector = Mock()
        sink = Mock()
        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        connector.gitlab.projects.get.assert_not_called()
        sink.insert_git_file_data.assert_not_called()


class _FakeGitLabCodeClientForStats:
    """Minimal instrumented GitLabCodeClient stand-in for the commit-stats
    backfill branch (CHAOS-2814/CS13)."""

    def __init__(self, *, commits=None, commit_stats=None, observations=None):
        self.commits = commits or []
        self.commit_stats = commit_stats or {}
        self.observations = observations or []
        self.get_commits_calls: list[Any] = []
        self.get_commits_window_calls: list[Any] = []
        self.get_commit_stats_calls: list[Any] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get_commits(
        self, project_id, *, max_commits, since=None, until=None, per_page=100
    ):
        self.get_commits_calls.append(project_id)
        self.get_commits_window_calls.append((project_id, max_commits, since, until))
        return self.commits[:max_commits] if max_commits is not None else self.commits

    async def get_commit_stats(self, project_id, commit_sha):
        self.get_commit_stats_calls.append((project_id, commit_sha))
        return self.commit_stats[commit_sha]

    def drain_usage_observations(self):
        observations = list(self.observations)
        self.observations.clear()
        return observations


class TestGitLabBackfillCommitStats:
    """CHAOS-2814/CS13: the commit-stats backfill branch must fetch through
    the canonical, instrumented GitLabCodeClient (``_fetch_gitlab_commits_sync``
    / ``_fetch_gitlab_commit_stats_sync``) -- never the frozen python-gitlab
    ``project.commits.list()`` or the un-instrumented
    ``connector.get_commit_stats_by_project()``.
    """

    @pytest.mark.asyncio
    async def test_backfill_commit_stats_uses_gitlab_code_client(self, monkeypatch):
        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=True)

        recorded_stats: list[Any] = []
        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock(side_effect=recorded_stats.extend)

        commit = GitLabCommitData(
            commit_id="abc123",
            message="ship it",
            author_name="Ada",
            authored_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            committer_name="Ada",
            committed_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            parent_ids=(),
        )
        fake_client = _FakeGitLabCodeClientForStats(
            commits=[commit],
            commit_stats={"abc123": GitLabCommitStatsData("abc123", 5, 2)},
            observations=[{"route_family": "project"}],
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        connector = Mock()
        connector.gitlab.projects.get.return_value = Mock()
        # The legacy, frozen connector API must never be invoked by the fixed
        # backfill path.
        connector.get_commit_stats_by_project = Mock(
            side_effect=AssertionError(
                "legacy connector.get_commit_stats_by_project must not be called"
            )
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        usage_sink: list[dict[str, Any]] = []

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_files=False,
            include_blame=False,
            include_commit_stats=True,
            usage_sink=usage_sink,
        )

        # Fetched via the canonical GitLabCodeClient, keyed by project_full_name
        # (the caller-supplied project id/path), not the frozen connector API.
        assert fake_client.get_commits_calls == ["group/proj"]
        assert fake_client.get_commit_stats_calls == [("group/proj", "abc123")]
        connector.get_commit_stats_by_project.assert_not_called()
        connector.gitlab.projects.get.return_value.commits.list.assert_not_called()

        assert len(recorded_stats) == 1
        assert recorded_stats[0].commit_hash == "abc123"
        assert recorded_stats[0].additions == 5
        assert recorded_stats[0].deletions == 2
        assert recorded_stats[0].repo_id == db_repo.id

        # usage_sink plumbing (CHAOS-2803/CS2): the client's drained per-request
        # observations must reach the caller-supplied sink.
        assert usage_sink == [{"route_family": "project"}]

    @pytest.mark.asyncio
    async def test_historical_commit_stats_backfill_fetches_entire_window(
        self, monkeypatch
    ):
        since = datetime(2026, 1, 10, tzinfo=timezone.utc)
        until = datetime(2026, 1, 12, tzinfo=timezone.utc)
        commits = [
            GitLabCommitData(
                commit_id=f"sha-{idx}",
                message="ship it",
                author_name="Ada",
                authored_date=since,
                committer_name="Ada",
                committed_date=since,
                parent_ids=(),
            )
            for idx in range(60)
        ]
        fake_client = _FakeGitLabCodeClientForStats(
            commits=commits,
            commit_stats={
                commit.commit_id: GitLabCommitStatsData(commit.commit_id, 1, 0)
                for commit in commits
            },
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=True)

        recorded_stats: list[Any] = []
        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock(side_effect=recorded_stats.extend)

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=Mock(),
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_files=False,
            include_blame=False,
            include_commit_stats=True,
            since=since,
            until=until,
        )

        assert fake_client.get_commits_window_calls == [
            ("group/proj", None, since, until)
        ]
        assert len(fake_client.get_commit_stats_calls) == 60
        assert [row.commit_hash for row in recorded_stats] == [
            f"sha-{idx}" for idx in range(60)
        ]

    @pytest.mark.asyncio
    async def test_historical_backfill_uses_resolved_ref_for_tree_content_blame_and_complexity(
        self, monkeypatch
    ):
        until = datetime(2026, 1, 12, tzinfo=timezone.utc)
        store = TestGitLabBackfillBlame._store({"src/app.py"})
        store.org_id = "test-org"
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.get_blamed_paths = AsyncMock(return_value={"README.md"})

        written_files: list[Any] = []
        written_blame: list[Any] = []

        async def insert_files(batch):
            written_files.extend(batch)

        async def insert_blame(batch):
            written_blame.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert_files)
        sink.insert_blame_data = AsyncMock(side_effect=insert_blame)
        metrics_sink = ComplexityReadinessSink(ComplexityReadinessClient([]))

        connector = Mock()
        connector.gitlab.projects.get.return_value = Mock(id=42)
        tree_items = [
            {"type": "blob", "path": "src/app.py"},
            {"type": "blob", "path": "README.md"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={"src/app.py": "def app():\n    return 1\n"},
            blame_by_path={
                "src/app.py": GitLabFileBlame(
                    file_path="src/app.py",
                    ranges=(
                        GitLabBlameRange(
                            1,
                            1,
                            "resolved-sha",
                            "Ada",
                            "ada@example.com",
                            0,
                            ("def app():",),
                        ),
                    ),
                )
            },
            tree_items=tree_items,
            latest_commit_sha="resolved-sha",
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=True,
            include_commit_stats=False,
            until=until,
            metrics_sink=metrics_sink,
        )

        assert fake_client.get_latest_commit_sha_calls == [
            ("group/proj", "main", until)
        ]
        assert fake_client.list_repository_tree_calls == [
            ("group/proj", "resolved-sha", 100, 1_000_000)
        ]
        assert fake_client.get_file_contents_calls == [
            ("group/proj", ("src/app.py",), "resolved-sha", 1_000_000)
        ]
        assert fake_client.get_file_blame_calls == [
            ("group/proj", "src/app.py", "resolved-sha")
        ]
        sink.insert_git_file_data.assert_not_called()
        assert written_files == []
        assert [row.commit_hash for row in written_blame] == ["resolved-sha"]
        assert [snap.ref for snap in metrics_sink.snapshots] == ["resolved-sha"]
        assert [snap.as_of_day for snap in metrics_sink.snapshots] == [until.date()]
        assert [daily.day for daily in metrics_sink.dailies] == [until.date()]
        assert metrics_sink.dailies[0].org_id == "test-org"

    @pytest.mark.asyncio
    async def test_backfill_skips_commit_stats_when_already_present(self, monkeypatch):
        """Regression guard: when the store already has commit stats, the
        GitLabCodeClient must not be invoked at all."""
        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=True)

        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock()

        connector = Mock()
        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        client_factory = Mock()
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            client_factory,
        )

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_files=False,
            include_blame=False,
            include_commit_stats=True,
        )

        client_factory.assert_not_called()
        sink.insert_git_commit_stats.assert_not_called()


class TestGitLabBackfillBlame:
    """CHAOS-2815/CS14: the blame backfill branch must fetch through the
    canonical, instrumented ``GitLabCodeClient.get_file_blame`` -- never the
    frozen, un-instrumented ``connector.rest_client.get_file_blame``.
    """

    @staticmethod
    def _store(all_paths: set[str]) -> Mock:
        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=False)
        store.get_blamed_paths = AsyncMock(return_value=set())
        store.has_unblamed_files = AsyncMock(return_value=bool(all_paths))
        return store

    @pytest.mark.asyncio
    async def test_backfill_writes_blame_via_gitlab_code_client(self, monkeypatch):
        store = self._store({"src/a.py", "src/b.py"})

        written: list[Any] = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_blame_data = AsyncMock(side_effect=insert)

        project = Mock()
        project.id = 42
        connector = Mock()
        connector.gitlab.projects.get.return_value = project
        # The legacy, frozen REST helper must never be invoked by the fixed
        # blame backfill path.
        connector.rest_client.get_file_blame = Mock(
            side_effect=AssertionError(
                "legacy connector.rest_client.get_file_blame must not be called"
            )
        )

        tree_items = [
            {"type": "blob", "path": "src/a.py"},
            {"type": "blob", "path": "src/b.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            blame_by_path={
                "src/a.py": GitLabFileBlame(
                    file_path="src/a.py",
                    ranges=(
                        GitLabBlameRange(
                            1,
                            2,
                            "sha1",
                            "Ada",
                            "ada@example.com",
                            0,
                            ("a = 1", "a = 2"),
                        ),
                        GitLabBlameRange(
                            3, 3, "sha3", "Linus", "linus@example.com", 0, ("a = 3",)
                        ),
                    ),
                ),
                "src/b.py": GitLabFileBlame(
                    file_path="src/b.py",
                    ranges=(
                        GitLabBlameRange(
                            1, 1, "sha2", "Grace", "grace@example.com", 0, ("b = 1",)
                        ),
                    ),
                ),
            },
            tree_items=tree_items,
            observations=[{"route_family": "project"}],
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()
        usage_sink: list[dict[str, Any]] = []

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_files=False,
            include_commit_stats=False,
            usage_sink=usage_sink,
        )

        assert connector.rest_client.get_file_blame.call_count == 0
        assert [
            (row.path, row.line_no, row.line, row.commit_hash, row.author_name)
            for row in written
        ] == [
            ("src/a.py", 1, "a = 1", "sha1", "Ada"),
            ("src/a.py", 2, "a = 2", "sha1", "Ada"),
            ("src/a.py", 3, "a = 3", "sha3", "Linus"),
            ("src/b.py", 1, "b = 1", "sha2", "Grace"),
        ]
        assert usage_sink[0]["route_family"] == "project"

    @pytest.mark.asyncio
    async def test_per_file_failure_is_skipped_but_crawl_continues(self, monkeypatch):
        """A single file's fetch failure logs and continues -- bounded
        per-file resilience is preserved after the GitLabCodeClient rewire."""
        store = self._store({"src/a.py", "src/b.py"})

        written: list[Any] = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_blame_data = AsyncMock(side_effect=insert)

        project = Mock()
        project.id = 42
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/a.py"},
            {"type": "blob", "path": "src/b.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            blame_by_path={
                "src/b.py": GitLabFileBlame(
                    file_path="src/b.py",
                    ranges=(
                        GitLabBlameRange(1, 1, "sha2", "Grace", "", 0, ("b = 1",)),
                    ),
                ),
            },
            blame_error_paths={"src/a.py": RuntimeError("boom")},
            tree_items=tree_items,
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_files=False,
            include_commit_stats=False,
        )

        assert {row.path for row in written} == {"src/b.py"}

    @pytest.mark.asyncio
    async def test_per_file_failure_warnings_are_bounded(self, monkeypatch, caplog):
        paths = {f"src/{idx}.py" for idx in range(6)}
        store = self._store(paths)
        sink = Mock()
        sink.insert_blame_data = AsyncMock()
        project = Mock()
        project.id = 42
        connector = Mock()
        connector.gitlab.projects.get.return_value = project
        tree_items = [{"type": "blob", "path": path} for path in sorted(paths)]
        fake_client = _FakeGitLabCodeClientForFiles(
            blame_error_paths={path: RuntimeError("boom") for path in paths},
            tree_items=tree_items,
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )
        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        with caplog.at_level(logging.WARNING):
            await _backfill_gitlab_missing_data(
                store=store,
                ingestion_sink=sink,
                connector=connector,
                db_repo=db_repo,
                project_full_name="group/proj",
                default_branch="main",
                max_commits=None,
                include_files=False,
                include_commit_stats=False,
            )

        messages = [record.getMessage() for record in caplog.records]
        assert sum("Failed GitLab blame fetch" in message for message in messages) == 5
        assert any(
            "Skipped GitLab blame for 6 file(s) in group/proj; logged first 5 failures"
            in message
            for message in messages
        )

    @pytest.mark.asyncio
    async def test_rate_limit_exception_propagates_and_stops_crawl(self, monkeypatch):
        """An exhausted rate limit on one file must propagate -- never be
        swallowed as a per-file warning like other errors."""
        from dev_health_ops.exceptions import RateLimitException

        store = self._store({"src/a.py", "src/b.py"})

        sink = Mock()
        sink.insert_blame_data = AsyncMock()

        project = Mock()
        project.id = 42
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/a.py"},
            {"type": "blob", "path": "src/b.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            blame_error_paths={"src/a.py": RateLimitException("rate limited")},
            tree_items=tree_items,
            observations=[{"route_family": "project"}],
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()
        usage_sink: list[dict[str, Any]] = []

        with pytest.raises(RateLimitException):
            await _backfill_gitlab_missing_data(
                store=store,
                ingestion_sink=sink,
                connector=connector,
                db_repo=db_repo,
                project_full_name="group/proj",
                default_branch="main",
                max_commits=None,
                include_files=False,
                include_commit_stats=False,
                usage_sink=usage_sink,
            )

        # Partial observations gathered before the raise still reach the sink
        # (CHAOS-2754/2803 partial-observations-on-exception contract).
        assert usage_sink == [{"route_family": "project"}]


class _FakeGitLabCodeClientForMergeRequests:
    def __init__(
        self, *, mrs=None, pages=None, approvals=None, notes=None, observations=None
    ):
        self.mrs = mrs or []
        self.pages = pages or {1: self.mrs, 2: []}
        self.approvals = approvals or {}
        self.notes = notes or {}
        self.observations = observations or []
        self.iter_merge_requests_calls: list[Any] = []
        self.get_merge_requests_page_calls: list[Any] = []
        self.get_mr_approvals_calls: list[Any] = []
        self.iter_mr_notes_calls: list[Any] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def iter_merge_requests(self, *, project_id, state, per_page):
        self.iter_merge_requests_calls.append((project_id, state, per_page))
        return self.mrs

    async def get_merge_requests_page(self, *, project_id, page, state, per_page):
        self.get_merge_requests_page_calls.append((project_id, page, state, per_page))
        val = self.pages.get(page, [])
        if isinstance(val, Exception):
            raise val
        return list(val)

    async def get_mr_approvals(self, project_id, iid):
        self.get_mr_approvals_calls.append((project_id, iid))
        return self.approvals.get(iid)

    async def iter_mr_notes(self, project_id, iid, *, per_page):
        self.iter_mr_notes_calls.append((project_id, iid, per_page))
        return self.notes.get(iid, [])

    def drain_usage_observations(self):
        observations = list(self.observations)
        self.observations.clear()
        return observations


class _FakeMrSink:
    def __init__(self):
        self.prs: list[Any] = []
        self.reviews: list[Any] = []

    def insert_git_pull_requests(self, batch):
        self.prs.extend(batch)

        async def _noop():
            return None

        return _noop()

    def insert_git_pull_request_reviews(self, batch):
        self.reviews.extend(batch)

        async def _noop():
            return None

        return _noop()


class _NoSleepGitLabGate:
    def wait_sync(self) -> None:
        return

    def penalize(self, delay_seconds=None) -> float:
        return float(delay_seconds or 0)

    def reset(self) -> None:
        return


def test_gitlab_mr_sync_uses_code_client_and_drains_usage() -> None:
    mr = {
        "iid": 42,
        "title": "Ship",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 1,
    }
    fake_client = _FakeGitLabCodeClientForMergeRequests(
        mrs=[mr],
        approvals={42: {"approved_by": []}},
        notes={
            42: [
                {
                    "id": 300,
                    "system": True,
                    "body": "approved this merge request",
                    "author": {"username": "alice"},
                    "created_at": "2026-01-04T10:00:00Z",
                }
            ]
        },
        observations=[
            {"route_family": "merge_requests", "request_count": 2},
            {"route_family": "notes", "request_count": 1},
        ],
    )
    connector = Mock()
    connector.per_page = 100
    connector.rest_client.get_merge_requests.side_effect = AssertionError(
        "legacy MR list path must not be called"
    )
    connector.rest_client.get_merge_request_approvals.side_effect = AssertionError(
        "legacy approvals path must not be called"
    )
    connector.rest_client.get_merge_request_notes.side_effect = AssertionError(
        "legacy notes path must not be called"
    )

    sink = _FakeMrSink()
    usage_sink: list[dict[str, Any]] = []

    async def _driver() -> int:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: _sync_gitlab_mrs_to_store(
                connector,
                99,
                uuid.uuid4(),
                cast(IngestionSink, sink),
                loop,
                50,
                gate=_NoSleepGitLabGate(),
                usage_sink=usage_sink,
            ),
        )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: fake_client,
    ):
        total = asyncio.run(_driver())

    assert total == 1
    assert [pr.number for pr in sink.prs] == [42]
    assert [review.state for review in sink.reviews] == ["APPROVED"]
    assert fake_client.get_merge_requests_page_calls == [
        (99, 1, "all", 100),
        (99, 2, "all", 100),
    ]
    assert fake_client.get_mr_approvals_calls == [(99, 42)]
    assert fake_client.iter_mr_notes_calls == [(99, 42, 100)]
    connector.rest_client.get_merge_requests.assert_not_called()
    connector.rest_client.get_merge_request_approvals.assert_not_called()
    connector.rest_client.get_merge_request_notes.assert_not_called()
    assert usage_sink == [
        {"route_family": "merge_requests", "request_count": 2},
        {"route_family": "notes", "request_count": 1},
    ]


def test_gitlab_mr_sync_stops_pagination_at_since_boundary() -> None:
    recent_mr = {
        "iid": 10,
        "title": "Recent",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-03T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-05T13:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
    }
    older_mr = {
        "iid": 9,
        "title": "Older",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-01T12:00:00Z",
        "merged_at": "2026-01-01T13:00:00Z",
        "source_branch": "old",
        "target_branch": "main",
    }
    fake_client = _FakeGitLabCodeClientForMergeRequests(
        pages={1: [recent_mr, older_mr], 2: [older_mr]},
        approvals={10: {"approved_by": []}},
    )
    connector = Mock()
    connector.per_page = 100
    sink = _FakeMrSink()

    async def _driver() -> int:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: _sync_gitlab_mrs_to_store(
                connector,
                99,
                uuid.uuid4(),
                cast(IngestionSink, sink),
                loop,
                50,
                gate=_NoSleepGitLabGate(),
                since=datetime(2026, 1, 2, tzinfo=timezone.utc),
            ),
        )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: fake_client,
    ):
        total = asyncio.run(_driver())

    assert total == 1
    assert [pr.number for pr in sink.prs] == [10]
    assert fake_client.get_merge_requests_page_calls == [(99, 1, "all", 100)]


def test_gitlab_mr_sync_flushes_rows_before_terminal_rate_limit() -> None:
    from dev_health_ops.exceptions import RateLimitException

    mr = {
        "iid": 77,
        "title": "Persist before limit",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-03T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-05T13:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
    }
    fake_client = _FakeGitLabCodeClientForMergeRequests(
        pages={1: [mr], 2: RateLimitException("limited")},
        approvals={77: {"approved_by": []}},
    )
    connector = Mock()
    connector.per_page = 100
    sink = _FakeMrSink()

    async def _driver() -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: _sync_gitlab_mrs_to_store(
                connector,
                99,
                uuid.uuid4(),
                cast(IngestionSink, sink),
                loop,
                50,
                gate=_NoSleepGitLabGate(),
            ),
        )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: fake_client,
    ):
        with pytest.raises(RateLimitException):
            asyncio.run(_driver())

    assert [pr.number for pr in sink.prs] == [77]
    assert fake_client.get_merge_requests_page_calls == [
        (99, 1, "all", 100),
        (99, 2, "all", 100),
    ]


class TestGitLabBackfillFeedsComplexityReadiness:
    """CHAOS-2888 Workstream D: mirrors the GitHub-side regression in
    ``tests/test_github_content_backfill.py`` -- proves persisted
    GitLab-backfilled ``git_files`` content satisfies the complexity job's
    readiness contract end-to-end."""

    @staticmethod
    def _store() -> Mock:
        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=False)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=True)
        return store

    @pytest.mark.asyncio
    async def test_gitlab_scanner_backfilled_contents_satisfy_complexity_job(
        self, monkeypatch
    ):
        store = self._store()
        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)

        project = Mock()
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/alpha.py"},
            {"type": "blob", "path": "src/beta.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={
                "src/alpha.py": "def alpha():\n    return 1\n",
                "src/beta.py": (
                    "def beta(x):\n    if x:\n        return x\n    return 0\n"
                ),
            },
            tree_items=tree_items,
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=db_repo.id,
            db_url="clickhouse://localhost:8123/default",
            date=date(2026, 6, 12),
            backfill_days=1,
            language_globs=None,
            max_files=None,
            org_id="test-org",
        )

        assert rc == 0
        assert fake_client.get_file_contents_calls == [
            ("group/proj", ("src/alpha.py", "src/beta.py"), "main", 1_000_000)
        ]
        assert {snap.file_path for snap in ch_sink.snapshots} == {
            "src/alpha.py",
            "src/beta.py",
        }
        assert sum(snap.functions_count for snap in ch_sink.snapshots) == 2
        assert len(ch_sink.dailies) == 1
        daily = ch_sink.dailies[0]
        assert daily.day == date(2026, 6, 12)
        assert daily.org_id == "test-org"
        assert daily.loc_total > 0
        assert daily.cyclomatic_total > 0

    @pytest.mark.asyncio
    async def test_gitlab_paths_only_records_do_not_satisfy_complexity_job(
        self, monkeypatch
    ):
        """A GitLab sync that discovered file paths but whose content fetch
        degraded to empty (content backfill not effectively run) must NOT
        let the complexity job silently report success -- it must fail
        loudly per the job's existing readiness contract."""
        store = self._store()
        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)

        project = Mock()
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/alpha.py"},
            {"type": "blob", "path": "src/beta.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={},
            tree_items=tree_items,
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        assert [f.contents for f in written] == [None, None]

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=db_repo.id,
            db_url="clickhouse://localhost:8123/default",
            date=date(2026, 6, 12),
            backfill_days=1,
            language_globs=None,
            max_files=None,
            org_id="test-org",
        )

        assert rc == 1
        assert not ch_sink.snapshots
        assert not ch_sink.dailies

    @pytest.mark.asyncio
    async def test_partial_content_backfill_still_computes_complexity_for_available_files(
        self, monkeypatch
    ):
        """A GitLab project where content backfill only hydrated some paths
        (mixed content / paths-only rows) must still compute complexity from
        the files that DO have contents. This exercises the complexity job's
        missing-paths query and git_blame usable-line-text probe branches
        that the full-content and empty-content cases above never reach."""
        store = self._store()
        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)

        project = Mock()
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/alpha.py"},
            {"type": "blob", "path": "src/beta.py"},
            {"type": "blob", "path": "src/gamma.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={
                "src/alpha.py": "def alpha():\n    return 1\n",
                "src/beta.py": (
                    "def beta(x):\n    if x:\n        return x\n    return 0\n"
                ),
            },
            tree_items=tree_items,
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        by_path = {f.path: f.contents for f in written}
        assert by_path == {
            "src/alpha.py": "def alpha():\n    return 1\n",
            "src/beta.py": "def beta(x):\n    if x:\n        return x\n    return 0\n",
            "src/gamma.py": None,
        }

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=db_repo.id,
            db_url="clickhouse://localhost:8123/default",
            date=date(2026, 6, 12),
            backfill_days=1,
            language_globs=None,
            max_files=None,
            org_id="test-org",
        )

        assert rc == 0
        assert {snap.file_path for snap in ch_sink.snapshots} == {
            "src/alpha.py",
            "src/beta.py",
        }
        assert len(ch_sink.dailies) == 1
        assert ch_sink.dailies[0].loc_total > 0

    @pytest.mark.asyncio
    async def test_backfill_days_greater_than_one_does_not_fabricate_historical_rows(
        self, monkeypatch
    ):
        """CHAOS-2850/2888: even when a historical multi-day window is
        requested, the job has no historical content snapshot store, so it
        must write exactly ONE ``repo_complexity_daily`` row -- for the
        requested ``date`` only -- never ``backfill_days`` duplicate flat
        rows that would fabricate a misleading historical trend."""
        store = self._store()
        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)

        project = Mock()
        connector = Mock()
        connector.gitlab.projects.get.return_value = project

        tree_items = [
            {"type": "blob", "path": "src/alpha.py"},
            {"type": "blob", "path": "src/beta.py"},
        ]
        fake_client = _FakeGitLabCodeClientForFiles(
            contents={
                "src/alpha.py": "def alpha():\n    return 1\n",
                "src/beta.py": (
                    "def beta(x):\n    if x:\n        return x\n    return 0\n"
                ),
            },
            tree_items=tree_items,
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_gitlab_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            project_full_name="group/proj",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=db_repo.id,
            db_url="clickhouse://localhost:8123/default",
            date=date(2026, 6, 12),
            backfill_days=5,
            language_globs=None,
            max_files=None,
            org_id="test-org",
        )

        assert rc == 0
        assert len(ch_sink.dailies) == 1
        assert ch_sink.dailies[0].day == date(2026, 6, 12)
        assert len({snap.as_of_day for snap in ch_sink.snapshots}) == 1
