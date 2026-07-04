"""Tests for GitLab API-based file-content backfill (no local checkout).

Mirrors tests/test_github_content_backfill.py: covers the batched GraphQL
blob fetch on the connector, the scanner-driven path selection in
``processors.gitlab._fetch_scannable_contents``, and the paths-only repo
upgrade through ``_backfill_gitlab_missing_data``.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Initialize the connectors package before processors.gitlab to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.connectors import GitLabConnector
from dev_health_ops.connectors.models import BlameRange, FileBlame
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.processors import gitlab as gitlab_processor
from dev_health_ops.processors.gitlab import (
    _backfill_gitlab_missing_data,
    _fetch_scannable_contents,
    _sync_gitlab_mrs_to_store,
)
from dev_health_ops.providers.gitlab.code_client import (
    GitLabCommitData,
    GitLabCommitStatsData,
)


class _FakeGraphQLResponse:
    def __init__(self, nodes, status_code=200):
        self.status_code = status_code
        self._nodes = nodes
        self.headers = {}
        self.text = ""

    def json(self):
        return {"data": {"project": {"repository": {"blobs": {"nodes": self._nodes}}}}}


class TestGetFileContents:
    @pytest.fixture
    def mock_gitlab_client(self):
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def test_size_pass_filters_oversized_before_text_fetch(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Pass 1 fetches only rawSize; oversized blobs never get a text fetch."""
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        responses = [
            # Pass 1: sizes
            _FakeGraphQLResponse(
                [
                    {"path": "src/a.py", "rawSize": 7},
                    {"path": "img/logo.png", "rawSize": 5000},
                    {"path": "big.py", "rawSize": 2_000_000},
                ]
            ),
            # Pass 2: text for survivors only
            _FakeGraphQLResponse(
                [
                    {"path": "src/a.py", "rawTextBlob": "x = 1\n"},
                    {"path": "img/logo.png", "rawTextBlob": None},
                ]
            ),
        ]
        with patch(
            "dev_health_ops.connectors.gitlab.requests.post",
            side_effect=responses,
        ) as post:
            result = connector.get_file_contents(
                "group/proj",
                ["src/a.py", "img/logo.png", "big.py"],
                ref="main",
            )

        assert result == {"src/a.py": "x = 1\n"}
        assert post.call_count == 2
        size_query = post.call_args_list[0].kwargs["json"]["query"]
        assert "rawSize" in size_query and "rawTextBlob" not in size_query
        text_call = post.call_args_list[1].kwargs["json"]
        assert "rawTextBlob" in text_call["query"]
        # big.py was filtered by the size pass
        assert text_call["variables"]["paths"] == ["src/a.py", "img/logo.png"]
        assert post.call_args_list[0].kwargs["headers"]["PRIVATE-TOKEN"] == "tok"
        assert (
            post.call_args_list[0].args[0] == "https://gitlab.example.com/api/graphql"
        )

    def test_no_size_pass_when_max_bytes_disabled(
        self, mock_gitlab_client, mock_rest_client
    ):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        response = _FakeGraphQLResponse([{"path": "a.py", "rawTextBlob": "a"}])
        with patch(
            "dev_health_ops.connectors.gitlab.requests.post",
            return_value=response,
        ) as post:
            result = connector.get_file_contents(
                "group/proj", ["a.py"], ref="main", max_bytes=None
            )

        assert result == {"a.py": "a"}
        post.assert_called_once()
        assert "rawTextBlob" in post.call_args.kwargs["json"]["query"]

    def test_chunks_by_batch_size(self, mock_gitlab_client, mock_rest_client):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        responses = [
            # size pass, two chunks
            _FakeGraphQLResponse([{"path": "a.py", "rawSize": 1}]),
            _FakeGraphQLResponse([{"path": "b.py", "rawSize": 1}]),
            # text pass, two chunks
            _FakeGraphQLResponse([{"path": "a.py", "rawTextBlob": "a"}]),
            _FakeGraphQLResponse([{"path": "b.py", "rawTextBlob": "b"}]),
        ]
        with patch(
            "dev_health_ops.connectors.gitlab.requests.post",
            side_effect=responses,
        ) as post:
            result = connector.get_file_contents(
                "group/proj",
                ["a.py", "b.py"],
                ref="main",
                batch_size=1,
                max_bytes=1_000_000,
            )

        assert result == {"a.py": "a", "b.py": "b"}
        assert post.call_count == 4

    def test_size_pass_failure_degrades_to_unfiltered_text_fetch(
        self, mock_gitlab_client, mock_rest_client
    ):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        get_blobs = Mock(
            side_effect=[
                RuntimeError("size pass boom"),
                [{"path": "a.py", "rawTextBlob": "a"}],
            ]
        )
        with patch.object(connector, "_graphql_blobs", get_blobs):
            result = connector.get_file_contents("group/proj", ["a.py"], ref="main")

        assert result == {"a.py": "a"}
        # Second call is the text pass over the un-filtered chunk.
        assert get_blobs.call_args_list[1].args[3] == "path rawTextBlob"

    def test_text_pass_chunk_failure_keeps_earlier_results(
        self, mock_gitlab_client, mock_rest_client
    ):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        get_blobs = Mock(
            side_effect=[
                [{"path": "a.py", "rawSize": 1}],
                [{"path": "b.py", "rawSize": 1}],
                [{"path": "a.py", "rawTextBlob": "a"}],
                RuntimeError("late chunk rate limited"),
            ]
        )
        with patch.object(connector, "_graphql_blobs", get_blobs):
            result = connector.get_file_contents(
                "group/proj",
                ["a.py", "b.py"],
                ref="main",
                batch_size=1,
                max_bytes=1_000_000,
            )

        assert result == {"a.py": "a"}

    def test_empty_paths_makes_no_request(self, mock_gitlab_client, mock_rest_client):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        with patch("dev_health_ops.connectors.gitlab.requests.post") as post:
            assert connector.get_file_contents("group/proj", []) == {}
        post.assert_not_called()


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
        observations=None,
    ):
        self.contents = contents or {}
        self.contents_error = contents_error
        self.blame_by_path = blame_by_path or {}
        self.blame_error_paths = blame_error_paths or {}
        self.observations = observations or []
        self.get_file_contents_calls: list[Any] = []
        self.get_file_blame_calls: list[Any] = []

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
        return self.blame_by_path.get(file_path, FileBlame(file_path=file_path))

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

        fake_client = _FakeGitLabCodeClientForFiles(contents={"src/app.py": "x = 1\n"})
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        tree_items = [
            {"type": "blob", "path": "src/app.py"},
            {"type": "blob", "path": "README.md"},
            {"type": "tree", "path": "src"},
        ]
        with patch(
            "dev_health_ops.processors.gitlab._iter_gitlab_repo_tree",
            return_value=tree_items,
        ):
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
        self.get_commit_stats_calls: list[Any] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get_commits(
        self, project_id, *, max_commits, since=None, until=None, per_page=100
    ):
        self.get_commits_calls.append(project_id)
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

        fake_client = _FakeGitLabCodeClientForFiles(
            blame_by_path={
                "src/a.py": FileBlame(
                    file_path="src/a.py",
                    ranges=[BlameRange(1, 2, "sha1", "Ada", "ada@example.com", 0)],
                ),
                "src/b.py": FileBlame(
                    file_path="src/b.py",
                    ranges=[BlameRange(1, 1, "sha2", "Grace", "grace@example.com", 0)],
                ),
            },
            observations=[{"route_family": "project"}],
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()
        usage_sink: list[dict[str, Any]] = []

        tree_items = [
            {"type": "blob", "path": "src/a.py"},
            {"type": "blob", "path": "src/b.py"},
        ]
        with patch(
            "dev_health_ops.processors.gitlab._iter_gitlab_repo_tree",
            return_value=tree_items,
        ):
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

        assert {(pc[1]) for pc in fake_client.get_file_blame_calls} == {
            "src/a.py",
            "src/b.py",
        }
        assert connector.rest_client.get_file_blame.call_count == 0
        by_path = {row.path: row for row in written}
        assert by_path["src/a.py"].commit_hash == "sha1"
        assert by_path["src/a.py"].author_name == "Ada"
        assert by_path["src/b.py"].commit_hash == "sha2"
        assert usage_sink == [{"route_family": "project"}]

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

        fake_client = _FakeGitLabCodeClientForFiles(
            blame_by_path={
                "src/b.py": FileBlame(
                    file_path="src/b.py",
                    ranges=[BlameRange(1, 1, "sha2", "Grace", "", 0)],
                ),
            },
            blame_error_paths={"src/a.py": RuntimeError("boom")},
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        tree_items = [
            {"type": "blob", "path": "src/a.py"},
            {"type": "blob", "path": "src/b.py"},
        ]
        with patch(
            "dev_health_ops.processors.gitlab._iter_gitlab_repo_tree",
            return_value=tree_items,
        ):
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

        fake_client = _FakeGitLabCodeClientForFiles(
            blame_error_paths={"src/a.py": RateLimitException("rate limited")},
            observations=[{"route_family": "project"}],
        )
        monkeypatch.setattr(
            "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
            lambda connector: fake_client,
        )

        db_repo = Mock()
        db_repo.id = uuid.uuid4()
        usage_sink: list[dict[str, Any]] = []

        tree_items = [
            {"type": "blob", "path": "src/a.py"},
            {"type": "blob", "path": "src/b.py"},
        ]
        with patch(
            "dev_health_ops.processors.gitlab._iter_gitlab_repo_tree",
            return_value=tree_items,
        ):
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
