"""Tests for GitLab API-based file-content backfill (no local checkout).

Mirrors tests/test_github_content_backfill.py: covers the batched GraphQL
blob fetch on the connector, the scanner-driven path selection in
``processors.gitlab._fetch_scannable_contents``, and the paths-only repo
upgrade through ``_backfill_gitlab_missing_data``.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Initialize the connectors package before processors.gitlab to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.connectors import GitLabConnector
from dev_health_ops.processors.gitlab import (
    _backfill_gitlab_missing_data,
    _fetch_scannable_contents,
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

    def test_batches_paths_and_filters_binary_and_oversized(
        self, mock_gitlab_client, mock_rest_client
    ):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        response = _FakeGraphQLResponse(
            [
                {"path": "src/a.py", "rawTextBlob": "x = 1\n", "rawSize": 7},
                {"path": "img/logo.png", "rawTextBlob": None, "rawSize": 5000},
                {"path": "big.py", "rawTextBlob": "huge", "rawSize": 2_000_000},
            ]
        )
        with patch(
            "dev_health_ops.connectors.gitlab.requests.post",
            return_value=response,
        ) as post:
            result = connector.get_file_contents(
                "group/proj",
                ["src/a.py", "img/logo.png", "big.py"],
                ref="main",
            )

        assert result == {"src/a.py": "x = 1\n"}
        post.assert_called_once()
        _, kwargs = post.call_args
        assert post.call_args.args[0] == "https://gitlab.example.com/api/graphql"
        assert kwargs["json"]["variables"] == {
            "fullPath": "group/proj",
            "ref": "main",
            "paths": ["src/a.py", "img/logo.png", "big.py"],
        }
        assert kwargs["headers"]["PRIVATE-TOKEN"] == "tok"

    def test_chunks_by_batch_size(self, mock_gitlab_client, mock_rest_client):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        responses = [
            _FakeGraphQLResponse([{"path": "a.py", "rawTextBlob": "a", "rawSize": 1}]),
            _FakeGraphQLResponse([{"path": "b.py", "rawTextBlob": "b", "rawSize": 1}]),
        ]
        with patch(
            "dev_health_ops.connectors.gitlab.requests.post",
            side_effect=responses,
        ) as post:
            result = connector.get_file_contents(
                "group/proj", ["a.py", "b.py"], ref="main", batch_size=1
            )

        assert result == {"a.py": "a", "b.py": "b"}
        assert post.call_count == 2

    def test_empty_paths_makes_no_request(self, mock_gitlab_client, mock_rest_client):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )
        with patch("dev_health_ops.connectors.gitlab.requests.post") as post:
            assert connector.get_file_contents("group/proj", []) == {}
        post.assert_not_called()


class TestFetchScannableContents:
    @pytest.mark.asyncio
    async def test_filters_by_scanner_globs(self):
        connector = Mock()
        connector.get_file_contents = Mock(return_value={"src/app.py": "x = 1\n"})

        result = await _fetch_scannable_contents(
            connector,
            "group/proj",
            "main",
            ["src/app.py", "README.md", "pkg/__init__.py"],
        )

        assert result == {"src/app.py": "x = 1\n"}
        connector.get_file_contents.assert_called_once_with(
            "group/proj", ["src/app.py"], ref="main", max_bytes=1_000_000
        )

    @pytest.mark.asyncio
    async def test_api_error_degrades_to_empty(self):
        connector = Mock()
        connector.get_file_contents = Mock(side_effect=RuntimeError("boom"))

        result = await _fetch_scannable_contents(
            connector, "group/proj", "main", ["src/app.py"]
        )

        assert result == {}


class TestGitLabBackfillContents:
    @pytest.mark.asyncio
    async def test_backfill_writes_contents_for_paths_only_repos(self):
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
        connector.get_file_contents = Mock(return_value={"src/app.py": "x = 1\n"})

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
