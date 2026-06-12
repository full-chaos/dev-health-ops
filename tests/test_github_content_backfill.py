"""Tests for API-based file-content backfill (no local checkout).

Covers the scanner-driven path selection in
``processors.github._fetch_scannable_contents``, content propagation through
``processors.base_git.backfill_file_records``, and the complexity job's
loud-failure exit when an org has no scannable contents at all.
"""

from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

# Initialize the connectors package before processors.github to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.metrics.job_complexity_db as job
from dev_health_ops.processors.base_git import backfill_file_records
from dev_health_ops.processors.github import (
    CONTENT_FETCH_MAX_BYTES,
    _fetch_scannable_contents,
)


class TestFetchScannableContents:
    @pytest.mark.asyncio
    async def test_filters_by_scanner_globs_and_size(self):
        connector = Mock()
        connector.get_file_contents = Mock(return_value={"src/app.py": "x = 1\n"})

        file_paths = [
            "src/app.py",
            "README.md",
            "lib/tests/test_app.py",
            "pkg/__init__.py",
            "src/huge.py",
        ]
        blob_sizes: dict[str, int | None] = {
            "src/app.py": 120,
            "README.md": 50,
            "lib/tests/test_app.py": 80,
            "pkg/__init__.py": 10,
            "src/huge.py": CONTENT_FETCH_MAX_BYTES + 1,
        }

        result = await _fetch_scannable_contents(
            connector, "octo", "repo", "main", file_paths, blob_sizes, "octo/repo"
        )

        assert result == {"src/app.py": "x = 1\n"}
        connector.get_file_contents.assert_called_once_with(
            "octo", "repo", ["src/app.py"], ref="main"
        )

    @pytest.mark.asyncio
    async def test_no_scannable_paths_skips_api(self):
        connector = Mock()
        connector.get_file_contents = Mock()

        result = await _fetch_scannable_contents(
            connector, "octo", "repo", "main", ["README.md"], {}, "octo/repo"
        )

        assert result == {}
        connector.get_file_contents.assert_not_called()

    @pytest.mark.asyncio
    async def test_api_error_degrades_to_empty(self):
        connector = Mock()
        connector.get_file_contents = Mock(side_effect=RuntimeError("boom"))

        result = await _fetch_scannable_contents(
            connector, "octo", "repo", "main", ["src/app.py"], {}, "octo/repo"
        )

        assert result == {}


class TestBackfillFileRecordsContents:
    @pytest.mark.asyncio
    async def test_writes_contents_for_known_paths(self):
        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        repo_id = uuid.uuid4()

        await backfill_file_records(
            sink,
            repo_id,
            ["src/app.py", "README.md"],
            "octo/repo",
            contents_by_path={"src/app.py": "x = 1\n"},
        )

        by_path = {f.path: f.contents for f in written}
        assert by_path == {"src/app.py": "x = 1\n", "README.md": None}

    @pytest.mark.asyncio
    async def test_defaults_to_null_contents(self):
        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)

        await backfill_file_records(sink, uuid.uuid4(), ["a.py"], "octo/repo")

        assert [f.contents for f in written] == [None]


class _EmptyClickHouseClient:
    def query(self, query, parameters=None):
        class _Result:
            result_rows = []

        result = _Result()
        if "count()" in query and "FROM git_files" in query:
            result.result_rows = [[0, 0]]
        elif "maxOrNull" in query:
            result.result_rows = [[None]]
        return result


class _EmptySink:
    def __init__(self):
        self.client = _EmptyClickHouseClient()
        self.snapshots = []
        self.dailies = []

    def ensure_tables(self):
        return None

    def write_file_complexity_snapshots(self, rows):
        self.snapshots.extend(rows)

    def write_repo_complexity_daily(self, rows):
        self.dailies.extend(rows)

    def close(self):
        return None


def test_complexity_job_returns_nonzero_when_no_repo_has_contents(monkeypatch):
    sink = _EmptySink()
    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)

    rc = job.run_complexity_db_job(
        repo_id=uuid.uuid4(),
        db_url="clickhouse://localhost:8123/default",
        date=date(2026, 6, 12),
        backfill_days=1,
        language_globs=None,
        max_files=None,
        org_id="test-org",
    )

    assert rc == 1
    assert not sink.snapshots
    assert not sink.dailies


class _FakeTreeEntry:
    def __init__(self, path, size=100, type_="blob"):
        self.path = path
        self.size = size
        self.type = type_


class TestPathsOnlyUpgrade:
    @pytest.mark.asyncio
    async def test_backfill_refetches_contents_for_paths_only_repos(self):
        """Repos with paths-only git_files rows (pre-content-sync) get upgraded."""
        from dev_health_ops.processors.github import _backfill_github_missing_data

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

        gh_repo = Mock()
        gh_repo.get_branch.return_value = Mock(commit=Mock(sha="abc"))
        gh_repo.get_git_tree.return_value = Mock(
            tree=[_FakeTreeEntry("src/app.py"), _FakeTreeEntry("README.md")]
        )
        connector = Mock()
        connector.github.get_repo.return_value = gh_repo
        connector.get_file_contents = Mock(return_value={"src/app.py": "x = 1\n"})

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_github_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            repo_full_name="octo/repo",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        by_path = {f.path: f.contents for f in written}
        assert by_path == {"src/app.py": "x = 1\n", "README.md": None}

    @pytest.mark.asyncio
    async def test_backfill_skips_when_contents_already_present(self):
        from dev_health_ops.processors.github import _backfill_github_missing_data

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=True)

        connector = Mock()
        sink = Mock()
        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        await _backfill_github_missing_data(
            store=store,
            ingestion_sink=sink,
            connector=connector,
            db_repo=db_repo,
            repo_full_name="octo/repo",
            default_branch="main",
            max_commits=None,
            include_blame=False,
            include_commit_stats=False,
        )

        connector.github.get_repo.assert_not_called()
        sink.insert_git_file_data.assert_not_called()
