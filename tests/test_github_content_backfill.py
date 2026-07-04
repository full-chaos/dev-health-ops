"""Tests for API-based file-content backfill (no local checkout).

Covers the scanner-driven path selection in
``processors.github._fetch_scannable_contents``, content propagation through
``processors.base_git.backfill_file_records``, and the complexity job's
loud-failure exit when an org has no scannable contents at all.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

# Initialize the connectors package before processors.github to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.metrics.job_complexity_db as job
from dev_health_ops.exceptions import RateLimitException
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


class TestCommitStatsBackfill:
    @staticmethod
    def _stat(commit_hash: str, repo_id: object):
        from dev_health_ops.models.git import GitCommitStat

        return GitCommitStat(
            repo_id=repo_id,
            commit_hash=commit_hash,
            file_path=f"{commit_hash}.py",
            additions=1,
            deletions=0,
            old_file_mode="unknown",
            new_file_mode="unknown",
        )

    @pytest.mark.asyncio
    async def test_backfill_commit_stats_uses_window_and_persists_when_cap_not_hit(
        self, monkeypatch
    ):
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _backfill_github_missing_data

        monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "2")
        since = datetime(2026, 1, 10, tzinfo=timezone.utc)
        until = datetime(2026, 1, 12, tzinfo=timezone.utc)

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=True)

        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock(side_effect=insert)

        commits = [SimpleNamespace(sha=f"sha-{idx}") for idx in range(2)]
        fetch_args = []
        stats_args = []

        async def fake_fetch(
            connector_arg,
            owner,
            repo_name,
            repo_id,
            max_commits,
            since_arg,
            until_arg,
            usage_sink,
        ):
            fetch_args.append((owner, repo_name, max_commits, since_arg, until_arg))
            return commits, [], False

        async def fake_stats(
            connector_arg,
            owner,
            repo_name,
            raw_commits,
            repo_id,
            max_stats,
            since_arg,
            usage_sink,
        ):
            stats_args.append((owner, repo_name, max_stats, since_arg))
            return [
                self._stat(commit.sha, repo_id) for commit in raw_commits[:max_stats]
            ]

        monkeypatch.setattr(github, "_fetch_github_commits_async", fake_fetch)
        monkeypatch.setattr(github, "_fetch_github_commit_stats_async", fake_stats)

        gh_repo = Mock()

        connector = Mock()
        connector.github.get_repo.return_value = gh_repo

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
            include_files=False,
            include_blame=False,
            since=since,
            until=until,
        )

        assert fetch_args == [("octo", "repo", None, since, until)]
        assert stats_args == [("octo", "repo", 2, since)]
        assert [row.commit_hash for row in written] == ["sha-0", "sha-1"]

    @pytest.mark.asyncio
    async def test_backfill_commit_stats_over_cap_writes_nothing(self, monkeypatch):
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _backfill_github_missing_data

        monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "2")
        since = datetime(2026, 1, 10, tzinfo=timezone.utc)
        until = datetime(2026, 1, 12, tzinfo=timezone.utc)

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=True)

        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock()

        commits = [SimpleNamespace(sha=f"sha-{idx}") for idx in range(3)]
        stats_calls = []

        async def fake_fetch(*args, **kwargs):
            return commits, [], False

        async def fake_stats(*args, **kwargs):
            stats_calls.append((args, kwargs))
            return []

        monkeypatch.setattr(github, "_fetch_github_commits_async", fake_fetch)
        monkeypatch.setattr(github, "_fetch_github_commit_stats_async", fake_stats)

        gh_repo = Mock()

        connector = Mock()
        connector.github.get_repo.return_value = gh_repo

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
            include_files=False,
            include_blame=False,
            since=since,
            until=until,
        )

        assert stats_calls == []
        sink.insert_git_commit_stats.assert_not_called()

    @pytest.mark.asyncio
    async def test_backfill_commit_stats_rate_limit_propagates_without_partial_write(
        self, monkeypatch
    ):
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _backfill_github_missing_data

        monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "3")
        since = datetime(2026, 1, 10, tzinfo=timezone.utc)
        until = datetime(2026, 1, 12, tzinfo=timezone.utc)

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=True)

        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock()

        commits = [SimpleNamespace(sha="sha-0"), SimpleNamespace(sha="sha-1")]

        async def fake_fetch(*args, **kwargs):
            return commits, [], False

        async def fake_stats(*args, **kwargs):
            raise RateLimitException("limited", retry_after_seconds=42.0)

        monkeypatch.setattr(github, "_fetch_github_commits_async", fake_fetch)
        monkeypatch.setattr(github, "_fetch_github_commit_stats_async", fake_stats)

        gh_repo = Mock()

        connector = Mock()
        connector.github.get_repo.return_value = gh_repo

        db_repo = Mock()
        db_repo.id = uuid.uuid4()

        with pytest.raises(RateLimitException) as exc_info:
            await _backfill_github_missing_data(
                store=store,
                ingestion_sink=sink,
                connector=connector,
                db_repo=db_repo,
                repo_full_name="octo/repo",
                default_branch="main",
                max_commits=None,
                include_files=False,
                include_blame=False,
                since=since,
                until=until,
            )

        assert exc_info.value.retry_after_seconds == 42.0
        sink.insert_git_commit_stats.assert_not_called()

    @pytest.mark.asyncio
    async def test_backfill_commit_stats_over_cap_still_runs_blame(self, monkeypatch):
        from dev_health_ops.connectors.models import BlameRange, FileBlame
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _backfill_github_missing_data

        monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "2")
        since = datetime(2026, 1, 10, tzinfo=timezone.utc)
        until = datetime(2026, 1, 12, tzinfo=timezone.utc)

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=False)
        store.get_blamed_paths = AsyncMock(return_value=set())

        blame_rows = []

        async def insert_blame(batch):
            blame_rows.extend(batch)

        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock()
        sink.insert_blame_data = AsyncMock(side_effect=insert_blame)

        commits = [SimpleNamespace(sha=f"sha-{idx}") for idx in range(3)]
        stats_calls = []

        async def fake_fetch(*args, **kwargs):
            return commits, [], False

        async def fake_stats(*args, **kwargs):
            stats_calls.append((args, kwargs))
            return []

        monkeypatch.setattr(github, "_fetch_github_commits_async", fake_fetch)
        monkeypatch.setattr(github, "_fetch_github_commit_stats_async", fake_stats)

        gh_repo = Mock()
        gh_repo.get_branch.return_value = Mock(commit=Mock(sha="tree-sha"))
        gh_repo.get_git_tree.return_value = Mock(tree=[_FakeTreeEntry("src/app.py")])

        connector = Mock()
        connector.github.get_repo.return_value = gh_repo
        connector.get_file_blame.return_value = FileBlame(
            file_path="src/app.py",
            ranges=[
                BlameRange(
                    starting_line=1,
                    ending_line=1,
                    commit_sha="sha-0",
                    author="A",
                    author_email="a@example.com",
                    age_seconds=0,
                )
            ],
        )

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
            include_files=False,
            include_blame=True,
            since=since,
            until=until,
        )

        sink.insert_git_commit_stats.assert_not_called()
        assert stats_calls == []
        connector.get_file_blame.assert_called_once_with(
            owner="octo",
            repo="repo",
            path="src/app.py",
            ref="main",
        )
        assert [row.path for row in blame_rows] == ["src/app.py"]

    @pytest.mark.asyncio
    async def test_backfill_commit_stats_full_history_writes_capped_sample(
        self, monkeypatch
    ):
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _backfill_github_missing_data

        monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "2")

        store = Mock()
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=False)
        store.has_any_git_blame = AsyncMock(return_value=True)

        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_commit_stats = AsyncMock(side_effect=insert)

        commits = [SimpleNamespace(sha=f"sha-{idx}") for idx in range(3)]
        stats_args = []

        async def fake_fetch(*args, **kwargs):
            return commits, [], False

        async def fake_stats(
            connector_arg,
            owner,
            repo_name,
            raw_commits,
            repo_id,
            max_stats,
            since_arg,
            usage_sink,
        ):
            stats_args.append(max_stats)
            return [
                self._stat(commit.sha, repo_id) for commit in raw_commits[:max_stats]
            ]

        monkeypatch.setattr(github, "_fetch_github_commits_async", fake_fetch)
        monkeypatch.setattr(github, "_fetch_github_commit_stats_async", fake_stats)

        gh_repo = Mock()

        connector = Mock()
        connector.github.get_repo.return_value = gh_repo

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
            include_files=False,
            include_blame=False,
        )

        assert stats_args == [2]
        assert [row.commit_hash for row in written] == ["sha-0", "sha-1"]
