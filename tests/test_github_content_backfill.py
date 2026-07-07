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
from tests._complexity_readiness_fixtures import (
    ComplexityReadinessClient,
    ComplexityReadinessSink,
)


class _FakeCodeClient:
    """Stand-in for ``GitHubCodeClient`` (CHAOS-2773 CS7): the processor now
    fetches files/blame through this client's async ``get_file_contents``/
    ``get_file_blame`` instead of the frozen connector's sync methods, so
    tests double the CLIENT, never ``connector.get_file_contents``/
    ``connector.get_file_blame`` (which the processor no longer calls)."""

    def __init__(
        self,
        *,
        contents=None,
        blame=None,
        issues=None,
        latest_commit_sha="resolved-sha",
        side_effect=None,
    ):
        self.contents = contents if contents is not None else {}
        self.blame = blame
        self.issues = issues if issues is not None else []
        self.latest_commit_sha = latest_commit_sha
        self.side_effect = side_effect
        self.latest_commit_calls: list[tuple[str, str, str, datetime]] = []
        self.file_content_calls: list[tuple[str, str, list[str], str]] = []
        self.file_blame_calls: list[tuple[str, str, str, str]] = []
        self.issue_calls: list[tuple[str, str, str, list[str], int | None]] = []
        self.drain_usage_observations = Mock(return_value=[])
        self.close = AsyncMock()

    async def get_latest_commit_sha(
        self,
        owner: str,
        repo: str,
        *,
        ref: str,
        until: datetime,
    ) -> str | None:
        self.latest_commit_calls.append((owner, repo, ref, until))
        if self.side_effect is not None:
            raise self.side_effect
        return self.latest_commit_sha

    async def get_file_contents(
        self,
        owner: str,
        repo: str,
        paths: list[str],
        *,
        ref: str = "HEAD",
        batch_size: int = 50,
    ) -> dict[str, str]:
        del batch_size
        self.file_content_calls.append((owner, repo, paths, ref))
        if self.side_effect is not None:
            raise self.side_effect
        return self.contents

    async def get_file_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        *,
        ref: str = "HEAD",
    ):
        self.file_blame_calls.append((owner, repo, path, ref))
        if self.side_effect is not None:
            raise self.side_effect
        return self.blame

    async def iter_issues(
        self,
        owner: str,
        repo: str,
        *,
        state: str = "all",
        labels: list[str] | None = None,
        max_issues: int | None = None,
    ):
        self.issue_calls.append((owner, repo, state, labels or [], max_issues))
        if self.side_effect is not None:
            raise self.side_effect
        return self.issues[:max_issues]


class TestFetchScannableContents:
    @pytest.mark.asyncio
    async def test_filters_by_scanner_globs_and_size(self):
        client = _FakeCodeClient(contents={"src/app.py": "x = 1\n"})

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
            client, "octo", "repo", "main", file_paths, blob_sizes, "octo/repo"
        )

        assert result == {"src/app.py": "x = 1\n"}
        assert client.file_content_calls == [("octo", "repo", ["src/app.py"], "main")]

    @pytest.mark.asyncio
    async def test_no_scannable_paths_skips_api(self):
        client = _FakeCodeClient()

        result = await _fetch_scannable_contents(
            client, "octo", "repo", "main", ["README.md"], {}, "octo/repo"
        )

        assert result == {}
        assert client.file_content_calls == []

    @pytest.mark.asyncio
    async def test_api_error_degrades_to_empty(self):
        client = _FakeCodeClient(side_effect=RuntimeError("boom"))

        result = await _fetch_scannable_contents(
            client, "octo", "repo", "main", ["src/app.py"], {}, "octo/repo"
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_rate_limit_propagates_without_degrading(self):
        """CHAOS-2773 CS7: unlike a generic fetch error, a rate limit must NOT
        degrade to empty -- it propagates so the caller's deferral semantics
        apply, exactly like the commit-stats fetch path."""
        client = _FakeCodeClient(
            side_effect=RateLimitException("limited", retry_after_seconds=7.0)
        )

        with pytest.raises(RateLimitException) as exc_info:
            await _fetch_scannable_contents(
                client, "octo", "repo", "main", ["src/app.py"], {}, "octo/repo"
            )

        assert exc_info.value.retry_after_seconds == 7.0


class TestGithubIncidentsClientSeam:
    @pytest.mark.asyncio
    async def test_incidents_fetch_uses_code_client_and_not_legacy_repo(
        self, monkeypatch
    ):
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _fetch_github_incidents_async

        since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        issue = SimpleNamespace(
            issue_id="issue-1",
            state="closed",
            created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            closed_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
        )
        code_client = _FakeCodeClient(issues=[issue])
        monkeypatch.setattr(github, "resolve_incident_labels", lambda: ["incident"])
        monkeypatch.setattr(
            github, "_github_code_client_from_connector", lambda _connector: code_client
        )

        connector = Mock()
        connector.github.get_repo.side_effect = AssertionError(
            "legacy PyGithub repo path must not be called"
        )
        repo_id = uuid.uuid4()
        usage_sink: list[dict[str, object]] = []

        incidents = await _fetch_github_incidents_async(
            connector,
            "octo",
            "repo",
            repo_id,
            10,
            since,
            usage_sink=usage_sink,
        )

        assert [(row.incident_id, row.status, row.started_at) for row in incidents] == [
            ("issue-1", "closed", datetime(2026, 1, 2, tzinfo=timezone.utc))
        ]
        assert code_client.issue_calls == [("octo", "repo", "all", ["incident"], 10)]
        code_client.close.assert_awaited_once()
        connector.github.get_repo.assert_not_called()


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

    @pytest.mark.asyncio
    async def test_preserves_existing_contents_on_paths_only_rewrite(self):
        """CHAOS-2857: a re-backfill that fetched nothing must not shadow stored contents."""
        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        sink.get_git_file_contents_by_path = AsyncMock(
            return_value={"src/app.py": "x = 1\n"}
        )
        repo_id = uuid.uuid4()

        await backfill_file_records(
            sink,
            repo_id,
            ["src/app.py", "README.md"],
            "octo/repo",
            contents_by_path=None,
        )

        sink.get_git_file_contents_by_path.assert_awaited_once_with(repo_id)
        by_path = {f.path: f.contents for f in written}
        assert by_path == {"src/app.py": "x = 1\n", "README.md": None}

    @pytest.mark.asyncio
    async def test_fresh_contents_override_preserved_contents(self):
        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        sink.get_git_file_contents_by_path = AsyncMock(
            return_value={"src/app.py": "old = 0\n"}
        )

        await backfill_file_records(
            sink,
            uuid.uuid4(),
            ["src/app.py"],
            "octo/repo",
            contents_by_path={"src/app.py": "new = 1\n"},
        )

        assert [f.contents for f in written] == ["new = 1\n"]

    @pytest.mark.asyncio
    async def test_preservation_read_failure_degrades_to_paths_only(self):
        written = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        sink.get_git_file_contents_by_path = AsyncMock(side_effect=RuntimeError("boom"))

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
    async def test_backfill_refetches_contents_for_paths_only_repos(self, monkeypatch):
        """Repos with paths-only git_files rows (pre-content-sync) get upgraded."""
        from dev_health_ops.processors import github
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

        code_client = _FakeCodeClient(contents={"src/app.py": "x = 1\n"})
        monkeypatch.setattr(
            github, "_github_code_client_from_connector", lambda _connector: code_client
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
            include_blame=False,
            include_commit_stats=False,
        )

        by_path = {f.path: f.contents for f in written}
        assert by_path == {"src/app.py": "x = 1\n", "README.md": None}
        assert code_client.file_content_calls == [
            ("octo", "repo", ["src/app.py"], "abc")
        ]
        code_client.close.assert_awaited_once()
        connector.get_file_contents.assert_not_called()

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

        code_client = _FakeCodeClient(
            blame=FileBlame(
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
        )
        monkeypatch.setattr(
            github, "_github_code_client_from_connector", lambda _connector: code_client
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
        assert code_client.file_blame_calls == [
            ("octo", "repo", "src/app.py", "resolved-sha")
        ]
        code_client.close.assert_awaited_once()
        connector.get_file_blame.assert_not_called()
        assert [row.path for row in blame_rows] == ["src/app.py"]
        assert [row.line for row in blame_rows] == [None]

    @pytest.mark.asyncio
    async def test_historical_backfill_uses_resolved_ref_for_tree_content_blame_and_complexity(
        self, monkeypatch
    ):
        from dev_health_ops.connectors.models import BlameRange, FileBlame
        from dev_health_ops.processors import github
        from dev_health_ops.processors.github import _backfill_github_missing_data

        until = datetime(2026, 1, 12, tzinfo=timezone.utc)

        store = Mock()
        store.org_id = "test-org"
        store.has_any_git_files = AsyncMock(return_value=True)
        store.has_any_git_file_contents = AsyncMock(return_value=True)
        store.has_any_git_commit_stats = AsyncMock(return_value=True)
        store.has_any_git_blame = AsyncMock(return_value=False)
        store.get_blamed_paths = AsyncMock(return_value={"README.md"})

        written_files = []
        written_blame = []

        async def insert_files(batch):
            written_files.extend(batch)

        async def insert_blame(batch):
            written_blame.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert_files)
        sink.insert_blame_data = AsyncMock(side_effect=insert_blame)

        metrics_sink = ComplexityReadinessSink(ComplexityReadinessClient([]))

        gh_repo = Mock()
        gh_repo.get_git_tree.return_value = Mock(
            tree=[_FakeTreeEntry("src/app.py"), _FakeTreeEntry("README.md")]
        )
        connector = Mock()
        connector.github.get_repo.return_value = gh_repo

        code_client = _FakeCodeClient(
            contents={"src/app.py": "def app():\n    return 1\n"},
            blame=FileBlame(
                file_path="src/app.py",
                ranges=[
                    BlameRange(
                        starting_line=1,
                        ending_line=1,
                        commit_sha="resolved-sha",
                        author="Ada",
                        author_email="ada@example.com",
                        age_seconds=0,
                    )
                ],
            ),
            latest_commit_sha="resolved-sha",
        )
        monkeypatch.setattr(
            github, "_github_code_client_from_connector", lambda _connector: code_client
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
            include_blame=True,
            include_commit_stats=False,
            until=until,
            metrics_sink=metrics_sink,
        )

        assert code_client.latest_commit_calls == [("octo", "repo", "main", until)]
        gh_repo.get_branch.assert_not_called()
        gh_repo.get_git_tree.assert_called_once_with("resolved-sha", recursive=True)
        assert code_client.file_content_calls == [
            ("octo", "repo", ["src/app.py"], "resolved-sha")
        ]
        assert code_client.file_blame_calls == [
            ("octo", "repo", "src/app.py", "resolved-sha")
        ]
        sink.insert_git_file_data.assert_not_called()
        assert written_files == []
        assert [row.commit_hash for row in written_blame] == ["resolved-sha"]
        assert [snap.ref for snap in metrics_sink.snapshots] == ["resolved-sha"]
        assert [snap.as_of_day for snap in metrics_sink.snapshots] == [until.date()]
        assert [daily.day for daily in metrics_sink.dailies] == [until.date()]
        assert metrics_sink.dailies[0].org_id == "test-org"

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


class TestGithubBackfillFeedsComplexityReadiness:
    """CHAOS-2888 Workstream D: the whole point of scanner-driven content
    backfill (CHAOS-2859) is that persisted ``git_files`` rows are enough for
    ``job_complexity_db.run_complexity_db_job`` to succeed. These regression
    tests wire the GitHub-specific fetch+persist path directly into the
    complexity job's readiness contract, so a change on either side that
    breaks the contract fails loudly here -- not only in production."""

    @pytest.mark.asyncio
    async def test_github_scanner_backfilled_contents_satisfy_complexity_job(
        self, monkeypatch
    ):
        client = _FakeCodeClient(
            contents={
                "src/alpha.py": "def alpha():\n    return 1\n",
                "src/beta.py": (
                    "def beta(x):\n    if x:\n        return x\n    return 0\n"
                ),
            }
        )
        file_paths = ["src/alpha.py", "src/beta.py"]
        blob_sizes: dict[str, int | None] = {"src/alpha.py": 40, "src/beta.py": 70}

        contents_by_path = await _fetch_scannable_contents(
            client, "octo", "repo", "main", file_paths, blob_sizes, "octo/repo"
        )

        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        repo_id = uuid.uuid4()

        await backfill_file_records(
            sink,
            repo_id,
            file_paths,
            "octo/repo",
            contents_by_path=contents_by_path,
        )

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=repo_id,
            db_url="clickhouse://localhost:8123/default",
            date=date(2026, 6, 12),
            backfill_days=1,
            language_globs=None,
            max_files=None,
            org_id="test-org",
        )

        assert rc == 0
        assert client.file_content_calls == [
            ("octo", "repo", ["src/alpha.py", "src/beta.py"], "main")
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
    async def test_github_paths_only_records_do_not_satisfy_complexity_job(
        self, monkeypatch
    ):
        """A GitHub sync that has only written path rows (content backfill
        not yet run, CHAOS-2859) must NOT let the complexity job silently
        report success -- it must fail loudly per the job's existing
        readiness contract."""
        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        repo_id = uuid.uuid4()

        await backfill_file_records(
            sink, repo_id, ["src/alpha.py", "src/beta.py"], "octo/repo"
        )

        assert [f.contents for f in written] == [None, None]

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=repo_id,
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
        """A repo where content backfill only hydrated some paths (mixed
        content / paths-only rows -- e.g. an in-progress CHAOS-2859 upgrade)
        must still compute complexity from the files that DO have contents.
        This exercises the complexity job's missing-paths query and
        git_blame usable-line-text probe branches that the full-content and
        empty-content cases above never reach."""
        client = _FakeCodeClient(
            contents={
                "src/alpha.py": "def alpha():\n    return 1\n",
                "src/beta.py": (
                    "def beta(x):\n    if x:\n        return x\n    return 0\n"
                ),
            }
        )
        file_paths = ["src/alpha.py", "src/beta.py", "src/gamma.py"]
        blob_sizes: dict[str, int | None] = {
            "src/alpha.py": 40,
            "src/beta.py": 70,
            "src/gamma.py": 30,
        }

        contents_by_path = await _fetch_scannable_contents(
            client, "octo", "repo", "main", file_paths, blob_sizes, "octo/repo"
        )

        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        repo_id = uuid.uuid4()

        await backfill_file_records(
            sink,
            repo_id,
            file_paths,
            "octo/repo",
            contents_by_path=contents_by_path,
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
            repo_id=repo_id,
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
        client = _FakeCodeClient(
            contents={
                "src/alpha.py": "def alpha():\n    return 1\n",
                "src/beta.py": (
                    "def beta(x):\n    if x:\n        return x\n    return 0\n"
                ),
            }
        )
        file_paths = ["src/alpha.py", "src/beta.py"]
        blob_sizes: dict[str, int | None] = {"src/alpha.py": 40, "src/beta.py": 70}

        contents_by_path = await _fetch_scannable_contents(
            client, "octo", "repo", "main", file_paths, blob_sizes, "octo/repo"
        )

        written: list = []

        async def insert(batch):
            written.extend(batch)

        sink = Mock()
        sink.insert_git_file_data = AsyncMock(side_effect=insert)
        repo_id = uuid.uuid4()

        await backfill_file_records(
            sink,
            repo_id,
            file_paths,
            "octo/repo",
            contents_by_path=contents_by_path,
        )

        ch_client = ComplexityReadinessClient([(f.path, f.contents) for f in written])
        ch_sink = ComplexityReadinessSink(ch_client)
        monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: ch_sink)

        rc = job.run_complexity_db_job(
            repo_id=repo_id,
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
