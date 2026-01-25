from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import Mock

import pytest

from dev_health_ops.processors.local import process_git_commits
from dev_health_ops.utils import collect_changed_files, iter_commits_since


class DummyPerson:
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email


class DummyCommit:
    def __init__(self, sha: str, committed_at: datetime):
        self.hexsha = sha
        self.message = f"commit {sha}"
        self.author = DummyPerson("Author", "author@example.com")
        self.committer = DummyPerson("Committer", "committer@example.com")
        self.authored_datetime = committed_at
        self.committed_datetime = committed_at
        self.parents: List = []

    def diff(self, *_args, **_kwargs):
        return []


def test_iter_commits_since_filters_old_commits():
    now = datetime.now(timezone.utc)
    recent = DummyCommit("new", now)
    old = DummyCommit("old", now - timedelta(days=5))

    repo = Mock()
    repo.iter_commits.return_value = [recent, old]

    since = now - timedelta(days=1)
    commits = list(iter_commits_since(repo, since))

    assert commits == [recent]


@pytest.mark.asyncio
async def test_process_git_commits_respects_since(monkeypatch):
    now = datetime.now(timezone.utc)
    recent = DummyCommit("new", now)
    old = DummyCommit("old", now - timedelta(days=10))

    repo = Mock()
    repo.id = uuid.uuid4()

    inserted = []

    class DummyStore:
        async def insert_git_commit_data(self, data):
            inserted.extend(data)

    await process_git_commits(
        repo,
        DummyStore(),
        commits=[recent, old],
        since=now - timedelta(days=1),
    )

    assert [c.hash for c in inserted] == ["new"]


def test_collect_changed_files_tracks_paths(tmp_path):
    now = datetime.now(timezone.utc)
    tracked_file = tmp_path / "src" / "file.py"
    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    tracked_file.write_text("print('hi')")

    class Diff:
        def __init__(self, path: str):
            self.a_path = None
            self.b_path = path
            self.diff = b""

    class CommitWithDiff(DummyCommit):
        def __init__(self, sha: str, committed_at: datetime, path: str):
            super().__init__(sha, committed_at)
            self._path = path

        def diff(self, *_args, **_kwargs):
            return [Diff(self._path)]

    commits = [
        CommitWithDiff("new", now, "src/file.py"),
        CommitWithDiff("missing", now, "deleted.txt"),
    ]

    paths = collect_changed_files(tmp_path, commits)

    assert paths == [tracked_file]
