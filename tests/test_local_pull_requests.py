from __future__ import annotations

import uuid
from datetime import datetime, timezone

from dev_health_ops.processors.local import infer_merged_pull_requests_from_commits


class DummyPerson:
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email


class DummyCommit:
    def __init__(self, message: str, committed_at: datetime):
        self.hexsha = "deadbeef"
        self.message = message
        self.author = DummyPerson("Alice", "alice@example.com")
        self.committer = DummyPerson("Alice", "alice@example.com")
        self.committed_datetime = committed_at
        self.parents = [object(), object()]


def test_infer_github_merged_pr_from_merge_commit_message():
    now = datetime.now(timezone.utc)
    message = """Merge pull request #123 from org/branch

Add feature X

Some body text
"""
    prs = infer_merged_pull_requests_from_commits(
        [DummyCommit(message, now)],
        repo_id=uuid.uuid4(),
    )
    assert len(prs) == 1
    assert prs[0].number == 123
    assert prs[0].state == "merged"
    assert prs[0].title == "Add feature X"
    assert prs[0].created_at is not None
    assert prs[0].merged_at is not None


def test_infer_gitlab_merged_mr_from_trailer():
    now = datetime.now(timezone.utc)
    message = """Merge branch 'feature' into 'main'

My MR title

See merge request group/project!45
"""
    prs = infer_merged_pull_requests_from_commits(
        [DummyCommit(message, now)],
        repo_id=uuid.uuid4(),
    )
    assert len(prs) == 1
    assert prs[0].number == 45
    assert prs[0].state == "merged"
    assert prs[0].title == "My MR title"
