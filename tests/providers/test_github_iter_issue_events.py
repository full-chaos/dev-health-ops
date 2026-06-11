"""Test GitHubWorkClient.iter_issue_events against real PyGithub vocabulary.

Regression: PyGithub's PullRequest has no get_events() — only Issue does;
PullRequest exposes the same endpoint as get_issue_events(). The fakes here
deliberately mirror that asymmetry (no MagicMock, which would auto-create
any attribute and hide the bug).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient


@pytest.fixture
def client() -> GitHubWorkClient:
    with (
        patch("github.Github"),
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
    ):
        return GitHubWorkClient(auth=GitHubAuth(token="fake"))


class _FakeEvent:
    created_at = None
    event = "closed"
    label = None
    actor = None


class _FakeIssue:
    """Mirrors github.Issue.Issue: has get_events, no get_issue_events."""

    def get_events(self) -> list[_FakeEvent]:
        return [_FakeEvent(), _FakeEvent()]


class _FakePullRequest:
    """Mirrors github.PullRequest.PullRequest: has get_issue_events ONLY."""

    def get_issue_events(self) -> list[_FakeEvent]:
        return [_FakeEvent(), _FakeEvent(), _FakeEvent()]


class TestIterIssueEvents:
    def test_issue_uses_get_events(self, client: GitHubWorkClient) -> None:
        events = list(client.iter_issue_events(_FakeIssue(), limit=None))  # type: ignore[arg-type]
        assert len(events) == 2

    def test_pull_request_uses_get_issue_events(self, client: GitHubWorkClient) -> None:
        # Must not raise AttributeError: 'PullRequest' object has no
        # attribute 'get_events'.
        events = list(client.iter_issue_events(_FakePullRequest(), limit=None))  # type: ignore[arg-type]
        assert len(events) == 3

    def test_pull_request_respects_limit(self, client: GitHubWorkClient) -> None:
        events = list(client.iter_issue_events(_FakePullRequest(), limit=1))  # type: ignore[arg-type]
        assert len(events) == 1

    def test_pygithub_vocabulary_unchanged(self) -> None:
        """Pin the upstream contract the dispatch in iter_issue_events relies on."""
        from github.Issue import Issue
        from github.PullRequest import PullRequest

        assert hasattr(Issue, "get_events")
        assert not hasattr(PullRequest, "get_events")
        assert hasattr(PullRequest, "get_issue_events")
