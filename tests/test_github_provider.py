"""
Unit tests for the GitHub Provider implementation.

Tests cover:
- Provider instantiation
- Work item normalization (issues and PRs)
- Status transition extraction
- Reopen event detection
- Dependency extraction
- Interaction event creation
- Sprint/milestone normalization
- Priority extraction from labels
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.base import (
    IngestionContext,
    IngestionWindow,
    ProviderBatch,
    ProviderCapabilities,
)
from dev_health_ops.providers.github.normalize import (
    _priority_from_labels,
    detect_github_reopen_events,
    enrich_work_item_with_priority,
    extract_github_dependencies,
    github_comment_to_interaction_event,
    github_issue_to_work_item,
    github_milestone_to_sprint,
    github_pr_to_work_item,
    github_project_v2_item_to_work_item,
)
from dev_health_ops.providers.github.provider import GitHubProvider
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.registry import get_provider, is_registered
from dev_health_ops.providers.status_mapping import StatusMapping


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_status_mapping() -> StatusMapping:
    """Create a mock status mapping."""
    mapping = MagicMock(spec=StatusMapping)
    mapping.normalize_status.return_value = "todo"
    mapping.normalize_type.return_value = "issue"
    return mapping


@pytest.fixture
def mock_identity() -> IdentityResolver:
    """Create a mock identity resolver."""
    resolver = MagicMock(spec=IdentityResolver)
    resolver.resolve.return_value = "test-user"
    return resolver


def _mock_issue(
    *,
    number: int = 1,
    title: str = "Test Issue",
    state: str = "open",
    labels: Optional[List[str]] = None,
    body: str = "",
    created_at: Optional[datetime] = None,
    updated_at: Optional[datetime] = None,
    closed_at: Optional[datetime] = None,
) -> MagicMock:
    """Create a mock GitHub issue."""
    issue = MagicMock()
    issue.number = number
    issue.title = title
    issue.state = state
    issue.body = body
    issue.created_at = created_at or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    issue.updated_at = updated_at or issue.created_at
    issue.closed_at = closed_at
    issue.html_url = f"https://github.com/test/repo/issues/{number}"
    issue.pull_request = None  # Not a PR

    # Mock labels
    mock_labels = []
    for label_name in labels or []:
        label = MagicMock()
        label.name = label_name
        mock_labels.append(label)
    issue.labels = mock_labels

    # Mock assignees
    assignee = MagicMock()
    assignee.login = "assignee1"
    assignee.email = "assignee1@example.com"
    assignee.name = "Assignee One"
    issue.assignees = [assignee]

    # Mock user (reporter)
    user = MagicMock()
    user.login = "reporter1"
    user.email = "reporter1@example.com"
    user.name = "Reporter One"
    issue.user = user

    return issue


def _mock_pr(
    *,
    number: int = 1,
    title: str = "Test PR",
    state: str = "open",
    merged: bool = False,
    draft: bool = False,
    labels: Optional[List[str]] = None,
    body: str = "",
    created_at: Optional[datetime] = None,
    updated_at: Optional[datetime] = None,
    closed_at: Optional[datetime] = None,
    merged_at: Optional[datetime] = None,
) -> MagicMock:
    """Create a mock GitHub pull request."""
    pr = MagicMock()
    pr.number = number
    pr.title = title
    pr.state = state
    pr.merged = merged
    pr.draft = draft
    pr.body = body
    pr.created_at = created_at or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    pr.updated_at = updated_at or pr.created_at
    pr.closed_at = closed_at
    pr.merged_at = merged_at
    pr.html_url = f"https://github.com/test/repo/pull/{number}"

    # Mock labels
    mock_labels = []
    for label_name in labels or []:
        label = MagicMock()
        label.name = label_name
        mock_labels.append(label)
    pr.labels = mock_labels

    # Mock assignees
    assignee = MagicMock()
    assignee.login = "assignee1"
    assignee.email = "assignee1@example.com"
    assignee.name = "Assignee One"
    pr.assignees = [assignee]

    # Mock user (author)
    user = MagicMock()
    user.login = "author1"
    user.email = "author1@example.com"
    user.name = "Author One"
    pr.user = user

    return pr


def _mock_event(
    event_type: str,
    created_at: Optional[datetime] = None,
    label_name: Optional[str] = None,
) -> MagicMock:
    """Create a mock GitHub event."""
    ev = MagicMock()
    ev.event = event_type
    ev.created_at = created_at or datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    if label_name:
        label = MagicMock()
        label.name = label_name
        ev.label = label
    else:
        ev.label = None

    # Mock actor
    actor = MagicMock()
    actor.login = "actor1"
    actor.email = "actor1@example.com"
    actor.name = "Actor One"
    ev.actor = actor

    return ev


def _mock_comment(
    *,
    comment_id: int = 1,
    body: str = "Test comment",
    created_at: Optional[datetime] = None,
) -> MagicMock:
    """Create a mock GitHub comment."""
    comment = MagicMock()
    comment.id = comment_id
    comment.body = body
    comment.created_at = created_at or datetime(
        2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc
    )

    user = MagicMock()
    user.login = "commenter1"
    user.email = "commenter1@example.com"
    user.name = "Commenter One"
    comment.user = user

    return comment


def _mock_milestone(
    *,
    milestone_id: int = 1,
    number: int = 1,
    title: str = "Sprint 1",
    state: str = "open",
    created_at: Optional[datetime] = None,
    due_on: Optional[datetime] = None,
) -> MagicMock:
    """Create a mock GitHub milestone."""
    ms = MagicMock()
    ms.id = milestone_id
    ms.number = number
    ms.title = title
    ms.state = state
    ms.created_at = created_at or datetime(2024, 1, 1, tzinfo=timezone.utc)
    ms.due_on = due_on

    return ms


# ============================================================================
# Provider Tests
# ============================================================================


def test_github_provider_instantiation():
    """Test that GitHubProvider can be instantiated."""
    provider = GitHubProvider()
    assert provider.name == "github"
    assert isinstance(provider.capabilities, ProviderCapabilities)


def test_github_provider_capabilities():
    """Test that GitHubProvider declares expected capabilities."""
    provider = GitHubProvider()
    assert provider.capabilities.work_items is True
    assert provider.capabilities.status_transitions is True
    assert provider.capabilities.dependencies is True
    assert provider.capabilities.interactions is True
    assert provider.capabilities.sprints is True
    assert provider.capabilities.reopen_events is True
    assert provider.capabilities.priority is True


def test_github_provider_registered():
    """Test that GitHubProvider is registered in the provider registry."""
    assert is_registered("github")
    provider = get_provider("github")
    assert provider.name == "github"


def test_github_provider_status_mapping_lazy_load(mock_status_mapping):
    """Test that status mapping is lazy loaded."""
    provider = GitHubProvider(status_mapping=mock_status_mapping)
    assert provider.status_mapping is mock_status_mapping


def test_github_provider_identity_lazy_load(mock_identity):
    """Test that identity resolver is lazy loaded."""
    provider = GitHubProvider(identity=mock_identity)
    assert provider.identity is mock_identity


# ============================================================================
# Issue Normalization Tests
# ============================================================================


def test_github_issue_to_work_item_basic(mock_status_mapping, mock_identity):
    """Test basic issue normalization."""
    issue = _mock_issue(number=123, title="Test Issue", state="open")

    wi, transitions = github_issue_to_work_item(
        issue=issue,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.work_item_id == "gh:owner/repo#123"
    assert wi.provider == "github"
    assert wi.title == "Test Issue"
    assert wi.project_id == "owner/repo"


def test_github_issue_to_work_item_with_labels(mock_status_mapping, mock_identity):
    """Test issue normalization with labels."""
    issue = _mock_issue(labels=["bug", "priority::high"])

    wi, transitions = github_issue_to_work_item(
        issue=issue,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert "bug" in wi.labels
    assert "priority::high" in wi.labels


def test_github_issue_to_work_item_with_events(mock_status_mapping, mock_identity):
    """Test issue normalization with events produces transitions."""
    issue = _mock_issue()
    events = [
        _mock_event("closed", datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)),
        _mock_event("reopened", datetime(2024, 1, 4, 12, 0, 0, tzinfo=timezone.utc)),
    ]

    wi, transitions = github_issue_to_work_item(
        issue=issue,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
        events=events,
    )

    assert len(transitions) == 2
    assert transitions[0].to_status == "done"
    assert transitions[1].to_status == "todo"


def test_github_issue_to_work_item_closed(mock_status_mapping, mock_identity):
    """Test closed issue normalization."""
    issue = _mock_issue(
        state="closed",
        closed_at=datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc),
    )

    wi, transitions = github_issue_to_work_item(
        issue=issue,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.closed_at is not None
    assert wi.completed_at == wi.closed_at


# ============================================================================
# PR Normalization Tests
# ============================================================================


def test_github_pr_to_work_item_basic(mock_status_mapping, mock_identity):
    """Test basic PR normalization."""
    pr = _mock_pr(number=456, title="Test PR", state="open")

    wi, transitions = github_pr_to_work_item(
        pr=pr,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.work_item_id == "ghpr:owner/repo#456"
    assert wi.provider == "github"
    assert wi.title == "Test PR"
    assert wi.type == "pr"


def test_github_pr_to_work_item_merged(mock_status_mapping, mock_identity):
    """Test merged PR normalization."""
    pr = _mock_pr(
        number=456,
        state="closed",
        merged=True,
        merged_at=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
    )

    wi, transitions = github_pr_to_work_item(
        pr=pr,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.status == "done"
    assert wi.status_raw == "merged"
    assert wi.completed_at == datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)


def test_github_pr_to_work_item_closed_not_merged(mock_status_mapping, mock_identity):
    """Test closed but not merged PR normalization."""
    pr = _mock_pr(
        number=456,
        state="closed",
        merged=False,
        closed_at=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
    )

    wi, transitions = github_pr_to_work_item(
        pr=pr,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.status == "canceled"
    assert wi.status_raw == "closed"


def test_github_pr_to_work_item_draft(mock_status_mapping, mock_identity):
    """Test draft PR normalization."""
    pr = _mock_pr(state="open", draft=True)

    wi, transitions = github_pr_to_work_item(
        pr=pr,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.status == "todo"


def test_github_pr_to_work_item_started_at(mock_status_mapping, mock_identity):
    """Test PR started_at is set to created_at."""
    created = datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc)
    pr = _mock_pr(state="open", created_at=created)

    wi, transitions = github_pr_to_work_item(
        pr=pr,
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    assert wi.started_at == created


# ============================================================================
# Comment/Interaction Tests
# ============================================================================


def test_github_comment_to_interaction_event(mock_identity):
    """Test comment normalization to interaction event."""
    comment = _mock_comment(body="This is a test comment")

    event = github_comment_to_interaction_event(
        comment=comment,
        work_item_id="gh:owner/repo#123",
        identity=mock_identity,
    )

    assert event is not None
    assert event.work_item_id == "gh:owner/repo#123"
    assert event.provider == "github"
    assert event.interaction_type == "comment"
    assert event.body_length == len("This is a test comment")


def test_github_comment_to_interaction_event_no_id(mock_identity):
    """Test that comment without ID returns None."""
    comment = MagicMock()
    comment.id = None

    event = github_comment_to_interaction_event(
        comment=comment,
        work_item_id="gh:owner/repo#123",
        identity=mock_identity,
    )

    assert event is None


def test_github_comment_to_interaction_event_no_created_at(mock_identity):
    """Test that comment without created_at returns None."""
    comment = MagicMock()
    comment.id = 1
    comment.created_at = None

    event = github_comment_to_interaction_event(
        comment=comment,
        work_item_id="gh:owner/repo#123",
        identity=mock_identity,
    )

    assert event is None


# ============================================================================
# Milestone/Sprint Tests
# ============================================================================


def test_github_milestone_to_sprint():
    """Test milestone normalization to sprint."""
    ms = _mock_milestone(
        milestone_id=10,
        title="Sprint 1",
        state="open",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        due_on=datetime(2024, 1, 15, tzinfo=timezone.utc),
    )

    sprint = github_milestone_to_sprint(
        milestone=ms,
        repo_full_name="owner/repo",
    )

    assert sprint.sprint_id == "ghms:owner/repo:10"
    assert sprint.provider == "github"
    assert sprint.name == "Sprint 1"
    assert sprint.state == "active"
    assert sprint.started_at == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert sprint.ended_at == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_github_milestone_to_sprint_closed():
    """Test closed milestone normalization."""
    ms = _mock_milestone(state="closed")

    sprint = github_milestone_to_sprint(
        milestone=ms,
        repo_full_name="owner/repo",
    )

    assert sprint.state == "closed"


# ============================================================================
# Reopen Event Tests
# ============================================================================


def test_detect_github_reopen_events(mock_identity):
    """Test reopen event detection."""
    events = [
        _mock_event("closed", datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)),
        _mock_event("reopened", datetime(2024, 1, 4, 12, 0, 0, tzinfo=timezone.utc)),
        _mock_event("closed", datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc)),
    ]

    reopen_events = detect_github_reopen_events(
        work_item_id="gh:owner/repo#123",
        events=events,
        identity=mock_identity,
    )

    assert len(reopen_events) == 1
    assert reopen_events[0].work_item_id == "gh:owner/repo#123"
    assert reopen_events[0].occurred_at == datetime(
        2024, 1, 4, 12, 0, 0, tzinfo=timezone.utc
    )


def test_detect_github_reopen_events_multiple(mock_identity):
    """Test detection of multiple reopen events."""
    events = [
        _mock_event("closed", datetime(2024, 1, 1, tzinfo=timezone.utc)),
        _mock_event("reopened", datetime(2024, 1, 2, tzinfo=timezone.utc)),
        _mock_event("closed", datetime(2024, 1, 3, tzinfo=timezone.utc)),
        _mock_event("reopened", datetime(2024, 1, 4, tzinfo=timezone.utc)),
    ]

    reopen_events = detect_github_reopen_events(
        work_item_id="gh:owner/repo#123",
        events=events,
        identity=mock_identity,
    )

    assert len(reopen_events) == 2


def test_detect_github_reopen_events_empty(mock_identity):
    """Test no reopen events when there are none."""
    events = [
        _mock_event("closed", datetime(2024, 1, 1, tzinfo=timezone.utc)),
        _mock_event(
            "labeled", datetime(2024, 1, 2, tzinfo=timezone.utc), label_name="bug"
        ),
    ]

    reopen_events = detect_github_reopen_events(
        work_item_id="gh:owner/repo#123",
        events=events,
        identity=mock_identity,
    )

    assert len(reopen_events) == 0


# ============================================================================
# Dependency Tests
# ============================================================================


def test_extract_github_dependencies_depends_on():
    """Test dependency extraction from 'depends on' pattern."""
    issue = _mock_issue(body="This depends on #123")

    deps = extract_github_dependencies(
        work_item_id="gh:owner/repo#456",
        issue_or_pr=issue,
        repo_full_name="owner/repo",
    )

    assert len(deps) == 1
    assert deps[0].source_work_item_id == "gh:owner/repo#456"
    assert deps[0].target_work_item_id == "gh:owner/repo#123"
    assert deps[0].relationship_type == "blocks"


def test_extract_github_dependencies_blocked_by():
    """Test dependency extraction from 'blocked by' pattern."""
    issue = _mock_issue(body="Blocked by #99")

    deps = extract_github_dependencies(
        work_item_id="gh:owner/repo#456",
        issue_or_pr=issue,
        repo_full_name="owner/repo",
    )

    assert len(deps) == 1
    assert deps[0].relationship_type == "blocks"


def test_extract_github_dependencies_blocks():
    """Test dependency extraction from 'blocks' pattern."""
    issue = _mock_issue(body="This blocks #789")

    deps = extract_github_dependencies(
        work_item_id="gh:owner/repo#456",
        issue_or_pr=issue,
        repo_full_name="owner/repo",
    )

    assert len(deps) == 1
    assert deps[0].relationship_type == "is_blocked_by"


def test_extract_github_dependencies_fixes():
    """Test dependency extraction from 'fixes' pattern."""
    issue = _mock_issue(body="Fixes #100")

    deps = extract_github_dependencies(
        work_item_id="gh:owner/repo#456",
        issue_or_pr=issue,
        repo_full_name="owner/repo",
    )

    assert len(deps) == 1
    assert deps[0].relationship_type == "relates_to"


def test_extract_github_dependencies_cross_repo():
    """Test cross-repository dependency extraction."""
    issue = _mock_issue(body="Depends on other/repo#42")

    deps = extract_github_dependencies(
        work_item_id="gh:owner/repo#456",
        issue_or_pr=issue,
        repo_full_name="owner/repo",
    )

    assert len(deps) == 1
    assert deps[0].target_work_item_id == "gh:other/repo#42"


def test_extract_github_dependencies_no_body():
    """Test no dependencies when body is empty."""
    issue = _mock_issue(body="")

    deps = extract_github_dependencies(
        work_item_id="gh:owner/repo#456",
        issue_or_pr=issue,
        repo_full_name="owner/repo",
    )

    assert len(deps) == 0


# ============================================================================
# Priority Tests
# ============================================================================


def test_priority_from_labels():
    """Test priority extraction from labels."""
    assert _priority_from_labels(["priority::critical"]) == ("critical", "expedite")
    assert _priority_from_labels(["priority::high"]) == ("high", "fixed_date")
    assert _priority_from_labels(["priority::medium"]) == ("medium", "standard")
    assert _priority_from_labels(["priority::low"]) == ("low", "intangible")
    assert _priority_from_labels(["p0"]) == ("critical", "expedite")
    assert _priority_from_labels(["p1"]) == ("high", "fixed_date")
    assert _priority_from_labels(["p2"]) == ("medium", "standard")
    assert _priority_from_labels(["p3"]) == ("low", "intangible")
    assert _priority_from_labels(["urgent"]) == ("critical", "expedite")
    assert _priority_from_labels(["bug"]) == (None, None)


def test_enrich_work_item_with_priority():
    """Test work item enrichment with priority."""
    wi = WorkItem(
        work_item_id="gh:owner/repo#123",
        provider="github",
        repo_id=None,
        project_key=None,
        project_id="owner/repo",
        title="Test",
        type="issue",
        status="todo",
        status_raw="open",
        assignees=[],
        reporter=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        labels=["priority::high"],
    )

    enriched = enrich_work_item_with_priority(wi, wi.labels)

    assert enriched.priority_raw == "high"
    assert enriched.service_class == "fixed_date"


def test_enrich_work_item_no_priority():
    """Test work item enrichment without matching priority labels."""
    wi = WorkItem(
        work_item_id="gh:owner/repo#123",
        provider="github",
        repo_id=None,
        project_key=None,
        project_id="owner/repo",
        title="Test",
        type="issue",
        status="todo",
        status_raw="open",
        assignees=[],
        reporter=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        labels=["bug", "enhancement"],
    )

    enriched = enrich_work_item_with_priority(wi, wi.labels)

    # Should return unchanged work item
    assert enriched.priority_raw is None
    assert enriched.service_class is None


# ============================================================================
# GitHub Project v2 Tests
# ============================================================================


def test_github_project_v2_item_with_transitions(mock_status_mapping, mock_identity):
    """Test project v2 item normalization with status transitions."""
    # Create a mock project v2 item with status changes
    item_node = {
        "id": "PVTI_test123",
        "createdAt": "2024-01-01T10:00:00Z",
        "updatedAt": "2024-01-05T15:00:00Z",
        "content": {
            "__typename": "Issue",
            "id": "I_test456",
            "number": 42,
            "title": "Test Issue in Project",
            "url": "https://github.com/owner/repo/issues/42",
            "state": "OPEN",
            "createdAt": "2024-01-01T10:00:00Z",
            "updatedAt": "2024-01-05T15:00:00Z",
            "closedAt": None,
            "repository": {"nameWithOwner": "owner/repo"},
            "labels": {"nodes": [{"name": "bug"}]},
            "assignees": {"nodes": []},
            "author": {"login": "testuser", "email": None, "name": "Test User"},
        },
        "fieldValues": {
            "nodes": [
                {
                    "__typename": "ProjectV2ItemFieldSingleSelectValue",
                    "name": "In Progress",
                    "field": {"name": "Status"},
                }
            ]
        },
        "changes": {
            "nodes": [
                {
                    "field": {"name": "Status"},
                    "previousValue": {"name": "Todo"},
                    "newValue": {"name": "In Progress"},
                    "createdAt": "2024-01-02T12:00:00Z",
                    "actor": {"login": "developer1"},
                },
                {
                    "field": {"name": "Status"},
                    "previousValue": {"name": "In Progress"},
                    "newValue": {"name": "In Review"},
                    "createdAt": "2024-01-03T14:00:00Z",
                    "actor": {"login": "developer2"},
                },
            ]
        },
    }

    wi, transitions = github_project_v2_item_to_work_item(
        item_node=item_node,
        project_scope_id="test-project",
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    # Verify work item was created
    assert wi is not None
    assert wi.work_item_id == "gh:owner/repo#42"
    assert wi.provider == "github"
    assert wi.title == "Test Issue in Project"

    # Verify transitions were extracted
    assert len(transitions) == 2

    # First transition: Todo -> In Progress
    assert transitions[0].work_item_id == "gh:owner/repo#42"
    assert transitions[0].from_status_raw == "Todo"
    assert transitions[0].to_status_raw == "In Progress"
    assert transitions[0].provider == "github"

    # Second transition: In Progress -> In Review
    assert transitions[1].work_item_id == "gh:owner/repo#42"
    assert transitions[1].from_status_raw == "In Progress"
    assert transitions[1].to_status_raw == "In Review"
    assert transitions[1].provider == "github"


def test_github_project_v2_item_no_transitions(mock_status_mapping, mock_identity):
    """Test project v2 item normalization without status changes."""
    item_node = {
        "id": "PVTI_test789",
        "createdAt": "2024-01-01T10:00:00Z",
        "updatedAt": "2024-01-01T10:00:00Z",
        "content": {
            "__typename": "Issue",
            "id": "I_test999",
            "number": 99,
            "title": "New Issue",
            "url": "https://github.com/owner/repo/issues/99",
            "state": "OPEN",
            "createdAt": "2024-01-01T10:00:00Z",
            "updatedAt": "2024-01-01T10:00:00Z",
            "closedAt": None,
            "repository": {"nameWithOwner": "owner/repo"},
            "labels": {"nodes": []},
            "assignees": {"nodes": []},
            "author": {"login": "testuser", "email": None, "name": "Test User"},
        },
        "fieldValues": {"nodes": []},
        "changes": {"nodes": []},  # No changes
    }

    wi, transitions = github_project_v2_item_to_work_item(
        item_node=item_node,
        project_scope_id="test-project",
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    # Verify work item was created
    assert wi is not None
    assert wi.work_item_id == "gh:owner/repo#99"

    # Verify no transitions
    assert len(transitions) == 0


# ============================================================================
# Integration Tests
# ============================================================================


@patch.dict(os.environ, {"GITHUB_TOKEN": "test-token"}, clear=False)
@patch("dev_health_ops.providers.github.client.GitHubWorkClient")
def test_github_provider_ingest(mock_client_class, mock_status_mapping, mock_identity):
    """Test full provider ingest flow."""
    # Setup mock client
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Mock milestones
    mock_client.iter_repo_milestones.return_value = [
        _mock_milestone(milestone_id=1, title="Sprint 1")
    ]

    # Mock issues
    mock_issue = _mock_issue(number=1, title="Test Issue")
    mock_client.iter_issues.return_value = [mock_issue]
    mock_client.iter_issue_events.return_value = []
    mock_client.iter_issue_comments.return_value = []

    # Mock PRs
    mock_pr = _mock_pr(number=100, title="Test PR")
    mock_client.iter_pull_requests.return_value = [mock_pr]
    mock_client.iter_pr_comments.return_value = []

    provider = GitHubProvider(
        status_mapping=mock_status_mapping,
        identity=mock_identity,
    )

    ctx = IngestionContext(
        repo="owner/repo",
        window=IngestionWindow(),
    )

    batch = provider.ingest(ctx)

    assert isinstance(batch, ProviderBatch)
    assert len(batch.work_items) == 2  # 1 issue + 1 PR
    assert len(batch.sprints) == 1


@patch.dict(os.environ, {}, clear=True)
def test_github_provider_requires_token():
    """Test that provider raises error without token."""
    provider = GitHubProvider()
    ctx = IngestionContext(
        repo="owner/repo",
        window=IngestionWindow(),
    )

    with pytest.raises(ValueError, match="GITHUB_TOKEN"):
        provider.ingest(ctx)


def test_github_provider_requires_repo():
    """Test that provider raises error without repo."""
    provider = GitHubProvider()
    ctx = IngestionContext(
        repo=None,
        window=IngestionWindow(),
    )

    with pytest.raises(ValueError, match="ctx.repo"):
        provider.ingest(ctx)


def test_github_provider_invalid_repo_format():
    """Test that provider raises error with invalid repo format."""
    provider = GitHubProvider()
    ctx = IngestionContext(
        repo="invalid-format",
        window=IngestionWindow(),
    )

    # Will fail before token check due to format validation
    with pytest.raises(ValueError, match="expected owner/repo"):
        provider.ingest(ctx)
