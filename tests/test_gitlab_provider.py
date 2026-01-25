"""
Unit tests for GitLab provider and normalize functions.
"""

from unittest.mock import MagicMock

import pytest

from dev_health_ops.providers.gitlab.normalize import (
    _priority_from_labels,
    detect_gitlab_reopen_events,
    enrich_work_item_with_priority,
    extract_gitlab_dependencies,
    gitlab_issue_to_work_item,
    gitlab_milestone_to_sprint,
    gitlab_mr_to_work_item,
    gitlab_note_to_interaction_event,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping


@pytest.fixture
def mock_identity() -> IdentityResolver:
    """Mock identity resolver that returns 'user:' prefixed username."""
    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: (
        f"user:{kwargs.get('username', 'unknown')}"
    )
    return identity


@pytest.fixture
def mock_status_mapping() -> StatusMapping:
    """Mock status mapping with basic label-based status resolution."""
    mapping = MagicMock(spec=StatusMapping)
    mapping.normalize_status.side_effect = lambda **kwargs: (
        "in_progress"
        if "doing" in (kwargs.get("labels") or [])
        else "done"
        if "done" in (kwargs.get("labels") or [])
        else "unknown"
    )
    mapping.normalize_type.return_value = "issue"
    return mapping


class TestPriorityFromLabels:
    """Tests for _priority_from_labels helper."""

    def test_critical_priority(self) -> None:
        priority, service_class = _priority_from_labels(["priority::critical"])
        assert priority == "critical"
        assert service_class == "expedite"

    def test_high_priority(self) -> None:
        priority, service_class = _priority_from_labels(["priority::high"])
        assert priority == "high"
        assert service_class == "fixed_date"

    def test_medium_priority(self) -> None:
        priority, service_class = _priority_from_labels(["priority::medium"])
        assert priority == "medium"
        assert service_class == "standard"

    def test_low_priority(self) -> None:
        priority, service_class = _priority_from_labels(["priority::low"])
        assert priority == "low"
        assert service_class == "intangible"

    def test_p0_label(self) -> None:
        priority, service_class = _priority_from_labels(["p0"])
        assert priority == "critical"
        assert service_class == "expedite"

    def test_blocker_label(self) -> None:
        priority, service_class = _priority_from_labels(["blocker"])
        assert priority == "critical"
        assert service_class == "expedite"

    def test_no_priority_labels(self) -> None:
        priority, service_class = _priority_from_labels(["bug", "enhancement"])
        assert priority is None
        assert service_class is None

    def test_first_priority_wins(self) -> None:
        priority, service_class = _priority_from_labels(["low", "high"])
        assert priority == "low"  # First match wins
        assert service_class == "intangible"  # Corresponding service class


class TestGitLabIssueToWorkItem:
    """Tests for gitlab_issue_to_work_item function."""

    def test_basic_issue(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = {
            "iid": 42,
            "title": "Test Issue",
            "state": "opened",
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-15T12:00:00Z",
            "labels": ["bug"],
            "assignees": [{"username": "dev1", "name": "Developer One"}],
            "author": {"username": "reporter1", "name": "Reporter One"},
            "web_url": "https://gitlab.com/group/project/-/issues/42",
        }

        work_item, transitions = gitlab_issue_to_work_item(
            issue=issue,
            project_full_path="group/project",
            repo_id=None,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.work_item_id == "gitlab:group/project#42"
        assert work_item.title == "Test Issue"
        assert work_item.provider == "gitlab"
        assert work_item.assignees == ["user:dev1"]
        assert work_item.reporter == "user:reporter1"
        assert "bug" in work_item.labels

    def test_closed_issue_with_milestone(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = {
            "iid": 100,
            "title": "Closed Issue",
            "state": "closed",
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-20T14:00:00Z",
            "closed_at": "2025-01-20T14:00:00Z",
            "labels": [],
            "milestone": {"id": 5, "title": "Sprint 1"},
            "assignees": [],
            "author": None,
        }

        work_item, _ = gitlab_issue_to_work_item(
            issue=issue,
            project_full_path="org/repo",
            repo_id=None,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.sprint_id == "5"
        assert work_item.sprint_name == "Sprint 1"
        assert work_item.closed_at is not None


class TestGitLabMRToWorkItem:
    """Tests for gitlab_mr_to_work_item function."""

    def test_open_mr(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        mr = {
            "iid": 99,
            "title": "Add feature X",
            "state": "opened",
            "created_at": "2025-01-10T10:00:00Z",
            "updated_at": "2025-01-15T11:00:00Z",
            "labels": ["priority::high"],
            "assignees": [{"username": "reviewer1"}],
            "author": {"username": "author1"},
            "web_url": "https://gitlab.com/group/project/-/merge_requests/99",
        }

        work_item, transitions = gitlab_mr_to_work_item(
            mr=mr,
            project_full_path="group/project",
            repo_id=None,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.work_item_id == "gitlab:group/project!99"  # ! for MRs
        assert work_item.type == "merge_request"
        assert work_item.status == "in_progress"
        assert work_item.priority_raw == "high"
        assert work_item.service_class == "fixed_date"

    def test_merged_mr(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        mr = {
            "iid": 50,
            "title": "Fix bug",
            "state": "merged",
            "created_at": "2025-01-05T10:00:00Z",
            "updated_at": "2025-01-10T15:00:00Z",
            "merged_at": "2025-01-10T15:00:00Z",
            "labels": [],
            "assignees": [],
            "author": {"username": "dev"},
        }

        work_item, _ = gitlab_mr_to_work_item(
            mr=mr,
            project_full_path="org/repo",
            repo_id=None,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.status == "done"
        assert work_item.completed_at is not None


class TestDetectGitLabReopenEvents:
    """Tests for detect_gitlab_reopen_events function."""

    def test_reopen_detected(self, mock_identity: IdentityResolver) -> None:
        state_events = [
            {
                "state": "closed",
                "created_at": "2025-01-10T10:00:00Z",
                "user": {"username": "closer"},
            },
            {
                "state": "reopened",
                "created_at": "2025-01-11T09:00:00Z",
                "user": {"username": "reopener"},
            },
        ]

        events = detect_gitlab_reopen_events(
            work_item_id="gitlab:test/proj#1",
            state_events=state_events,
            identity=mock_identity,
        )

        assert len(events) == 1
        assert events[0].work_item_id == "gitlab:test/proj#1"
        assert events[0].actor == "user:reopener"
        assert events[0].from_status == "done"
        assert events[0].to_status == "in_progress"

    def test_no_reopen(self, mock_identity: IdentityResolver) -> None:
        state_events = [
            {"state": "opened", "created_at": "2025-01-01T10:00:00Z"},
            {"state": "closed", "created_at": "2025-01-10T10:00:00Z"},
        ]

        events = detect_gitlab_reopen_events(
            work_item_id="gitlab:test/proj#1",
            state_events=state_events,
            identity=mock_identity,
        )

        assert len(events) == 0


class TestGitLabNoteToInteractionEvent:
    """Tests for gitlab_note_to_interaction_event function."""

    def test_user_comment(self, mock_identity: IdentityResolver) -> None:
        note = {
            "id": 123,
            "body": "This is a comment with some text.",
            "created_at": "2025-01-12T14:30:00Z",
            "system": False,
            "author": {"username": "commenter", "name": "Comment Author"},
        }

        event = gitlab_note_to_interaction_event(
            note=note,
            work_item_id="gitlab:org/repo#10",
            identity=mock_identity,
        )

        assert event is not None
        assert event.work_item_id == "gitlab:org/repo#10"
        assert event.interaction_type == "comment"
        assert event.actor == "user:commenter"
        assert event.body_length == len("This is a comment with some text.")

    def test_system_note_skipped(self, mock_identity: IdentityResolver) -> None:
        note = {
            "id": 456,
            "body": "changed the description",
            "created_at": "2025-01-12T14:00:00Z",
            "system": True,
            "author": {"username": "system"},
        }

        event = gitlab_note_to_interaction_event(
            note=note,
            work_item_id="gitlab:org/repo#10",
            identity=mock_identity,
        )

        assert event is None


class TestExtractGitLabDependencies:
    """Tests for extract_gitlab_dependencies function."""

    def test_explicit_block_link(self) -> None:
        issue = {"iid": 1, "description": ""}
        linked_issues = [
            {
                "iid": 2,
                "link_type": "blocks",
                "references": {"full": "group/project#2"},
            }
        ]

        deps = extract_gitlab_dependencies(
            work_item_id="gitlab:group/project#1",
            issue=issue,
            project_full_path="group/project",
            linked_issues=linked_issues,
        )

        assert len(deps) == 1
        assert deps[0].source_work_item_id == "gitlab:group/project#1"
        assert deps[0].target_work_item_id == "gitlab:group/project#2"
        assert deps[0].relationship_type == "blocks"

    def test_relates_to_link(self) -> None:
        issue = {"iid": 5, "description": ""}
        linked_issues = [
            {
                "iid": 10,
                "link_type": "relates_to",
                "references": {"full": "group/project#10"},
            }
        ]

        deps = extract_gitlab_dependencies(
            work_item_id="gitlab:group/project#5",
            issue=issue,
            project_full_path="group/project",
            linked_issues=linked_issues,
        )

        assert len(deps) == 1
        assert deps[0].relationship_type == "relates_to"

    def test_description_reference(self) -> None:
        issue = {
            "iid": 1,
            "description": "This is blocked by #3 which needs to be fixed first.",
        }

        deps = extract_gitlab_dependencies(
            work_item_id="gitlab:group/project#1",
            issue=issue,
            project_full_path="group/project",
            linked_issues=None,
        )

        assert len(deps) == 1
        assert deps[0].target_work_item_id == "gitlab:group/project#3"
        assert deps[0].relationship_type == "blocked_by"


class TestGitLabMilestoneToSprint:
    """Tests for gitlab_milestone_to_sprint function."""

    def test_active_milestone(self) -> None:
        milestone = {
            "id": 100,
            "title": "Q1 Sprint 1",
            "state": "active",
            "start_date": "2025-01-01",
            "due_date": "2025-01-14",
        }

        sprint = gitlab_milestone_to_sprint(
            milestone=milestone,
            project_full_path="org/project",
        )

        assert sprint.sprint_id == "gitlab:org/project:milestone:100"
        assert sprint.name == "Q1 Sprint 1"
        assert sprint.state == "active"
        assert sprint.started_at is not None
        assert sprint.ended_at is not None

    def test_closed_milestone(self) -> None:
        milestone = {
            "id": 50,
            "title": "Past Sprint",
            "state": "closed",
            "start_date": "2024-12-01",
            "due_date": "2024-12-14",
        }

        sprint = gitlab_milestone_to_sprint(
            milestone=milestone,
            project_full_path="group/repo",
        )

        assert sprint.state == "closed"
        assert sprint.completed_at is not None


class TestEnrichWorkItemWithPriority:
    """Tests for enrich_work_item_with_priority function."""

    def test_adds_priority(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        # Create a work item without priority
        issue = {
            "iid": 1,
            "title": "Test",
            "state": "opened",
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T10:00:00Z",
            "labels": ["blocker", "bug"],
            "assignees": [],
            "author": None,
        }

        work_item, _ = gitlab_issue_to_work_item(
            issue=issue,
            project_full_path="test/proj",
            repo_id=None,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        enriched = enrich_work_item_with_priority(work_item, ["blocker", "bug"])

        assert enriched.priority_raw == "critical"
        assert enriched.service_class == "expedite"

    def test_skips_if_already_set(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        mr = {
            "iid": 1,
            "title": "Test MR",
            "state": "opened",
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T10:00:00Z",
            "labels": ["priority::high"],
            "assignees": [],
            "author": None,
        }

        work_item, _ = gitlab_mr_to_work_item(
            mr=mr,
            project_full_path="test/proj",
            repo_id=None,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        # Already has priority::high
        enriched = enrich_work_item_with_priority(work_item, ["low"])

        # Should keep original priority::high
        assert enriched.priority_raw == "high"


class TestGitLabProviderRegistration:
    """Tests for GitLab provider registration."""

    def test_gitlab_registered(self) -> None:
        from dev_health_ops.providers.registry import is_registered, list_providers

        assert is_registered("gitlab")
        assert "gitlab" in list_providers()

    def test_gitlab_provider_instantiation(self) -> None:
        from dev_health_ops.providers.registry import get_provider

        # Should not raise - provider is instantiated lazily without client
        provider = get_provider("gitlab")
        assert provider.name == "gitlab"
        assert provider.capabilities.work_items is True
        assert provider.capabilities.dependencies is True
        assert provider.capabilities.interactions is True
        assert provider.capabilities.sprints is True
        assert provider.capabilities.reopen_events is True
        assert provider.capabilities.priority is True
