from __future__ import annotations

from datetime import datetime, timezone

from atlassian import JiraChangelogEvent, JiraChangelogItem, JiraIssue, JiraUser
from dev_health_ops.providers.jira.normalize import (
    canonical_changelog_to_transitions,
    canonical_jira_issue_to_work_item,
    derive_started_completed_from_transitions,
)
from dev_health_ops.providers.status_mapping import load_status_mapping


class MockIdentityResolver:
    def resolve(self, *, provider: str, email=None, account_id=None, display_name=None):
        if account_id:
            return f"user:{account_id}"
        if email:
            return f"user:{email}"
        return "unknown"


class TestCanonicalJiraIssueToWorkItem:
    def test_basic_conversion(self) -> None:
        issue = JiraIssue(
            cloud_id="test-cloud",
            key="TEST-123",
            project_key="TEST",
            issue_type="Story",
            status="In Progress",
            created_at="2025-01-15T10:00:00+00:00",
            updated_at="2025-01-20T15:30:00+00:00",
            resolved_at=None,
            assignee=JiraUser(
                account_id="user-123",
                display_name="Test User",
                email="test@example.com",
            ),
            reporter=JiraUser(
                account_id="user-456",
                display_name="Reporter User",
                email="reporter@example.com",
            ),
            labels=["backend", "api"],
            components=["core"],
            story_points=5.0,
            sprint_ids=["sprint-42"],
        )
        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        wi = canonical_jira_issue_to_work_item(
            issue=issue,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
        )

        assert wi.work_item_id == "jira:TEST-123"
        assert wi.provider == "jira"
        assert wi.project_key == "TEST"
        assert wi.status == "in_progress"
        assert wi.type == "story"
        assert wi.assignees == ["user:user-123"]
        assert wi.reporter == "user:user-456"
        assert wi.labels == ["backend", "api"]
        assert wi.story_points == 5.0
        assert wi.sprint_id == "sprint-42"

    def test_no_assignee(self) -> None:
        issue = JiraIssue(
            cloud_id="test-cloud",
            key="TEST-456",
            project_key="TEST",
            issue_type="Bug",
            status="Open",
            created_at="2025-01-15T10:00:00+00:00",
            updated_at="2025-01-15T10:00:00+00:00",
            assignee=None,
            reporter=None,
        )
        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        wi = canonical_jira_issue_to_work_item(
            issue=issue,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
        )

        assert wi.assignees == []
        assert wi.reporter is None

    def test_resolved_issue(self) -> None:
        issue = JiraIssue(
            cloud_id="test-cloud",
            key="TEST-789",
            project_key="TEST",
            issue_type="Task",
            status="Done",
            created_at="2025-01-01T10:00:00+00:00",
            updated_at="2025-01-10T16:00:00+00:00",
            resolved_at="2025-01-10T15:00:00+00:00",
        )
        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        wi = canonical_jira_issue_to_work_item(
            issue=issue,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
        )

        assert wi.status == "done"
        assert wi.completed_at is not None


class TestCanonicalChangelogToTransitions:
    def test_status_transitions(self) -> None:
        events = [
            JiraChangelogEvent(
                issue_key="TEST-123",
                event_id="evt-1",
                created_at="2025-01-16T09:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="status",
                        from_value="1",
                        to_value="2",
                        from_string="Open",
                        to_string="In Progress",
                    )
                ],
                author=JiraUser(
                    account_id="user-123",
                    display_name="Dev",
                    email=None,
                ),
            ),
            JiraChangelogEvent(
                issue_key="TEST-123",
                event_id="evt-2",
                created_at="2025-01-18T14:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="status",
                        from_value="2",
                        to_value="3",
                        from_string="In Progress",
                        to_string="Done",
                    )
                ],
                author=JiraUser(
                    account_id="user-123",
                    display_name="Dev",
                    email=None,
                ),
            ),
        ]
        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        transitions = canonical_changelog_to_transitions(
            issue_key="TEST-123",
            changelog_events=events,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
            labels=[],
        )

        assert len(transitions) == 2
        assert transitions[0].from_status_raw == "Open"
        assert transitions[0].to_status_raw == "In Progress"
        assert transitions[0].to_status == "in_progress"
        assert transitions[1].to_status == "done"
        assert transitions[0].actor == "user:user-123"

    def test_filters_non_status_changes(self) -> None:
        events = [
            JiraChangelogEvent(
                issue_key="TEST-123",
                event_id="evt-1",
                created_at="2025-01-16T09:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="assignee",
                        from_value=None,
                        to_value="user-123",
                        from_string=None,
                        to_string="Test User",
                    ),
                    JiraChangelogItem(
                        field="status",
                        from_value="1",
                        to_value="2",
                        from_string="Open",
                        to_string="In Progress",
                    ),
                ],
                author=None,
            ),
        ]
        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        transitions = canonical_changelog_to_transitions(
            issue_key="TEST-123",
            changelog_events=events,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
            labels=[],
        )

        assert len(transitions) == 1
        assert transitions[0].to_status == "in_progress"


class TestDeriveStartedCompleted:
    def test_derives_from_transitions(self) -> None:
        from dev_health_ops.models.work_items import WorkItemStatusTransition

        transitions = [
            WorkItemStatusTransition(
                work_item_id="jira:TEST-1",
                provider="jira",
                occurred_at=datetime(2025, 1, 16, 9, 0, tzinfo=timezone.utc),
                from_status_raw="Open",
                to_status_raw="In Progress",
                from_status="todo",
                to_status="in_progress",
            ),
            WorkItemStatusTransition(
                work_item_id="jira:TEST-1",
                provider="jira",
                occurred_at=datetime(2025, 1, 18, 14, 0, tzinfo=timezone.utc),
                from_status_raw="In Progress",
                to_status_raw="Done",
                from_status="in_progress",
                to_status="done",
            ),
        ]

        started, completed = derive_started_completed_from_transitions(
            transitions=transitions,
            normalized_status="done",
            resolved_at=datetime(2025, 1, 18, 14, 0, tzinfo=timezone.utc),
            updated_at=datetime(2025, 1, 18, 14, 0, tzinfo=timezone.utc),
        )

        assert started == datetime(2025, 1, 16, 9, 0, tzinfo=timezone.utc)
        assert completed == datetime(2025, 1, 18, 14, 0, tzinfo=timezone.utc)

    def test_fallback_for_no_transitions(self) -> None:
        started, completed = derive_started_completed_from_transitions(
            transitions=[],
            normalized_status="done",
            resolved_at=datetime(2025, 1, 18, 14, 0, tzinfo=timezone.utc),
            updated_at=datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc),
        )

        assert started is None
        assert completed == datetime(2025, 1, 18, 14, 0, tzinfo=timezone.utc)

    def test_fallback_to_updated_at(self) -> None:
        started, completed = derive_started_completed_from_transitions(
            transitions=[],
            normalized_status="done",
            resolved_at=None,
            updated_at=datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc),
        )

        assert completed == datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc)
