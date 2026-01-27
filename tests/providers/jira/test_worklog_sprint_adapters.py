from __future__ import annotations

from datetime import datetime, timezone

from atlassian import JiraSprint, JiraUser, JiraWorklog
from dev_health_ops.providers.jira.normalize import (
    canonical_sprint_to_model,
    canonical_worklog_to_model,
)


class MockIdentityResolver:
    def resolve(self, *, provider: str, email=None, account_id=None, display_name=None):
        if account_id:
            return f"user:{account_id}"
        if email:
            return f"user:{email}"
        return "unknown"


class TestCanonicalWorklogToModel:
    def test_basic_worklog_conversion(self) -> None:
        worklog = JiraWorklog(
            issue_key="TEST-123",
            worklog_id="wl-001",
            started_at="2025-01-15T09:00:00+00:00",
            time_spent_seconds=3600,
            created_at="2025-01-15T09:30:00+00:00",
            updated_at="2025-01-15T09:30:00+00:00",
            author=JiraUser(
                account_id="user-123",
                display_name="Test User",
                email="test@example.com",
            ),
        )
        identity = MockIdentityResolver()

        wl = canonical_worklog_to_model(
            issue_key="TEST-123",
            worklog=worklog,
            identity=identity,  # type: ignore[arg-type]
        )

        assert wl.work_item_id == "jira:TEST-123"
        assert wl.provider == "jira"
        assert wl.worklog_id == "wl-001"
        assert wl.author == "user:user-123"
        assert wl.time_spent_seconds == 3600
        assert wl.started_at == datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)

    def test_worklog_without_author(self) -> None:
        worklog = JiraWorklog(
            issue_key="TEST-456",
            worklog_id="wl-002",
            started_at="2025-01-16T10:00:00+00:00",
            time_spent_seconds=1800,
            created_at="2025-01-16T10:00:00+00:00",
            updated_at="2025-01-16T10:00:00+00:00",
            author=None,
        )
        identity = MockIdentityResolver()

        wl = canonical_worklog_to_model(
            issue_key="TEST-456",
            worklog=worklog,
            identity=identity,  # type: ignore[arg-type]
        )

        assert wl.author is None
        assert wl.time_spent_seconds == 1800

    def test_worklog_various_durations(self) -> None:
        identity = MockIdentityResolver()
        test_cases = [
            (60, "1 minute"),
            (3600, "1 hour"),
            (28800, "8 hours"),
            (604800, "1 week"),
        ]

        for seconds, _description in test_cases:
            worklog = JiraWorklog(
                issue_key="TEST-789",
                worklog_id=f"wl-{seconds}",
                started_at="2025-01-17T08:00:00+00:00",
                time_spent_seconds=seconds,
                created_at="2025-01-17T08:00:00+00:00",
                updated_at="2025-01-17T08:00:00+00:00",
                author=None,
            )

            wl = canonical_worklog_to_model(
                issue_key="TEST-789",
                worklog=worklog,
                identity=identity,  # type: ignore[arg-type]
            )

            assert wl.time_spent_seconds == seconds


class TestCanonicalSprintToModel:
    def test_active_sprint(self) -> None:
        sprint = JiraSprint(
            id="sprint-42",
            name="Sprint 42",
            state="active",
            start_at="2025-01-13T00:00:00+00:00",
            end_at="2025-01-27T00:00:00+00:00",
            complete_at=None,
        )

        s = canonical_sprint_to_model(sprint=sprint)

        assert s.sprint_id == "sprint-42"
        assert s.name == "Sprint 42"
        assert s.state == "active"
        assert s.provider == "jira"
        assert s.started_at == datetime(2025, 1, 13, 0, 0, tzinfo=timezone.utc)
        assert s.ended_at == datetime(2025, 1, 27, 0, 0, tzinfo=timezone.utc)
        assert s.completed_at is None

    def test_closed_sprint(self) -> None:
        sprint = JiraSprint(
            id="sprint-41",
            name="Sprint 41",
            state="closed",
            start_at="2024-12-30T00:00:00+00:00",
            end_at="2025-01-13T00:00:00+00:00",
            complete_at="2025-01-13T16:00:00+00:00",
        )

        s = canonical_sprint_to_model(sprint=sprint)

        assert s.state == "closed"
        assert s.completed_at == datetime(2025, 1, 13, 16, 0, tzinfo=timezone.utc)

    def test_future_sprint(self) -> None:
        sprint = JiraSprint(
            id="sprint-43",
            name="Sprint 43",
            state="future",
            start_at=None,
            end_at=None,
            complete_at=None,
        )

        s = canonical_sprint_to_model(sprint=sprint)

        assert s.state == "future"
        assert s.started_at is None
        assert s.ended_at is None
        assert s.completed_at is None

    def test_sprint_state_values(self) -> None:
        for state in ["future", "active", "closed"]:
            sprint = JiraSprint(
                id=f"sprint-{state}",
                name=f"Sprint {state}",
                state=state,
                start_at=None,
                end_at=None,
                complete_at=None,
            )

            s = canonical_sprint_to_model(sprint=sprint)
            assert s.state == state
