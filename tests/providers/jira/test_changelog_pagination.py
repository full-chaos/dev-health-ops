from __future__ import annotations

from atlassian import JiraChangelogEvent, JiraChangelogItem, JiraUser
from dev_health_ops.providers.jira.normalize import (
    canonical_changelog_to_transitions,
)
from dev_health_ops.providers.status_mapping import load_status_mapping


class MockIdentityResolver:
    def resolve(self, *, provider: str, email=None, account_id=None, display_name=None):
        if account_id:
            return f"user:{account_id}"
        return "unknown"


class TestChangelogPagination:
    def test_handles_large_changelog_over_100_entries(self) -> None:
        events = []
        for i in range(150):
            day = (i // 24) + 1
            hour = i % 24
            events.append(
                JiraChangelogEvent(
                    issue_key="TEST-1",
                    event_id=f"evt-{i}",
                    created_at=f"2025-01-{day:02d}T{hour:02d}:00:00+00:00",
                    items=[
                        JiraChangelogItem(
                            field="status",
                            from_value=str(i),
                            to_value=str(i + 1),
                            from_string=f"Status {i}",
                            to_string=f"Status {i + 1}",
                        )
                    ],
                    author=JiraUser(
                        account_id=f"user-{i % 5}",
                        display_name=f"User {i % 5}",
                        email=None,
                    ),
                )
            )

        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        transitions = canonical_changelog_to_transitions(
            issue_key="TEST-1",
            changelog_events=events,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
            labels=[],
        )

        assert len(transitions) == 150
        assert transitions[0].from_status_raw == "Status 0"
        assert transitions[149].to_status_raw == "Status 150"

    def test_empty_changelog(self) -> None:
        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        transitions = canonical_changelog_to_transitions(
            issue_key="TEST-1",
            changelog_events=[],
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
            labels=[],
        )

        assert transitions == []

    def test_changelog_with_mixed_field_changes(self) -> None:
        events = [
            JiraChangelogEvent(
                issue_key="TEST-1",
                event_id="evt-1",
                created_at="2025-01-15T10:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="assignee",
                        from_value=None,
                        to_value="user-1",
                        from_string=None,
                        to_string="Alice",
                    ),
                    JiraChangelogItem(
                        field="priority",
                        from_value="3",
                        to_value="1",
                        from_string="Medium",
                        to_string="High",
                    ),
                ],
                author=None,
            ),
            JiraChangelogEvent(
                issue_key="TEST-1",
                event_id="evt-2",
                created_at="2025-01-15T11:00:00+00:00",
                items=[
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
            JiraChangelogEvent(
                issue_key="TEST-1",
                event_id="evt-3",
                created_at="2025-01-15T12:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="labels",
                        from_value=None,
                        to_value="bug",
                        from_string=None,
                        to_string="bug",
                    ),
                    JiraChangelogItem(
                        field="status",
                        from_value="2",
                        to_value="3",
                        from_string="In Progress",
                        to_string="Done",
                    ),
                ],
                author=None,
            ),
        ]

        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        transitions = canonical_changelog_to_transitions(
            issue_key="TEST-1",
            changelog_events=events,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
            labels=[],
        )

        assert len(transitions) == 2
        assert transitions[0].from_status_raw == "Open"
        assert transitions[0].to_status_raw == "In Progress"
        assert transitions[1].from_status_raw == "In Progress"
        assert transitions[1].to_status_raw == "Done"

    def test_changelog_preserves_chronological_order(self) -> None:
        events = [
            JiraChangelogEvent(
                issue_key="TEST-1",
                event_id="evt-3",
                created_at="2025-01-17T10:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="status",
                        from_string="In Progress",
                        to_string="Done",
                    )
                ],
                author=None,
            ),
            JiraChangelogEvent(
                issue_key="TEST-1",
                event_id="evt-1",
                created_at="2025-01-15T10:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="status",
                        from_string="Open",
                        to_string="To Do",
                    )
                ],
                author=None,
            ),
            JiraChangelogEvent(
                issue_key="TEST-1",
                event_id="evt-2",
                created_at="2025-01-16T10:00:00+00:00",
                items=[
                    JiraChangelogItem(
                        field="status",
                        from_string="To Do",
                        to_string="In Progress",
                    )
                ],
                author=None,
            ),
        ]

        status_mapping = load_status_mapping()
        identity = MockIdentityResolver()

        transitions = canonical_changelog_to_transitions(
            issue_key="TEST-1",
            changelog_events=events,
            status_mapping=status_mapping,
            identity=identity,  # type: ignore[arg-type]
            labels=[],
        )

        assert len(transitions) == 3
        assert transitions[0].from_status_raw == "Open"
        assert transitions[1].from_status_raw == "To Do"
        assert transitions[2].from_status_raw == "In Progress"
