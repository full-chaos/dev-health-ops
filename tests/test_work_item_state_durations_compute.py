from __future__ import annotations

from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition


def test_time_in_state_is_bucketed_to_day() -> None:
    day = date(2025, 12, 18)
    created = datetime(2025, 12, 17, 20, 0, tzinfo=timezone.utc)
    updated = datetime(2025, 12, 19, 12, 0, tzinfo=timezone.utc)

    item = WorkItem(
        work_item_id="jira:ABC-1",
        provider="jira",
        project_key="ABC",
        project_id="1",
        title="Test",
        type="task",
        status="done",
        status_raw="Done",
        assignees=[],
        reporter=None,
        created_at=created,
        updated_at=updated,
        started_at=None,
        completed_at=None,
    )

    transitions = [
        WorkItemStatusTransition(
            work_item_id="jira:ABC-1",
            provider="jira",
            occurred_at=datetime(2025, 12, 18, 2, 0, tzinfo=timezone.utc),
            from_status_raw="To Do",
            to_status_raw="In Progress",
            from_status="todo",
            to_status="in_progress",
            actor=None,
        ),
        WorkItemStatusTransition(
            work_item_id="jira:ABC-1",
            provider="jira",
            occurred_at=datetime(2025, 12, 18, 10, 0, tzinfo=timezone.utc),
            from_status_raw="In Progress",
            to_status_raw="Done",
            from_status="in_progress",
            to_status="done",
            actor=None,
        ),
    ]

    rows = compute_work_item_state_durations_daily(
        day=day,
        work_items=[item],
        transitions=transitions,
        computed_at=datetime(2025, 12, 19, tzinfo=timezone.utc),
        team_resolver=None,
    )

    by_status = {r.status: r for r in rows}
    # 00:00-02:00 todo = 2h; 02:00-10:00 in_progress = 8h; 10:00-24:00 done = 14h
    assert by_status["todo"].duration_hours == 2.0
    assert by_status["in_progress"].duration_hours == 8.0
    assert by_status["done"].duration_hours == 14.0
