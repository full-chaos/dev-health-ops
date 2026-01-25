from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.compute_work_items import compute_work_item_metrics_daily
from dev_health_ops.models.work_items import WorkItem


def test_unassigned_completed_items_are_tracked() -> None:
    day = date(2025, 12, 18)
    start = datetime(2025, 12, 18, tzinfo=timezone.utc)
    computed_at = start + timedelta(hours=1)

    assigned = WorkItem(
        work_item_id="jira:ABC-1",
        provider="jira",
        project_key="ABC",
        project_id="1",
        title="Assigned",
        type="task",
        status="done",
        status_raw="Done",
        assignees=["alice@example.com"],
        reporter=None,
        created_at=start - timedelta(days=2),
        updated_at=start,
        started_at=start - timedelta(hours=5),
        completed_at=start + timedelta(hours=1),
        closed_at=start + timedelta(hours=1),
        labels=[],
    )

    unassigned = WorkItem(
        work_item_id="jira:ABC-2",
        provider="jira",
        project_key="ABC",
        project_id="1",
        title="Unassigned",
        type="task",
        status="done",
        status_raw="Done",
        assignees=[],
        reporter=None,
        created_at=start - timedelta(days=2),
        updated_at=start,
        started_at=start - timedelta(hours=2),
        completed_at=start + timedelta(hours=2),
        closed_at=start + timedelta(hours=2),
        labels=[],
    )

    group_rows, user_rows, _cycle_rows = compute_work_item_metrics_daily(
        day=day,
        work_items=[assigned, unassigned],
        transitions=[],
        computed_at=computed_at,
        team_resolver=None,
    )

    assert len(group_rows) == 1
    group = group_rows[0]
    assert group.team_id == "unassigned"
    assert group.items_completed == 2
    assert group.items_completed_unassigned == 1

    # We emit a pseudo-user row so unassigned closures can be charted.
    unassigned_user = next(r for r in user_rows if r.user_identity == "unassigned")
    assert unassigned_user.items_completed == 1
