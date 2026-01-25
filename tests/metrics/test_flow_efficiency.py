from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.compute_work_items import (
    _calculate_flow_breakdown,
    compute_work_item_metrics_daily,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


def test_calculate_flow_breakdown_all_active():
    item_id = "item-1"

    start_time = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    complete_time = datetime(2023, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

    item = WorkItem(
        work_item_id=item_id,
        project_id="repo-1",
        provider="github",
        title="Test Item",
        created_at=start_time - timedelta(hours=1),
        updated_at=complete_time,
        started_at=start_time,
        completed_at=complete_time,
        type="story",
        status="done",
        status_raw="Done",
        assignees=[],
    )

    # 4 hours duration, no transitions (assumed active)
    active_h, wait_h = _calculate_flow_breakdown(item, [])

    # Logic in _calculate_flow_breakdown:
    # transitions=[]
    # start_utc != end_utc
    # current_status = "unknown" -> "in_progress"
    # loop empty.
    # duration = end - start (4 hours)
    # in_progress not in WAIT_STATUSES -> active_seconds += duration
    # Returns (4.0, 0.0)

    assert active_h == 4.0
    assert wait_h == 0.0


def test_calculate_flow_breakdown_with_wait():
    repo_id = "repo-1"
    item_id = "item-1"

    start_time = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    # 10:00 - 11:00 Active (1h)
    # 11:00 - 12:00 Blocked (1h)
    # 12:00 - 14:00 Active (2h)
    complete_time = datetime(2023, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

    item = WorkItem(
        work_item_id=item_id,
        project_id=repo_id,
        provider="github",
        title="Test Item Wait",
        created_at=start_time - timedelta(hours=1),
        updated_at=complete_time,
        started_at=start_time,
        completed_at=complete_time,
        type="story",
        status="done",
        status_raw="Done",
        assignees=[],
    )

    transitions = [
        WorkItemStatusTransition(
            work_item_id=item_id,
            provider="github",
            from_status="todo",
            to_status="in_progress",
            from_status_raw="Todo",
            to_status_raw="In Progress",
            occurred_at=start_time,
        ),
        WorkItemStatusTransition(
            work_item_id=item_id,
            provider="github",
            from_status="in_progress",
            to_status="blocked",
            from_status_raw="In Progress",
            to_status_raw="Blocked",
            occurred_at=datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        ),
        WorkItemStatusTransition(
            work_item_id=item_id,
            provider="github",
            from_status="blocked",
            to_status="in_progress",
            from_status_raw="Blocked",
            to_status_raw="In Progress",
            occurred_at=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ),
        WorkItemStatusTransition(
            work_item_id=item_id,
            provider="github",
            from_status="in_progress",
            to_status="done",
            from_status_raw="In Progress",
            to_status_raw="Done",
            occurred_at=complete_time,
        ),
    ]

    active_h, wait_h = _calculate_flow_breakdown(item, transitions)

    assert active_h == 3.0  # 1h start->blocked, 2h in_progress->done
    assert wait_h == 1.0  # 1h blocked->in_progress


def test_calculate_flow_breakdown_started_late():
    # Helper to ensure we handle transitions properly even if we don't have explicit "started" event
    # If started_at is t0, but first transition is at t1.
    pass


def test_compute_metrics_daily_populates_efficiency():
    day = date(2023, 1, 1)

    start_time = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    complete_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    item = WorkItem(
        work_item_id="item-2",
        project_id="repo-1",
        provider="github",
        title="Test Item Efficiency",
        created_at=start_time - timedelta(hours=1),
        updated_at=complete_time,
        started_at=start_time,
        completed_at=complete_time,
        type="story",
        status="done",
        status_raw="Done",
        assignees=[],
    )

    # 10-11: Blocked (Wait)
    # 11-12: In Progress (Active)
    # Total 2h. Active 1h. Efficiency 0.5
    transitions = [
        WorkItemStatusTransition(
            work_item_id="item-2",
            provider="github",
            from_status="todo",
            to_status="blocked",  # Immediate wait
            from_status_raw="Todo",
            to_status_raw="Blocked",
            occurred_at=start_time,
        ),
        WorkItemStatusTransition(
            work_item_id="item-2",
            provider="github",
            from_status="blocked",
            to_status="in_progress",
            from_status_raw="Blocked",
            to_status_raw="In Progress",
            occurred_at=datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        ),
    ]

    metrics_daily, user_daily, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[item],
        transitions=transitions,
        computed_at=datetime.now(timezone.utc),
    )

    assert len(cycle_times) == 1
    record = cycle_times[0]

    assert record.cycle_time_hours == 2.0
    assert record.active_time_hours == 1.0
    assert record.wait_time_hours == 1.0
    assert record.flow_efficiency == 0.5

    metrics_daily, user_daily, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[item],
        transitions=transitions,
        computed_at=datetime.now(timezone.utc),
    )

    assert len(cycle_times) == 1
    record = cycle_times[0]

    assert record.cycle_time_hours == 2.0
    assert record.active_time_hours == 1.0
    assert record.wait_time_hours == 1.0
    assert record.flow_efficiency == 0.5
