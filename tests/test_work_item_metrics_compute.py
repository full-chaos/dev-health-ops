from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.compute_work_items import compute_work_item_metrics_daily
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.teams import TeamResolver


def test_work_item_cycle_time_percentiles() -> None:
    day = date(2025, 2, 1)
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)

    # Cycle times in hours: 1, 3, 5, 10 -> p50=4, p90=8.5 (linear interpolation).
    cycle_hours = [1, 3, 5, 10]
    items = []
    for i, hours in enumerate(cycle_hours, start=1):
        items.append(
            WorkItem(
                work_item_id=f"jira:ABC-{i}",
                provider="jira",
                project_key="ABC",
                project_id="1",
                title=f"Item {i}",
                type="task",
                status="done",
                status_raw="Done",
                assignees=["alice@example.com"],
                reporter="reporter@example.com",
                created_at=start - timedelta(days=2),
                updated_at=start + timedelta(hours=hours),
                started_at=start,
                completed_at=start + timedelta(hours=hours),
                closed_at=start + timedelta(hours=hours),
                labels=[],
            )
        )

    # One completed item missing started_at -> excluded from cycle distributions.
    items.append(
        WorkItem(
            work_item_id="jira:ABC-999",
            provider="jira",
            project_key="ABC",
            project_id="1",
            title="No started_at",
            type="task",
            status="done",
            status_raw="Done",
            assignees=["alice@example.com"],
            reporter="reporter@example.com",
            created_at=start - timedelta(days=2),
            updated_at=start + timedelta(hours=2),
            started_at=None,
            completed_at=start + timedelta(hours=2),
            closed_at=start + timedelta(hours=2),
            labels=[],
        )
    )

    team_resolver = TeamResolver(
        member_to_team={"alice@example.com": ("team-a", "Team A")}
    )
    computed_at = start + timedelta(days=1)

    group_rows, user_rows, cycle_rows = compute_work_item_metrics_daily(
        day=day,
        work_items=items,
        transitions=[],
        computed_at=computed_at,
        team_resolver=team_resolver,
    )

    assert len(group_rows) == 1
    group = group_rows[0]
    assert group.work_scope_id == "ABC"
    assert group.items_completed == 5
    assert group.items_started == 4
    assert group.cycle_time_p50_hours == 4.0
    assert group.cycle_time_p90_hours == 8.5

    # Per-item facts only include items completed on the day.
    assert {r.work_item_id for r in cycle_rows} == {w.work_item_id for w in items}

    assert len(user_rows) == 1
    user = user_rows[0]
    assert user.work_scope_id == "ABC"
    assert user.user_identity == "alice@example.com"
    assert user.items_completed == 5
    assert user.cycle_time_p50_hours == 4.0


def test_linear_item_in_project_attributes_team_via_project_key() -> None:
    """A Linear issue inside a project has work_scope_id = project name, but
    team mappings carry the TEAM key (project_key) — attribution must retry
    with project_key when the work_scope_id lookup misses (CHAOS-2262)."""
    from dev_health_ops.providers.teams import build_project_key_resolver

    day = date(2025, 2, 1)
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)

    item = WorkItem(
        work_item_id="linear:ENG-1",
        provider="linear",
        project_key="ENG",  # Linear TEAM key
        project_id="Q1 Platform Revamp",  # Linear PROJECT name
        title="In-project issue",
        type="task",
        status="done",
        status_raw="Done",
        assignees=[],  # no membership fallback available
        reporter=None,
        created_at=start - timedelta(days=2),
        updated_at=start + timedelta(hours=2),
        started_at=start,
        completed_at=start + timedelta(hours=2),
        closed_at=start + timedelta(hours=2),
        labels=[],
    )

    resolver = build_project_key_resolver(
        [{"id": "ENG", "name": "Engineering", "project_keys": ["ENG"]}]
    )

    group_rows, _, cycle_rows = compute_work_item_metrics_daily(
        day=day,
        work_items=[item],
        transitions=[],
        computed_at=start,
        team_resolver=TeamResolver(member_to_team={}),
        project_key_resolver=resolver,
    )

    assert group_rows, "expected at least one group row"
    assert group_rows[0].work_scope_id == "Q1 Platform Revamp"
    assert group_rows[0].team_id == "ENG"
    assert cycle_rows[0].team_id == "ENG"
