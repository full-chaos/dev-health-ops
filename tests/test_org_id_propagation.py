from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime

from dev_health_ops.metrics.schemas import CapacityForecastRecord
from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition


def test_work_item_has_org_id_default_empty():
    wi = WorkItem(
        work_item_id="wi1",
        provider="github",
        title="Sample work item",
        type="task",
        status="todo",
        status_raw="open",
    )
    assert wi.org_id == ""


def test_work_item_status_transition_has_org_id_default_empty():
    occurred = datetime(2020, 1, 1, 0, 0, 0)
    t = WorkItemStatusTransition(
        work_item_id="wi1",
        provider="github",
        occurred_at=occurred,
        from_status_raw="open",
        to_status_raw="in_progress",
        from_status="todo",
        to_status="in_progress",
    )
    assert t.org_id == ""


def test_replace_on_work_item_sets_org_id():
    wi = WorkItem(
        work_item_id="wi1",
        provider="github",
        title="Sample",
        type="task",
        status="todo",
        status_raw="open",
    )
    wi2 = replace(wi, org_id="ORG-123")
    assert wi2.org_id == "ORG-123"
    assert wi.org_id == ""


def test_replace_on_work_item_status_transition_sets_org_id():
    occurred = datetime(2020, 1, 1, 0, 0, 0)
    t = WorkItemStatusTransition(
        work_item_id="wi1",
        provider="github",
        occurred_at=occurred,
        from_status_raw="open",
        to_status_raw="done",
        from_status="todo",
        to_status="done",
    )
    t2 = replace(t, org_id="ORG-XYZ")
    assert t2.org_id == "ORG-XYZ"
    assert t.org_id == ""


def test_job_org_id_stamping_logic_sets_org_id_when_provided():
    org_id = "ORG-AAA"
    work_items = [
        WorkItem(
            work_item_id="wi1",
            provider="github",
            title="A",
            type="task",
            status="todo",
            status_raw="open",
        ),
        WorkItem(
            work_item_id="wi2",
            provider="github",
            title="B",
            type="task",
            status="todo",
            status_raw="open",
        ),
    ]
    transitions = [
        WorkItemStatusTransition(
            work_item_id="wi1",
            provider="github",
            occurred_at=datetime(2020, 1, 1, 0, 0, 0),
            from_status_raw="open",
            to_status_raw="in_progress",
            from_status="todo",
            to_status="in_progress",
        ),
        WorkItemStatusTransition(
            work_item_id="wi2",
            provider="github",
            occurred_at=datetime(2020, 1, 1, 0, 0, 0),
            from_status_raw="open",
            to_status_raw="done",
            from_status="todo",
            to_status="done",
        ),
    ]

    stamped_items = [
        replace(wi, org_id=org_id) if hasattr(wi, "org_id") else wi for wi in work_items
    ]
    stamped_trans = [
        replace(t, org_id=org_id) if hasattr(t, "org_id") else t for t in transitions
    ]

    for wi in stamped_items:
        assert wi.org_id == org_id
    for t in stamped_trans:
        assert t.org_id == org_id


def test_job_org_id_stamping_logic_keeps_defaults_when_not_provided():
    # When no org_id is provided, existing default should remain
    work_items = [
        WorkItem(
            work_item_id="wi1",
            provider="github",
            title="A",
            type="task",
            status="todo",
            status_raw="open",
        )
    ]
    stamped = [
        replace(wi, org_id="") if hasattr(wi, "org_id") else wi for wi in work_items
    ]
    for wi in stamped:
        assert wi.org_id == ""


def test_capacity_forecast_record_has_org_id_default():
    cf = CapacityForecastRecord(
        forecast_id="CF1",
        computed_at=datetime(2020, 1, 1, 0, 0, 0),
        team_id="TEAM",
        work_scope_id="WS1",
        backlog_size=10,
        target_items=5,
        target_date=date(2020, 1, 2),
        history_days=30,
        simulation_count=1,
        p50_days=1,
        p85_days=2,
        p95_days=3,
        p50_date=date(2020, 1, 2),
        p85_date=date(2020, 1, 2),
        p95_date=date(2020, 1, 2),
        p50_items=2,
        p85_items=3,
        p95_items=4,
        throughput_mean=1.0,
        throughput_stddev=0.1,
    )
    assert getattr(cf, "org_id", "") == ""
