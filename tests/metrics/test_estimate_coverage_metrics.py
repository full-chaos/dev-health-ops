from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.metrics.compute_work_items import (
    compute_estimate_coverage_metrics_daily,
)
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemProvider,
    WorkItemStatusCategory,
)


def _item(
    provider: WorkItemProvider,
    item_id: str,
    points: float | None,
    *,
    status: WorkItemStatusCategory = "todo",
    completed_at: datetime | None = None,
    closed_at: datetime | None = None,
) -> WorkItem:
    return WorkItem(
        work_item_id=f"{provider}:{item_id}",
        provider=provider,
        title=item_id,
        type="task",
        status=status,
        status_raw=status,
        project_id="scope-a",
        created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        completed_at=completed_at,
        closed_at=closed_at,
        story_points=points,
    )


def test_compute_estimate_coverage_counts_null_as_unestimated_and_zero_as_estimated():
    records = compute_estimate_coverage_metrics_daily(
        day=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
        work_items=[
            _item("jira", "estimated", 3.0),
            _item("jira", "zero", 0.0),
            _item("jira", "missing", None),
        ],
        computed_at=datetime(2026, 6, 30, 12, tzinfo=timezone.utc),
    )

    assert len(records) == 1
    record = records[0]
    assert record.estimated_count == 2
    assert record.unestimated_count == 1
    assert record.backlog_size == 3
    assert record.ratio == pytest.approx(2 / 3)


@pytest.mark.parametrize("provider", ["jira", "gitlab", "github", "linear"])
def test_compute_estimate_coverage_provider_matrix_uses_normalized_story_points(
    provider: WorkItemProvider,
):
    records = compute_estimate_coverage_metrics_daily(
        day=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
        work_items=[
            _item(provider, "estimated", 5.0),
            _item(provider, "zero", 0.0),
            _item(provider, "missing", None),
        ],
        computed_at=datetime(2026, 6, 30, 12, tzinfo=timezone.utc),
    )

    assert len(records) == 1
    assert records[0].provider == provider
    assert records[0].estimated_count == 2
    assert records[0].unestimated_count == 1
    assert records[0].backlog_size == 3


def test_compute_estimate_coverage_excludes_completed_backlog_items():
    records = compute_estimate_coverage_metrics_daily(
        day=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
        work_items=[
            _item("github", "open", None),
            WorkItem(
                work_item_id="github:done",
                provider="github",
                title="done",
                type="task",
                status="done",
                status_raw="done",
                project_id="scope-a",
                created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                completed_at=datetime(2026, 6, 2, tzinfo=timezone.utc),
                story_points=8.0,
            ),
        ],
        computed_at=datetime(2026, 6, 30, 12, tzinfo=timezone.utc),
    )

    assert len(records) == 1
    assert records[0].estimated_count == 0
    assert records[0].unestimated_count == 1
    assert records[0].backlog_size == 1


@pytest.mark.parametrize("provider", ["jira", "gitlab", "github", "linear"])
def test_compute_estimate_coverage_excludes_closed_terminal_backlog_items(
    provider: WorkItemProvider,
):
    records = compute_estimate_coverage_metrics_daily(
        day=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
        work_items=[
            _item(provider, "open", None),
            _item(
                provider,
                "canceled",
                8.0,
                status="canceled",
                closed_at=datetime(2026, 6, 2, tzinfo=timezone.utc),
            ),
        ],
        computed_at=datetime(2026, 6, 30, 12, tzinfo=timezone.utc),
    )

    assert len(records) == 1
    assert records[0].estimated_count == 0
    assert records[0].unestimated_count == 1
    assert records[0].backlog_size == 1


def test_compute_estimate_coverage_emits_zero_backlog_for_known_empty_grain():
    records = compute_estimate_coverage_metrics_daily(
        day=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
        work_items=[
            _item(
                "linear",
                "canceled",
                8.0,
                status="canceled",
                closed_at=datetime(2026, 6, 2, tzinfo=timezone.utc),
            ),
        ],
        computed_at=datetime(2026, 6, 30, 12, tzinfo=timezone.utc),
    )

    assert len(records) == 1
    assert records[0].estimated_count == 0
    assert records[0].unestimated_count == 0
    assert records[0].backlog_size == 0
    assert records[0].ratio is None
