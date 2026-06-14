"""CHAOS-2377: daily-job work-item state-duration rollup wiring.

``compute_work_item_state_durations_daily`` already existed and was used by the
fixtures runner and ``job_work_items``, but the *scheduled* daily job
(``run_daily_metrics_job``) never invoked it. As a result
``work_item_state_durations_daily`` stayed empty for real orgs and the /metrics
Flow Sankey + Flame and Operating Review state-duration panel rendered nothing.

These tests pin the seam: the daily job must invoke the state-duration compute
with the day's already-loaded work_items/transitions and persist the result via
``sink.write_work_item_state_durations``. The compute's own row-mapping logic is
covered by ``tests/test_work_item_state_durations_compute.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from dev_health_ops.metrics import job_daily
from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition

DAY = date(2025, 12, 18)
START = datetime(2025, 12, 18, tzinfo=timezone.utc)
END = datetime(2025, 12, 19, tzinfo=timezone.utc)


def _sample_work_items() -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
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
        created_at=datetime(2025, 12, 17, 20, 0, tzinfo=timezone.utc),
        updated_at=datetime(2025, 12, 19, 12, 0, tzinfo=timezone.utc),
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
    return [item], transitions


class _RecordingSink:
    """Captures the work-item state-duration rows the daily job persists."""

    org_id = ""

    def __init__(self, db_url: str) -> None:  # mirrors ClickHouseMetricsSink(db_url)
        self.state_durations: list[Any] = []

    def ensure_tables(self) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return []

    def write_work_item_state_durations(self, rows: list[Any]) -> None:
        self.state_durations.extend(rows)

    # Every other write_* / metric sink method is a no-op for this seam test.
    def __getattr__(self, name: str) -> Any:
        if name.startswith("write_"):
            return lambda *a, **k: None
        raise AttributeError(name)


class _FakeLoader:
    """Returns the day's work items/transitions and empty everything else."""

    def __init__(self, work_items: list[Any], transitions: list[Any]) -> None:
        self._work_items = work_items
        self._transitions = transitions

    async def load_git_rows(self, *a: Any, **k: Any) -> tuple[list, list, list]:
        return [], [], []

    async def load_cicd_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_pipeline_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_test_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_coverage_data(self, *a: Any, **k: Any) -> list:
        return []

    async def load_incidents(self, *a: Any, **k: Any) -> list:
        return []

    async def load_work_items(self, *a: Any, **k: Any) -> tuple[list, list]:
        return self._work_items, self._transitions


@pytest.mark.asyncio
async def test_daily_job_invokes_state_duration_compute_and_persists(
    monkeypatch: Any,
) -> None:
    work_items, transitions = _sample_work_items()
    sink = _RecordingSink("clickhouse://test")
    loader = _FakeLoader(work_items, transitions)

    calls: dict[str, Any] = {}

    real_compute = job_daily.compute_work_item_state_durations_daily

    def spy_compute(**kwargs: Any) -> list[Any]:
        calls["work_items"] = kwargs["work_items"]
        calls["transitions"] = kwargs["transitions"]
        calls["day"] = kwargs["day"]
        rows = real_compute(**kwargs)
        calls["rows"] = rows
        return rows

    # Sink + loader seams.
    monkeypatch.setattr(job_daily, "ClickHouseMetricsSink", lambda db_url: sink)

    async def fake_get_loader(*a: Any, **k: Any) -> Any:
        return loader

    monkeypatch.setattr(job_daily, "_get_loader", fake_get_loader)

    # The state-duration compute under test.
    monkeypatch.setattr(
        job_daily, "compute_work_item_state_durations_daily", spy_compute
    )

    # Neutralize the surrounding daily-job machinery so we reach the work-item
    # block without a live ClickHouse / identity / benchmarking dependency.
    async def _noop_init_team_resolver(*a: Any, **k: Any) -> None:
        return None

    class _NullResolver:
        def resolve(self, *a: Any, **k: Any) -> tuple[None, None]:
            return (None, None)

    monkeypatch.setattr(job_daily, "init_team_resolver", _noop_init_team_resolver)
    monkeypatch.setattr(job_daily, "get_team_resolver", lambda: _NullResolver())
    monkeypatch.setattr(
        job_daily, "build_repo_pattern_resolver", lambda *a, **k: _NullResolver()
    )
    monkeypatch.setattr(job_daily, "load_identity_resolver", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "discover_repos", lambda **k: [])
    monkeypatch.setattr(
        job_daily, "build_governance_rows_for_day", lambda *a, **k: ([], [])
    )
    monkeypatch.setattr(
        job_daily, "_extract_ai_workflow_for_day", lambda **k: ([], [], [], [], [], [])
    )
    monkeypatch.setattr(job_daily, "compute_ai_impact_metrics_daily", lambda **k: [])
    monkeypatch.setattr(job_daily, "run_benchmarking_for_day", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "_write_compounding_risk_for_day", lambda **k: 0)

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id="22222222-2222-2222-2222-222222222222",
        skip_finalize=True,
    )

    # The compute ran for the day, fed the day's already-loaded rows.
    assert calls["day"] == DAY
    assert calls["work_items"] == work_items
    assert calls["transitions"] == transitions

    # The mapped rows reached the sink via write_work_item_state_durations.
    assert calls["rows"], "compute produced no rows from the sample transitions"
    assert sink.state_durations == calls["rows"]
    statuses = {r.status for r in sink.state_durations}
    assert "in_progress" in statuses
