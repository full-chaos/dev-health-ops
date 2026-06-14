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

import dev_health_ops.connectors  # noqa: F401  # break providers<->connectors cycle
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
    teams: list[Any] = []

    def __init__(self, db_url: str) -> None:  # mirrors ClickHouseMetricsSink(db_url)
        self.state_durations: list[Any] = []

    def ensure_tables(self) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return list(type(self).teams)

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


class _NullResolver:
    """Membership resolver that never maps an identity to a team."""

    def resolve(self, *a: Any, **k: Any) -> tuple[None, None]:
        return (None, None)


def _neutralize_daily_job(
    monkeypatch: Any,
    *,
    sink: _RecordingSink,
    loader: _FakeLoader,
    spy_compute: Any,
) -> None:
    """Stub out the surrounding daily-job machinery (ClickHouse / identity /
    benchmarking) so the test reaches the work-item state-duration block.

    The team membership resolver is stubbed to a null resolver — assignee-based
    attribution is deliberately disabled so that project-key attribution can be
    exercised in isolation. The *project-key* resolver is left REAL so it is
    built from the sink's get_all_teams() data, exactly as production does.
    """
    monkeypatch.setattr(job_daily, "ClickHouseMetricsSink", lambda db_url: sink)

    async def fake_get_loader(*a: Any, **k: Any) -> Any:
        return loader

    monkeypatch.setattr(job_daily, "_get_loader", fake_get_loader)
    monkeypatch.setattr(
        job_daily, "compute_work_item_state_durations_daily", spy_compute
    )

    async def _noop_init_team_resolver(*a: Any, **k: Any) -> None:
        return None

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


@pytest.mark.asyncio
async def test_daily_job_invokes_state_duration_compute_and_persists(
    monkeypatch: Any,
) -> None:
    work_items, transitions = _sample_work_items()
    sink = _RecordingSink("clickhouse://test")
    _RecordingSink.teams = []
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

    _neutralize_daily_job(
        monkeypatch, sink=sink, loader=loader, spy_compute=spy_compute
    )

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


@pytest.mark.asyncio
async def test_daily_job_state_durations_attribute_unassigned_by_project_key(
    monkeypatch: Any,
) -> None:
    """CHAOS-2377 medium finding: the daily rollup must pass a project-key
    resolver so a team-owned-by-project-key item that is UNASSIGNED still lands
    under its team, not the normalized 'unassigned' bucket.

    The sample item has project_key 'ABC' (work_scope_id 'ABC') and an empty
    assignees list, so identity-based attribution cannot resolve a team. Only a
    project-key resolver built from teams_data can attribute it. Before the fix
    the daily job passed team_resolver only and these rows fell to
    'unassigned'.
    """
    work_items, transitions = _sample_work_items()
    assert work_items[0].assignees == []  # guard: no identity to resolve
    assert work_items[0].work_scope_id == "ABC"  # jira project_key drives scope

    sink = _RecordingSink("clickhouse://test")
    # teams_data the daily job feeds to build_project_key_resolver().
    _RecordingSink.teams = [
        {"id": "team-platform", "name": "Platform", "project_keys": ["ABC"]}
    ]
    loader = _FakeLoader(work_items, transitions)

    real_compute = job_daily.compute_work_item_state_durations_daily
    seen: dict[str, Any] = {}

    def spy_compute(**kwargs: Any) -> list[Any]:
        # The daily job must hand the compute a real project_key_resolver.
        seen["project_key_resolver"] = kwargs.get("project_key_resolver")
        rows = real_compute(**kwargs)
        seen["rows"] = rows
        return rows

    _neutralize_daily_job(
        monkeypatch, sink=sink, loader=loader, spy_compute=spy_compute
    )

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id="22222222-2222-2222-2222-222222222222",
        skip_finalize=True,
    )

    # A project-key resolver was actually built and passed through.
    pk_resolver = seen["project_key_resolver"]
    assert pk_resolver is not None
    assert pk_resolver.resolve("ABC") == ("team-platform", "Platform")

    # The unassigned item's state-duration rows are attributed to the
    # project-key team, NOT the 'unassigned' fallback.
    rows = [r for r in sink.state_durations if r.status == "in_progress"]
    assert rows, "expected an in_progress state-duration row"
    assert {r.team_id for r in rows} == {"team-platform"}
    assert {r.team_name for r in rows} == {"Platform"}
    assert all(r.team_id != "unassigned" for r in sink.state_durations)
