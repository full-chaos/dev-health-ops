"""CHAOS-4350 PR 2: the historical failed-case-names window must be relative
to the PARTITION day being computed, not wall-clock "now".

`run_daily_metrics_job`'s `for d in days:` loop backfills multiple days in
one run; each day's own `[d-29, d)` history window must track `d`, not the
day the job happened to execute. A wall-clock bug here would silently feed
every backfilled day the SAME (today-anchored) historical window, making
`failure_recurrence_score` wrong for every day except the most recent one.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

import dev_health_ops.connectors  # noqa: F401  # lgtm[py/unused-import]
from dev_health_ops.metrics import job_daily

DAY = date(2025, 12, 18)
ORG_ID = "22222222-2222-2222-2222-222222222222"


class _RecordingSink:
    org_id = ""
    teams: list[Any] = []

    def __init__(self, db_url: str) -> None:
        pass

    def ensure_tables(self) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return []

    def __getattr__(self, name: str) -> Any:
        if name.startswith("write_"):
            return lambda *a, **k: None
        raise AttributeError(name)


class _CountingLoader:
    """Empty on every read; records every historical-window call's args."""

    def __init__(self) -> None:
        self.historical_calls: list[tuple[datetime, datetime]] = []

    async def load_git_rows(self, *a: Any, **k: Any) -> tuple[list, list, list]:
        return [], [], []

    async def load_cicd_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_pipeline_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_test_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_historical_failed_case_names(
        self, start: datetime, end: datetime, *a: Any, **k: Any
    ) -> dict[Any, set[str]]:
        self.historical_calls.append((start, end))
        return {}

    async def load_testops_coverage_data(self, *a: Any, **k: Any) -> list:
        return []

    async def load_incidents(self, *a: Any, **k: Any) -> list:
        return []

    async def load_work_items(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []


class _NullResolver:
    def resolve(self, *a: Any, **k: Any) -> tuple[None, None]:
        return (None, None)


def _neutralize_daily_job(monkeypatch: Any, *, sink: Any, loader: Any) -> None:
    monkeypatch.setattr(job_daily, "ClickHouseMetricsSink", lambda db_url: sink)

    async def fake_get_loader(*a: Any, **k: Any) -> Any:
        return loader

    monkeypatch.setattr(job_daily, "_get_loader", fake_get_loader)

    async def _noop_init_team_resolver(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(job_daily, "init_team_resolver", _noop_init_team_resolver)
    monkeypatch.setattr(job_daily, "get_team_resolver", _NullResolver)
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
async def test_backfill_uses_each_days_own_history_window_not_wall_clock(
    monkeypatch: Any,
) -> None:
    sink = _RecordingSink("clickhouse://test")
    loader = _CountingLoader()
    _neutralize_daily_job(monkeypatch, sink=sink, loader=loader)

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=3,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
    )

    # One historical call per backfilled day (DAY-2, DAY-1, DAY), never
    # collapsed to a single wall-clock-anchored call.
    assert len(loader.historical_calls) == 3

    expected_windows = {
        (
            datetime.combine(d - timedelta(days=29), datetime.min.time()).replace(
                tzinfo=timezone.utc
            ),
            datetime.combine(d, datetime.min.time()).replace(tzinfo=timezone.utc),
        )
        for d in (DAY - timedelta(days=2), DAY - timedelta(days=1), DAY)
    }
    assert set(loader.historical_calls) == expected_windows, (
        f"expected each backfilled day's own [d-29, d) window, got: "
        f"{loader.historical_calls!r}"
    )

    # Explicitly: each window's upper bound must equal the historical
    # window's day + 29, not "today" (the day the test actually runs) --
    # this is what a wall-clock bug would get wrong.
    for start, end in loader.historical_calls:
        assert end - start == timedelta(days=29)
