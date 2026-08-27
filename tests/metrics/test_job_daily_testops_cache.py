"""CHAOS-4350: testops loader reads must not be re-fetched per backfilled day.

Before this fix, ``run_daily_metrics_job``'s per-day loop called
``load_testops_test_data`` with a fresh rolling 30-day window on EVERY
backfilled day (``metrics/job_daily.py``, the ``for d in days:`` loop) --
so a ``--backfill N`` run re-loaded a full org's 30-day test-case history N
times over, uncached. Combined with no row cap (fixed separately in
``metrics/loaders/clickhouse.py``), this N-times-redundant refetch is what
amplified the observed MemoryError.

The fix mirrors the existing ``daily_commit_cache`` /
``_get_cached_commits_for_window`` pattern already in ``job_daily.py``:
fetch one calendar day at a time and cache it, so each day is loaded from
ClickHouse exactly once for the whole run, regardless of how many
backfilled days' rolling windows need it.

Red on unmodified origin/main: for a 3-day backfill, the old code calls
``load_testops_test_data`` 3 times, each with a full 30-day span -- i.e. 30
distinct calendar days' worth of data fetched on EACH of the 3 calls, with
the 29-day overlap between consecutive days' windows re-fetched from
scratch every time. Counting DISTINCT (start, end) day-window calls to
``load_testops_test_data`` pins that: unmodified, this test asserts 3 calls
(one per backfilled day) each spanning the full window, not a per-day
count. The fix makes it call once per distinct calendar day covering the
whole run's window instead.
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
    """Empty on every read; records every load_testops_test_data window."""

    def __init__(self) -> None:
        self.testops_test_data_calls: list[tuple[datetime, datetime]] = []

    async def load_git_rows(self, *a: Any, **k: Any) -> tuple[list, list, list]:
        return [], [], []

    async def load_cicd_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_pipeline_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_test_data(
        self, start: datetime, end: datetime, *a: Any, **k: Any
    ) -> tuple[list, list]:
        self.testops_test_data_calls.append((start, end))
        return [], []

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
async def test_backfill_fetches_each_calendar_day_of_testops_history_once(
    monkeypatch: Any,
) -> None:
    sink = _RecordingSink("clickhouse://test")
    loader = _CountingLoader()
    _neutralize_daily_job(monkeypatch, sink=sink, loader=loader)

    backfill_days = 3
    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=backfill_days,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
    )

    # Union window across all 3 backfilled days' rolling 30-day histories:
    # earliest day is DAY-2, whose history starts (DAY-2)-29 = DAY-31;
    # latest day is DAY itself. That's 32 distinct calendar days
    # (DAY-31 .. DAY inclusive) -- NOT backfill_days * 30 (= 90) which is
    # what an uncached per-day refetch would have produced.
    expected_distinct_days = 32

    assert len(loader.testops_test_data_calls) == expected_distinct_days, (
        f"expected exactly {expected_distinct_days} load_testops_test_data "
        f"calls (one per distinct calendar day across the whole run's "
        f"window), got {len(loader.testops_test_data_calls)} -- each "
        f"backfilled day's 30-day window is being refetched instead of "
        f"cached: {loader.testops_test_data_calls!r}"
    )

    # Every call must be a single-calendar-day window (the caching helper
    # fetches one day at a time), never the old full 30-day span.
    for start, end in loader.testops_test_data_calls:
        assert end - start == timedelta(days=1), (
            f"expected a single-day fetch window, got start={start} end={end} "
            f"(span={end - start})"
        )

    # No duplicate day-window fetched twice.
    assert len(set(loader.testops_test_data_calls)) == len(
        loader.testops_test_data_calls
    ), f"a calendar day was fetched more than once: {loader.testops_test_data_calls!r}"


@pytest.mark.asyncio
async def test_single_day_run_fetches_its_thirty_day_history_once_each(
    monkeypatch: Any,
) -> None:
    """backfill_days=1 (the common case) still needs the full 30-day history
    for the recurrence signal, but each of those 30 days is fetched exactly
    once (never twice, never re-derived).
    """
    sink = _RecordingSink("clickhouse://test")
    loader = _CountingLoader()
    _neutralize_daily_job(monkeypatch, sink=sink, loader=loader)

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
    )

    assert len(loader.testops_test_data_calls) == 30
    expected_days = {
        datetime.combine(DAY - timedelta(days=29 - i), datetime.min.time()).replace(
            tzinfo=timezone.utc
        )
        for i in range(30)
    }
    actual_days = {start for start, _end in loader.testops_test_data_calls}
    assert actual_days == expected_days
