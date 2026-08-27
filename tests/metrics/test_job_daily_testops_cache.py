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

    def __init__(self, *, case_rows_per_day: int = 0) -> None:
        self.testops_test_data_calls: list[tuple[datetime, datetime]] = []
        self._case_rows_per_day = case_rows_per_day

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
        cases = [
            {"repo_id": "r", "run_id": f"{start.date()}-{i}", "org_id": ORG_ID}
            for i in range(self._case_rows_per_day)
        ]
        return [], cases

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
async def test_single_day_run_makes_one_call_not_thirty(
    monkeypatch: Any,
) -> None:
    """CHAOS-4350 (codex round 3 P1): backfill_days=1 is the NORMAL
    production shape (`worker_metrics.py` invokes this job with
    backfill_days=1 per repository), and it has zero cross-day reuse
    opportunity -- the day loop below runs exactly once. Splitting it into
    30 per-day fetches would replace the original 2 queries with 60 serial
    `FINAL` queries against tables not partitioned by event time for no
    caching benefit at all -- a straight regression. This case must still
    make exactly ONE `load_testops_test_data` call spanning the whole
    30-day window, matching the pre-CHAOS-4350 shape (now capped inside
    that single call).
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

    assert len(loader.testops_test_data_calls) == 1, (
        f"expected exactly 1 load_testops_test_data call for a single-day "
        f"(backfill_days=1) run, got {len(loader.testops_test_data_calls)}: "
        f"{loader.testops_test_data_calls!r}"
    )
    start, end = loader.testops_test_data_calls[0]
    expected_start = datetime.combine(
        DAY - timedelta(days=29), datetime.min.time()
    ).replace(tzinfo=timezone.utc)
    expected_end = datetime.combine(
        DAY + timedelta(days=1), datetime.min.time()
    ).replace(tzinfo=timezone.utc)
    assert start == expected_start
    assert end == expected_end


@pytest.mark.asyncio
async def test_multi_day_backfill_still_caches_per_day(
    monkeypatch: Any,
) -> None:
    """backfill_days > 1 IS where per-day caching earns its keep (multiple
    overlapping windows across the day loop) -- pin that the branch added
    for the codex round 3 P1 fix didn't regress the round-2 behavior for
    the case it was actually built for.
    """
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

    # Same union-window math as test_backfill_fetches_each_calendar_day_of_
    # testops_history_once: 32 distinct single-day calls, not 3 (one huge
    # call each) and not 90 (3 * 30, the old uncached-per-day-window shape).
    assert len(loader.testops_test_data_calls) == 32
    for start, end in loader.testops_test_data_calls:
        assert end - start == timedelta(days=1)


@pytest.mark.asyncio
async def test_assembled_thirty_day_window_is_capped_even_when_each_day_is_small(
    monkeypatch: Any,
) -> None:
    """CHAOS-4350 (codex round 2 P1): each day's fetch is individually
    capped inside ``load_testops_test_data``, but stitching many
    under-cap days together (``_get_cached_testops_for_window``) was NOT
    itself re-capped -- up to ``backfill_days`` * 30 * max_rows could be
    assembled and handed to ``compute_test_metrics_daily``, defeating the
    guard's whole purpose one layer up. 10 rows/day * 30 days = 300 total,
    comfortably over a cap of 50 even though no single day is anywhere
    close to it -- this must now raise, not silently compute on 300 rows.

    Uses backfill_days=2: per the round-3 P1 fix, backfill_days=1 takes a
    single direct call (no per-day cache, no assembled-window check needed
    there -- see test_single_day_run_makes_one_call_not_thirty), so this
    needs the multi-day branch to exercise the assembled-cap path at all.
    """
    monkeypatch.setenv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", "50")
    sink = _RecordingSink("clickhouse://test")
    loader = _CountingLoader(case_rows_per_day=10)
    _neutralize_daily_job(monkeypatch, sink=sink, loader=loader)

    from dev_health_ops.metrics.loaders.clickhouse import TestopsRowCapExceeded

    with pytest.raises(TestopsRowCapExceeded) as exc_info:
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=2,
            provider="auto",
            org_id=ORG_ID,
            skip_finalize=True,
        )

    assert exc_info.value.table == "test_case_results:window"
    assert "testops_row_cap_exceeded" in str(exc_info.value)
