"""run_daily_metrics_finalize(skip_families=...).

This gate protects any finalize-scope family with a native Go executor: when
the Go dispatcher has already computed and written a family for a run, it
names that family in skip_families on the compatibility-bridge finalize
request so this job does not recompute or rewrite it.

Why the skip is load-bearing rather than an optimisation: the tables these
finalize-scope families write are append-only, deduped by
(org_id, key..., day) LIMIT 1 BY computed_at DESC. Two writers therefore do
not conflict -- the LATER one wins silently. Without this gate the Python
finalize would recompute the family and its rows would supersede the native
ones, so a correct native executor would be invisibly overwritten with
nothing failing anywhere.

ic_finalize was the first finalize-scope family gated here (CHAOS-4290). Its
Python compute (compute_ic_metrics_daily / compute_ic_landscape_rolling) has
since been deleted entirely (CHAOS-4290 PR3, CHAOS-3092 no-straddle) -- the
native executor was the SOLE writer for it from the moment #2241's finalize
policy landed, so its skip_families gate was already dead weight, not a live
fallback, before the deletion. Its tests are gone with it; the parity proof
now lives in internal/jobs/metrics/daily/icfinalize/parity_golden_test.go.

benchmarking (CHAOS-5194) is the current live example of this gate's shape:
run_benchmarking_for_day takes no repo_id -- it recomputes the WHOLE ORG once
per finalize call -- so a native run and an un-gated Python run firing
together would duplicate a full row set across six append-only tables, not
merely redo redundant work.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from dev_health_ops.metrics import job_daily

DAY = date(2026, 9, 4)
ORG_ID = "00000000-0000-4000-8000-0000000000ic"[:36]


class _RecordingSink:
    def __init__(self, *_: Any, **__: Any) -> None:
        self.user_metrics_writes: list[Any] = []
        self.landscape_writes: list[Any] = []
        self.org_id = ""

    def ensure_tables(self) -> None:
        return None

    def write_user_metrics(self, rows: Any) -> None:
        self.user_metrics_writes.append(rows)

    def write_ic_landscape_rolling(self, rows: Any) -> None:
        self.landscape_writes.append(rows)

    async def get_all_teams(self) -> list[Any]:
        return []

    def query_dicts(self, *_: Any, **__: Any) -> list[Any]:
        """Several CHAOS-4365 writers that run AFTER the benchmarking block
        query the sink directly. Answering with no rows here is the
        structural fix -- an earlier revision chased them one monkeypatch at
        a time and each fix simply revealed the next caller."""
        return []

    def write_team_metrics(self, rows: Any) -> None:
        return None

    def write_compounding_risk(self, rows: Any) -> None:
        return None


class _FakeLoader:
    async def load_user_metrics_rolling_30d(
        self, *, as_of: date
    ) -> list[dict[str, Any]]:
        return []


def _neutralize_finalize(monkeypatch: Any, *, sink: Any, calls: dict[str, int]) -> None:
    """Strip finalize down to the gate under test."""
    monkeypatch.setattr(job_daily, "ClickHouseMetricsSink", lambda db_url: sink)
    monkeypatch.setattr(job_daily, "detect_db_type", lambda _: "clickhouse")

    async def fake_get_loader(*a: Any, **k: Any) -> Any:
        return _FakeLoader()

    monkeypatch.setattr(job_daily, "_get_loader", fake_get_loader)

    async def _noop_init_team_resolver(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(job_daily, "init_team_resolver", _noop_init_team_resolver)

    # Real dataclasses: run_daily_metrics_finalize calls dataclasses.fields()
    # on both record types before the benchmarking block, so a plain dict
    # raises TypeError there and the test never reaches the gate it is about.
    @dataclass
    class _UserRecord:
        day: date | None = None

    @dataclass
    class _WIRecord:
        day: date | None = None

    class _Deps:
        user_metrics_daily_record = _UserRecord
        work_item_user_metrics_daily_record = _WIRecord

        async def get_global_client(self, *_: Any, **__: Any) -> Any:
            return object()

        @staticmethod
        def clickhouse_query_dicts(*_: Any, **__: Any) -> list[Any]:
            return []

    monkeypatch.setattr(job_daily, "get_metrics_dependencies", lambda: _Deps())
    # Everything after the benchmarking block belongs to CHAOS-4365, not this
    # gate.
    monkeypatch.setattr(job_daily, "build_repo_pattern_resolver", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "discover_repos", lambda **k: [])
    # CHAOS-5084/no-straddle (#2275 v2): this used to ALSO neutralise
    # _write_compounding_risk_team_rows_for_day here (the finalize-scope
    # CHAOS-4365 writer that ran AFTER the benchmarking block, not what these
    # tests are about) -- deleted along with the function itself.
    # CompoundingRiskTeamExecutor (Go) is the sole writer for that scope now,
    # so there is nothing left in run_daily_metrics_finalize's Python path
    # for these IC-focused tests to neutralise on this family's behalf.


@pytest.mark.asyncio
async def test_benchmarking_in_skip_families_runs_nothing(monkeypatch: Any) -> None:
    """CHAOS-5194 (astra F3, #2277): benchmarking was relocated here from
    run_daily_metrics_job (partition scope) -- it now lives EXCLUSIVELY in
    run_daily_metrics_finalize, gated by the same skip_families shape
    ic_finalize used before its own Python compute was deleted. When the Go
    dispatcher names "benchmarking" in skip_families, the native
    BenchmarkingFinalizeExecutor already computed and wrote this org/day, so
    run_benchmarking_for_day must not run.

    Worth restating why the gate matters more here than for most families:
    run_benchmarking_for_day takes no repo_id -- it recomputes the WHOLE ORG
    once per finalize call -- so a native run and an un-gated Python run
    firing together would duplicate a full row set across six append-only
    tables, not merely redo redundant work."""
    sink = _RecordingSink()
    calls: dict[str, int] = {}
    _neutralize_finalize(monkeypatch, sink=sink, calls=calls)

    def _benchmarking_spy(*_a: Any, **_k: Any) -> None:
        calls["benchmarking"] = calls.get("benchmarking", 0) + 1

    monkeypatch.setattr(job_daily, "run_benchmarking_for_day", _benchmarking_spy)

    await job_daily.run_daily_metrics_finalize(
        db_url="clickhouse://test",
        day=DAY,
        org_id=ORG_ID,
        skip_families={"benchmarking"},
    )

    assert calls.get("benchmarking", 0) == 0, (
        "run_benchmarking_for_day ran despite being named in skip_families"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", [None, set(), {"some_other_family"}])
async def test_without_the_skip_benchmarking_still_runs(
    monkeypatch: Any, skip: set[str] | None
) -> None:
    """The CONTROL for the test above: without "benchmarking" in
    skip_families (or with an unrelated family named instead), the call still
    fires -- proving the assertion above is the gate, not a broken fixture,
    and that the gate is keyed on "benchmarking" specifically."""
    sink = _RecordingSink()
    calls: dict[str, int] = {}
    _neutralize_finalize(monkeypatch, sink=sink, calls=calls)

    def _benchmarking_spy(*_a: Any, **_k: Any) -> None:
        calls["benchmarking"] = calls.get("benchmarking", 0) + 1

    monkeypatch.setattr(job_daily, "run_benchmarking_for_day", _benchmarking_spy)

    await job_daily.run_daily_metrics_finalize(
        db_url="clickhouse://test",
        day=DAY,
        org_id=ORG_ID,
        skip_families=skip,
    )

    assert calls.get("benchmarking", 0) == 1
