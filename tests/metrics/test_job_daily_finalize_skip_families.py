"""CHAOS-4290: run_daily_metrics_finalize(skip_families=...).

ic_finalize is the first FINALIZE-scope family with a native Go executor. When
Go's FinalizeHandler has already computed and written it for a run, it names
"ic_finalize" in skip_families on the compatibility-bridge finalize request so
this job does not recompute or rewrite it.

Why the skip is load-bearing rather than an optimisation: user_metrics_daily is
append-only, deduped ``ORDER BY computed_at DESC LIMIT 1 BY (org_id, repo_id,
author_email, day)``. Two writers therefore do not conflict -- the LATER one
wins silently. Without this gate the Python finalize would recompute the family
and its rows would supersede the native ones, so a correct native executor
would be invisibly overwritten with nothing failing anywhere.

The gate has the team_wellbeing SHAPE (compute AND write both skipped), not the
repo_user_commit shape (compute still runs, only the write is skipped): nothing
downstream in this function consumes ic_metrics or ic_landscape, so there is no
in-process reader to keep alive.
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
        """Several CHAOS-4365 writers that run AFTER the IC block query the
        sink directly. Answering with no rows here is the structural fix --
        the earlier revision chased them one monkeypatch at a time and each
        fix simply revealed the next caller."""
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
    """Strip finalize down to the IC block this test is about."""
    monkeypatch.setattr(job_daily, "ClickHouseMetricsSink", lambda db_url: sink)
    monkeypatch.setattr(job_daily, "detect_db_type", lambda _: "clickhouse")

    async def fake_get_loader(*a: Any, **k: Any) -> Any:
        return _FakeLoader()

    monkeypatch.setattr(job_daily, "_get_loader", fake_get_loader)

    async def _noop_init_team_resolver(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(job_daily, "init_team_resolver", _noop_init_team_resolver)
    monkeypatch.setattr(job_daily, "load_team_map", lambda *a, **k: {})

    # Real dataclasses: run_daily_metrics_finalize calls dataclasses.fields()
    # on both record types before the IC block, so a plain dict raises
    # TypeError there and the test never reaches the gate it is about.
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

    def _ic_metrics(**_: Any) -> list[Any]:
        calls["ic_metrics"] = calls.get("ic_metrics", 0) + 1
        return []

    def _ic_landscape(**_: Any) -> list[Any]:
        calls["ic_landscape"] = calls.get("ic_landscape", 0) + 1
        return []

    monkeypatch.setattr(job_daily, "compute_ic_metrics_daily", _ic_metrics)
    monkeypatch.setattr(job_daily, "compute_ic_landscape_rolling", _ic_landscape)
    # Everything after the IC block belongs to CHAOS-4365, not this gate.
    monkeypatch.setattr(job_daily, "build_repo_pattern_resolver", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "discover_repos", lambda **k: [])
    # The finalize-scope CHAOS-4365 writers, which run AFTER the IC block and
    # are not what these tests are about. Neutralised by their real names --
    # an earlier revision patched _write_compounding_risk_for_day, which
    # exists but is the PARTITION-scope one and is never called here, so the
    # patch silently did nothing and the run died further down.
    monkeypatch.setattr(
        job_daily, "_write_compounding_risk_team_rows_for_day", lambda **k: 0
    )


@pytest.mark.asyncio
async def test_ic_finalize_in_skip_families_computes_and_writes_nothing(
    monkeypatch: Any,
) -> None:
    sink = _RecordingSink()
    calls: dict[str, int] = {}
    _neutralize_finalize(monkeypatch, sink=sink, calls=calls)

    await job_daily.run_daily_metrics_finalize(
        db_url="clickhouse://test",
        day=DAY,
        org_id=ORG_ID,
        skip_families={"ic_finalize"},
    )

    assert calls.get("ic_metrics", 0) == 0, (
        "compute_ic_metrics_daily ran despite the skip"
    )
    assert calls.get("ic_landscape", 0) == 0, (
        "compute_ic_landscape_rolling ran despite the skip"
    )
    assert sink.user_metrics_writes == [], (
        "Python wrote user_metrics despite the skip -- those rows would supersede "
        "the native ones via computed_at DESC LIMIT 1 BY"
    )
    assert sink.landscape_writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", [None, set(), {"some_other_family"}])
async def test_without_the_skip_the_ic_block_still_runs(
    monkeypatch: Any, skip: set[str] | None
) -> None:
    """The CONTROL. Without it the test above passes for a run that did nothing.

    It also pins that naming an UNRELATED family has no effect, so the gate is
    keyed on "ic_finalize" specifically rather than on skip_families being
    non-empty.
    """
    sink = _RecordingSink()
    calls: dict[str, int] = {}
    _neutralize_finalize(monkeypatch, sink=sink, calls=calls)

    await job_daily.run_daily_metrics_finalize(
        db_url="clickhouse://test",
        day=DAY,
        org_id=ORG_ID,
        skip_families=skip,
    )

    assert calls.get("ic_metrics", 0) == 1
    assert calls.get("ic_landscape", 0) == 1
    assert len(sink.user_metrics_writes) == 1
    assert len(sink.landscape_writes) == 1


@pytest.mark.asyncio
async def test_benchmarking_in_skip_families_runs_nothing(monkeypatch: Any) -> None:
    """CHAOS-5194 (astra F3, #2277): benchmarking was relocated here from
    run_daily_metrics_job (partition scope) -- it now lives EXCLUSIVELY in
    run_daily_metrics_finalize, gated by the same skip_families shape as
    ic_finalize above. When the Go dispatcher names "benchmarking" in
    skip_families, the native BenchmarkingFinalizeExecutor already computed
    and wrote this org/day, so run_benchmarking_for_day must not run.

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
