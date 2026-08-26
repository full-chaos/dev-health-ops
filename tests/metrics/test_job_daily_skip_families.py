"""CHAOS-4276: run_daily_metrics_job(skip_families=...).

team_wellbeing has a native Go executor (internal/jobs/metrics/daily/
wellbeing_native_executor.go). When PartitionHandler's Go dispatcher already
computed and wrote it for a partition, it names "team_wellbeing" in
skip_families on the compatibility-bridge request so this job does not
recompute or rewrite it. These tests pin the Python side of that contract:

1. skip_families=None (or empty) is a NO-OP -- team_wellbeing computes and
   writes exactly as it did before this parameter existed.
2. "team_wellbeing" in skip_families -> compute_team_wellbeing_metrics_daily
   is never called and write_team_metrics is never called (nothing written).
3. Every OTHER family is unaffected by skip_families naming team_wellbeing --
   naming a family with no native executor has no effect at all.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import pytest

import dev_health_ops.connectors  # noqa: F401  # lgtm[py/unused-import]
from dev_health_ops.metrics import job_daily

DAY = date(2025, 12, 18)
ORG_ID = "22222222-2222-2222-2222-222222222222"
REPO_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")


class _RecordingSink:
    org_id = ""
    teams: list[Any] = []

    def __init__(self, db_url: str) -> None:
        self.write_calls: list[str] = []
        self.team_metrics_writes: list[Any] = []

    def ensure_tables(self) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return []

    def write_team_metrics(self, rows: Any) -> None:
        # Mirrors write_team_metrics's own production no-op-on-empty
        # semantics (sinks/clickhouse/work_graph.py) -- an empty list must
        # not read as "nothing was written" here that would actually write
        # something in production.
        if not rows:
            return
        self.write_calls.append("team_metrics")
        self.team_metrics_writes.append(list(rows))

    def write_repo_metrics(self, rows: Any) -> None:
        self.write_calls.append("repo_metrics")

    def __getattr__(self, name: str) -> Any:
        if name.startswith("write_"):

            def _record(*_a: Any, **_k: Any) -> None:
                self.write_calls.append(name)

            return _record
        raise AttributeError(name)


class _FakeLoader:
    """A single commit, everything else empty -- enough for team_wellbeing
    to produce exactly one "unassigned" row when NOT skipped."""

    async def load_git_rows(self, *a: Any, **k: Any) -> tuple[list, list, list]:
        commit_row = {
            "repo_id": REPO_ID,
            "commit_hash": "abc123",
            "author_email": "dev@example.com",
            "author_name": "Dev",
            "committer_when": datetime(2025, 12, 18, 12, 0, tzinfo=timezone.utc),
            "file_path": "a.py",
            "additions": 1,
            "deletions": 0,
        }
        return [commit_row], [], []

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
async def test_skip_families_none_is_a_noop(monkeypatch: Any) -> None:
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families=None,
    )

    assert "team_metrics" in sink.write_calls
    assert len(sink.team_metrics_writes) == 1
    assert len(sink.team_metrics_writes[0]) == 1
    assert sink.team_metrics_writes[0][0].team_id == "unassigned"


@pytest.mark.asyncio
async def test_skip_families_empty_set_is_a_noop(monkeypatch: Any) -> None:
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families=set(),
    )

    assert "team_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_team_wellbeing_in_skip_families_writes_nothing(
    monkeypatch: Any,
) -> None:
    compute_calls: list[Any] = []
    original = job_daily.compute_team_wellbeing_metrics_daily

    def _spy(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(job_daily, "compute_team_wellbeing_metrics_daily", _spy)

    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"team_wellbeing"},
    )

    assert compute_calls == []
    assert "team_metrics" not in sink.write_calls
    assert sink.team_metrics_writes == []


@pytest.mark.asyncio
async def test_team_wellbeing_skip_does_not_affect_other_families(
    monkeypatch: Any,
) -> None:
    """Naming team_wellbeing in skip_families must not perturb any other
    family's compute or write path -- only the named family is affected."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"team_wellbeing"},
    )

    # repo_metrics is written unconditionally by run_daily_metrics_job's
    # final write block (s.write_repo_metrics(result.repo_metrics)) --
    # present whether or not repo_metrics itself has rows, and unaffected by
    # team_wellbeing being skipped.
    assert "repo_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_skip_families_naming_unrelated_family_has_no_effect(
    monkeypatch: Any,
) -> None:
    """A family with no native executor is unaffected by being named in
    skip_families -- only team_wellbeing checks this set today."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"cicd"},
    )

    assert "team_metrics" in sink.write_calls
