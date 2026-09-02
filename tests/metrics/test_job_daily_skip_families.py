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

CHAOS-4275 (repo_user_commit) added a SECOND, differently-shaped gate: unlike
team_wellbeing, `compute_daily_metrics` is still called even when
"repo_user_commit" is in skip_families, because `result.repo_metrics` is a
live in-process input to `_write_compounding_risk_for_day` a few lines
later and compounding_risk has no other source for it -- only the WRITE is
skipped. A codex adversarial review on the Go port caught that this gate was
entirely missing in an earlier revision (the native executor and this
unconditional write both fired for every partition); these tests pin the
fixed contract the same way the team_wellbeing tests above pin theirs.

CHAOS-4293 (deploy) has the SAME shape as repo_user_commit: `deploy_metrics`
is still computed when "deploy" is in skip_families, because it also feeds
`_note_family_zero_rows("deploy", deploy_metrics, day=d)` (the
CHAOS-4246/CHAOS-4263 staleness-with-source-data check), which has no other
source for it -- only `s.write_deploy_metrics(deploy_metrics)` is skipped.
Codex round 1 on the Go port (CHAOS-4293) caught this gate missing entirely,
the identical class of finding CHAOS-4275's own gate closed: without it, the
native DeployExecutor and this unconditional write both fire for every
partition, doubling every (org_id, repo_id, day) deploy_metrics_daily row's
generation. `test_deploy_in_skip_families_writes_nothing_but_still_computes`
is the red-on-baseline proof: it fails against the tree before this guard
existed (write_deploy_metrics fires unconditionally) and passes after.

CHAOS-4292 (cicd) added a gate shaped like team_wellbeing's (compute is
skipped entirely -- cicd_metrics has no downstream in-process consumer in
this function). It also has its OWN failure mode neither prior gate has:
cicd_metrics feeds `_note_family_zero_rows("cicd", cicd_metrics, day=d)`
(CHAOS-4246), which records a "zero rows computed" DEGRADE signal. Skipping
compute alone would leave cicd_metrics=[] and fire a FALSE zero-rows note
even when the native Go executor wrote real rows for this partition -- so
the note itself must also be skipped. The tests below pin both halves.

CHAOS-4277 (file_hotspots + file_risk_hotspots) added the SAME write-only
gate shape as repo_user_commit: `compute_file_hotspots`/
`compute_file_risk_hotspots` are still called (neither `all_file_metrics`
nor `all_file_hotspots` feeds anything else downstream, so this is a
deliberate "smallest diff over the reviewed precedent" choice, not a hard
requirement the way repo_user_commit's compounding_risk dependency is), but
`write_file_metrics`/`write_file_hotspot_daily` are gated. This gate was
caught MISSING ENTIRELY during cross-lane review (lane-4293's codex round
flagged the same class on a sibling port, which pointed back at this PR) --
the native Go executor and this unconditional write would otherwise both
fire for every partition, doubling every row in both append-only tables.
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
        self.cicd_metrics_writes: list[Any] = []

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

    def write_cicd_metrics(self, rows: Any) -> None:
        # Mirrors write_cicd_metrics's own production no-op-on-empty
        # semantics (sinks/clickhouse/ci.py) -- same discipline as
        # write_team_metrics above. Codex round-4 finding: the generic
        # __getattr__ fallback below records a "write_cicd_metrics" call
        # regardless of row count, so a regression that silently emptied
        # cicd_metrics (e.g. mis-wiring the unrelated-family skip to also
        # catch "cicd") would still show the call as present. Only
        # recording on a non-empty list, and capturing the rows, lets tests
        # assert on ACTUAL row content, not just call presence.
        if not rows:
            return
        self.write_calls.append("write_cicd_metrics")
        self.cicd_metrics_writes.append(list(rows))

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
        # One in-window pipeline run for REPO_ID -- enough for
        # compute_cicd_metrics_daily to produce exactly one row when NOT
        # skipped, so the skip tests below can tell "computed nothing"
        # apart from "computed something and just didn't write it".
        pipeline_row = {
            "repo_id": REPO_ID,
            "run_id": "run-1",
            "status": "success",
            "queued_at": None,
            "started_at": datetime(2025, 12, 18, 9, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2025, 12, 18, 9, 10, tzinfo=timezone.utc),
        }
        return [pipeline_row], []

    async def load_testops_pipeline_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_test_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_historical_failed_case_names(
        self, *a: Any, **k: Any
    ) -> dict:
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
    skip_families -- only team_wellbeing, repo_user_commit, incident, deploy,
    and cicd check this set today. "file_hotspots" has no Go executor yet."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"file_hotspots"},
    )

    assert "team_metrics" in sink.write_calls
    assert "repo_metrics" in sink.write_calls
    # Codex round-4 (CHAOS-4292) finding: "write_cicd_metrics" in write_calls
    # alone proves only that the METHOD was called, not that it carried real
    # rows -- a regression that mistakenly wired "file_hotspots" to also
    # suppress cicd (e.g. an "|| family == other_skipped_thing" typo) could
    # leave cicd_metrics=[] and still pass that weaker assertion, since
    # write_cicd_metrics fires unconditionally in production and the assert
    # never inspected content. Assert the actual row, not just the call.
    assert "write_cicd_metrics" in sink.write_calls
    assert len(sink.cicd_metrics_writes) == 1
    assert len(sink.cicd_metrics_writes[0]) == 1
    cicd_row = sink.cicd_metrics_writes[0][0]
    assert cicd_row.repo_id == REPO_ID
    assert cicd_row.pipelines_count == 1


@pytest.mark.asyncio
async def test_repo_user_commit_in_skip_families_writes_nothing_but_still_computes(
    monkeypatch: Any,
) -> None:
    """Unlike team_wellbeing, compute_daily_metrics must still run when
    repo_user_commit is skipped -- result.repo_metrics feeds
    _write_compounding_risk_for_day (compounding_risk has no other source
    for it, and is not yet ported). Only the three writes are gated."""
    compute_calls: list[Any] = []
    original = job_daily.compute_daily_metrics

    def _spy(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(job_daily, "compute_daily_metrics", _spy)

    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"repo_user_commit"},
    )

    assert len(compute_calls) == 1
    assert "repo_metrics" not in sink.write_calls
    assert "write_user_metrics" not in sink.write_calls
    assert "write_commit_metrics" not in sink.write_calls


@pytest.mark.asyncio
async def test_repo_user_commit_skip_does_not_affect_other_families(
    monkeypatch: Any,
) -> None:
    """Naming repo_user_commit in skip_families must not perturb team_metrics
    or any other family's write path."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"repo_user_commit"},
    )

    assert "team_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_deploy_in_skip_families_writes_nothing_but_still_computes(
    monkeypatch: Any,
) -> None:
    """CHAOS-4293 red-on-baseline proof. Before the skip_deploy_write guard
    existed, write_deploy_metrics fired unconditionally regardless of
    skip_families, which this test would have caught (it asserts the write
    is ABSENT). compute_deploy_metrics_daily must still run --
    deploy_metrics feeds _note_family_zero_rows a few lines later, which has
    no other source for it."""
    compute_calls: list[Any] = []
    original = job_daily.compute_deploy_metrics_daily

    def _spy(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(job_daily, "compute_deploy_metrics_daily", _spy)

    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"deploy"},
    )

    assert len(compute_calls) == 1
    assert "write_deploy_metrics" not in sink.write_calls


@pytest.mark.asyncio
async def test_deploy_skip_does_not_affect_other_families(
    monkeypatch: Any,
) -> None:
    """Naming deploy in skip_families must not perturb team_metrics/repo_metrics
    or any other family's write path."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"deploy"},
    )

    assert "team_metrics" in sink.write_calls
    assert "repo_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_deploy_not_skipped_writes_unconditionally(monkeypatch: Any) -> None:
    """Baseline sanity: with deploy NOT in skip_families, write_deploy_metrics
    still fires every partition regardless of row content -- mirrors
    write_repo_metrics' own unconditional-call shape (production sinks
    no-op internally on an empty list; this call site does not gate on
    truthiness)."""
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

    assert "write_deploy_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_cicd_not_skipped_computes_and_writes_real_rows(
    monkeypatch: Any,
) -> None:
    """Baseline (skip_families empty): compute_cicd_metrics_daily runs and
    produces the one row _FakeLoader.load_cicd_data's fixture pipeline run
    implies, and no false zero-rows note fires for it."""
    zero_rows_calls: list[tuple[str, str]] = []
    original_record = job_daily.record_metrics_family_zero_rows

    def _spy_record(*, family: str, cause: str) -> None:
        zero_rows_calls.append((family, cause))
        original_record(family=family, cause=cause)

    monkeypatch.setattr(job_daily, "record_metrics_family_zero_rows", _spy_record)

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

    assert "write_cicd_metrics" in sink.write_calls
    assert len(sink.cicd_metrics_writes) == 1
    assert len(sink.cicd_metrics_writes[0]) == 1
    cicd_row = sink.cicd_metrics_writes[0][0]
    assert cicd_row.repo_id == REPO_ID
    assert cicd_row.pipelines_count == 1
    assert not any(family == "cicd" for family, _cause in zero_rows_calls)


@pytest.mark.asyncio
async def test_cicd_in_skip_families_computes_nothing_and_notes_no_false_zero_rows(
    monkeypatch: Any,
) -> None:
    """CHAOS-4292: when the Go dispatcher reports cicd already computed and
    wrote this scope, this job must (1) never call
    compute_cicd_metrics_daily and (2) never fire the "cicd" zero-rows-
    computed DEGRADE note -- an earlier revision of this gate skipped only
    (1), which left cicd_metrics=[] and made _note_family_zero_rows fire a
    FALSE "no_rows_computed" signal on every native-executor partition
    regardless of how many rows Go actually wrote."""
    compute_calls: list[Any] = []
    original_compute = job_daily.compute_cicd_metrics_daily

    def _spy_compute(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original_compute(*args, **kwargs)

    monkeypatch.setattr(job_daily, "compute_cicd_metrics_daily", _spy_compute)

    zero_rows_calls: list[tuple[str, str]] = []

    def _spy_record(*, family: str, cause: str) -> None:
        zero_rows_calls.append((family, cause))

    monkeypatch.setattr(job_daily, "record_metrics_family_zero_rows", _spy_record)

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

    assert compute_calls == []
    assert not any(family == "cicd" for family, _cause in zero_rows_calls)


@pytest.mark.asyncio
async def test_cicd_skip_does_not_affect_other_families(monkeypatch: Any) -> None:
    """Naming cicd in skip_families must not perturb team_metrics/repo_metrics
    or any other family's compute or write path."""
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
    assert "repo_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_file_hotspots_in_skip_families_skips_write_but_still_computes(
    monkeypatch: Any,
) -> None:
    """file_hotspots has a native Go executor (FileHotspotsExecutor). Same
    write-only-skip shape as repo_user_commit: compute_file_hotspots still
    runs (it feeds nothing else downstream, but skipping compute too is not
    required for correctness and this keeps the diff minimal against the
    reviewed precedent), only write_file_metrics is gated."""
    compute_calls: list[Any] = []
    original = job_daily.compute_file_hotspots

    def _spy(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(job_daily, "compute_file_hotspots", _spy)

    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"file_hotspots"},
    )

    assert len(compute_calls) == 1
    assert "write_file_metrics" not in sink.write_calls
    # Unrelated families/writes are unaffected.
    assert "team_metrics" in sink.write_calls
    assert "repo_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_file_hotspots_skip_families_none_writes_it(monkeypatch: Any) -> None:
    """Red-on-baseline counterpart: without the gate, write_file_metrics
    fires every time regardless of skip_families -- this pins that it FIRES
    when file_hotspots is NOT skipped, so the skip test above is meaningful
    (not merely a family that never writes in this fixture)."""
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

    assert "write_file_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_file_risk_hotspots_in_skip_families_skips_write_but_still_computes(
    monkeypatch: Any,
) -> None:
    """file_risk_hotspots has a native Go executor (FileRiskHotspotsExecutor).
    Same write-only-skip shape as file_hotspots/repo_user_commit above."""
    compute_calls: list[Any] = []
    original = job_daily.compute_file_risk_hotspots

    def _spy(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(job_daily, "compute_file_risk_hotspots", _spy)

    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
        skip_families={"file_risk_hotspots"},
    )

    assert len(compute_calls) == 1
    assert "write_file_hotspot_daily" not in sink.write_calls
    assert "team_metrics" in sink.write_calls
    assert "repo_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_file_risk_hotspots_skip_families_none_writes_it(
    monkeypatch: Any,
) -> None:
    """Red-on-baseline counterpart for file_risk_hotspots, mirroring
    test_file_hotspots_skip_families_none_writes_it above."""
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

    assert "write_file_hotspot_daily" in sink.write_calls
