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

CHAOS-4275 (repo_user_commit) used to have a SECOND, differently-shaped
write-only gate: unlike team_wellbeing, `compute_daily_metrics` stayed
called even when "repo_user_commit" was in skip_families, because
`result.repo_metrics` was a live in-process input to
`_write_compounding_risk_for_day` a few lines later and compounding_risk had
no other source for it -- only the WRITE was skipped. CHAOS-5308/CHAOS-3092
later superseded this write-only gate with outright DELETION of the
compute+write (and compounding_risk's own REPO-scope compute+write right
alongside it, since repo_metrics was its only remaining Python-side
consumer) -- see
test_repo_user_commit_compute_and_write_are_deleted_from_job_daily and
test_compounding_risk_compute_and_write_are_deleted_from_job_daily below
(replacing the write-only-skip tests this paragraph used to describe).

CHAOS-4293 (deploy) used to have the SAME shape as repo_user_commit:
`deploy_metrics` was still computed when "deploy" was in skip_families,
because it also fed `_note_family_zero_rows("deploy", deploy_metrics,
day=d)` (the CHAOS-4246/CHAOS-4263 staleness-with-source-data check), which
had no other source for it -- only `s.write_deploy_metrics(deploy_metrics)`
was skipped. Codex round 1 on the Go port (CHAOS-4293) caught this gate
missing entirely, the identical class of finding CHAOS-4275's own gate
closed: without it, the native DeployExecutor and this unconditional write
both fired for every partition, doubling every (org_id, repo_id, day)
deploy_metrics_daily row's generation. CHAOS-5234/CHAOS-3092 (CHAOS-5309)
later superseded this write-only gate with outright deletion of the
compute+write+zero-rows-note -- see
test_deploy_compute_and_write_are_deleted_from_job_daily below (replaces
the two write-only-skip tests this paragraph used to describe).

CHAOS-4292 (cicd) added a gate shaped like team_wellbeing's (compute is
skipped entirely -- cicd_metrics has no downstream in-process consumer in
this function). It also has its OWN failure mode neither prior gate has:
cicd_metrics feeds `_note_family_zero_rows("cicd", cicd_metrics, day=d)`
(CHAOS-4246), which records a "zero rows computed" DEGRADE signal. Skipping
compute alone would leave cicd_metrics=[] and fire a FALSE zero-rows note
even when the native Go executor wrote real rows for this partition -- so
the note itself must also be skipped. The tests below pin both halves.

CHAOS-4277 (file_hotspots + file_risk_hotspots) originally added the SAME
write-only gate shape repo_user_commit used to have: `compute_file_hotspots`/
`compute_file_risk_hotspots` were still called (neither `all_file_metrics`
nor `all_file_hotspots` feeds anything else downstream, so this was a
deliberate "smallest diff over the reviewed precedent" choice, not a hard
requirement the way repo_user_commit's compounding_risk dependency used to
be), but `write_file_metrics`/`write_file_hotspot_daily` were gated. This gate was
caught MISSING ENTIRELY during cross-lane review (lane-4293's codex round
flagged the same class on a sibling port, which pointed back at this PR) --
the native Go executor and this unconditional write would otherwise both
fire for every partition, doubling every row in both append-only tables.

CHAOS-5234/CHAOS-3092 (chris's ruling: "once go is in main that does the
same thing, skip flags are pointless") superseded BOTH file_hotspots' and
file_risk_hotspots' write-only gates with outright DELETION of their
compute+write call sites -- see
test_file_hotspots_compute_and_write_are_deleted_from_job_daily and
test_file_risk_hotspots_compute_and_write_are_deleted_from_job_daily below,
the runtime counterparts of the structural guard in
tests/metrics/test_job_daily_skip_families_structural_guard.py. CHAOS-5233
(work_item_attribution), CHAOS-5309 (deploy), and CHAOS-5308
(repo_user_commit + compounding_risk repo scope) apply the same rule to
more families -- see
test_work_item_attribution_compute_and_write_are_deleted_from_job_daily,
test_deploy_compute_and_write_are_deleted_from_job_daily,
test_repo_user_commit_compute_and_write_are_deleted_from_job_daily, and
test_compounding_risk_compute_and_write_are_deleted_from_job_daily below.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import pytest

import dev_health_ops.connectors  # noqa: F401  # lgtm[py/unused-import]
from dev_health_ops.metrics import job_daily
from dev_health_ops.models.work_items import WorkItem

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

    async def load_incidents(self, *a: Any, **k: Any) -> list:
        return []

    async def load_work_items(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []


class _FakeLoaderWithWorkItem(_FakeLoader):
    """Like _FakeLoader, but returns ONE real work item so
    compute_work_item_metrics_daily/compute_estimate_coverage_metrics_daily
    produce non-empty output for the "siblings unaffected" assertion below
    (base _FakeLoader.load_work_items returns ([], []), which never even
    reaches the `if work_items:` block)."""

    async def load_work_items(self, *a: Any, **k: Any) -> tuple[list, list]:
        item = WorkItem(
            work_item_id="gh:owner/repo#1",
            provider="github",
            title="Fix the thing",
            type="issue",
            status="in_progress",
            status_raw="In Progress",
            repo_id=REPO_ID,
            org_id=ORG_ID,
            created_at=datetime(2025, 12, 18, 9, 0, tzinfo=timezone.utc),
            updated_at=datetime(2025, 12, 18, 10, 0, tzinfo=timezone.utc),
        )
        return [item], []


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
    # CHAOS-5308/CHAOS-3092: no load_identity_resolver to neutralize here
    # anymore -- its only consumer was compute_daily_metrics' identity_
    # resolver argument, deleted alongside repo_user_commit's compute+write
    # (monkeypatch.setattr on a nonexistent attribute raises).
    monkeypatch.setattr(job_daily, "discover_repos", lambda **k: [])
    # CHAOS-5234/CHAOS-3092: no build_governance_rows_for_day to neutralize
    # here anymore -- job_daily.py no longer calls it at all (deleted, not
    # skip-gated; see CHAOS-5233's shape for work_item_attribution).
    # CHAOS-5216/CHAOS-5242: no _extract_ai_workflow_for_day to neutralize
    # either anymore -- both of its halves (ai_workflow, work_graph_edges)
    # are deleted, so the function itself no longer exists on job_daily at
    # all (monkeypatch.setattr on a nonexistent attribute raises).
    # CHAOS-5234/CHAOS-3092: no compute_ai_impact_metrics_daily to neutralize
    # here anymore -- job_daily.py no longer calls it at all (deleted, not
    # skip-gated; see CHAOS-5233's shape for work_item_attribution).
    monkeypatch.setattr(job_daily, "run_benchmarking_for_day", lambda *a, **k: None)
    # CHAOS-5308/CHAOS-3092: no _write_compounding_risk_for_day to neutralize
    # here anymore -- job_daily.py no longer calls it at all (deleted, not
    # skip-gated; see CHAOS-5233's shape for work_item_attribution).


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
        skip_families={"team_wellbeing"},
    )

    # CHAOS-5308: repo_metrics is no longer written at all (repo_user_commit's
    # compute+write is deleted). team_metrics is NOT a safe substitute here
    # either -- write_team_metrics no-ops on an empty list, and skipping
    # team_wellbeing is exactly what empties it in this fixture. Use
    # write_cicd_metrics/write_incident_metrics instead: both are
    # unconditional in _FakeLoader's fixture and unaffected by either
    # team_wellbeing or repo_user_commit being skipped/deleted.
    assert "write_cicd_metrics" in sink.write_calls
    assert "write_incident_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_skip_families_naming_unrelated_family_has_no_effect(
    monkeypatch: Any,
) -> None:
    """A family that does not check skip_families is unaffected by being
    named in it -- only team_wellbeing, repo_user_commit, incident, deploy,
    and cicd check this set today (testops_pipeline/testops_test/
    testops_coverage/testops_risk used to as well, until CHAOS-5245 deleted
    their Python compute+write entirely -- naming them now has no effect at
    all, not even a no-op skip). "file_hotspots" DOES have a Go native
    executor (FileHotspotsExecutor), but CHAOS-5234/CHAOS-3092 deleted its
    Python compute+write entirely rather than gating it, so there is nothing
    left in job_daily.py to check skip_families for this family at all --
    naming it here is still a no-op, for a different reason than before."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_families={"file_hotspots"},
    )

    assert "team_metrics" in sink.write_calls
    # CHAOS-5308: repo_metrics is no longer written (repo_user_commit's
    # compute+write is deleted) -- team_metrics above is this test's
    # unrelated-family signal now.
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
async def test_repo_user_commit_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092 close condition 3 (CHAOS-5308).

    repo_user_commit used to have the write-only-skip shape pinned by
    test_repo_user_commit_in_skip_families_writes_nothing_but_still_computes /
    test_repo_user_commit_skip_does_not_affect_other_families (both replaced
    by this test): compute_daily_metrics stayed called even when
    "repo_user_commit" was in skip_families, because result.repo_metrics fed
    _write_compounding_risk_for_day (compounding_risk had no other source
    for it) -- only the three writes were skipped. RepoUserCommitExecutor
    being native (CHAOS-4275) superseded that: the compute+writes are
    deleted entirely now, not skip-gated -- same rule as CHAOS-5233's
    work_item_attribution and CHAOS-5309's deploy, but unlike those cases,
    compute_daily_metrics itself is ALSO deleted (from compute.py, along
    with DailyMetricsResult in schemas.py and commit_size_bucket): rg
    confirmed zero production callers outside this call site. Deleting it
    also deleted compounding_risk's REPO-scope compute+write in the same
    PR -- see test_compounding_risk_compute_and_write_are_deleted_from_
    job_daily below -- since result.repo_metrics was its only remaining
    Python-side consumer.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    assert not hasattr(job_daily, "compute_daily_metrics"), (
        "compute_daily_metrics must not be imported into job_daily.py's "
        "module namespace at all"
    )

    for skip_families in (None, {"repo_user_commit"}):
        sink.write_calls = []
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "repo_metrics" not in sink.write_calls
        assert "write_user_metrics" not in sink.write_calls
        assert "write_commit_metrics" not in sink.write_calls
        # Unrelated families/writes are unaffected by the deletion.
        assert "team_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_deploy_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092 close condition 3 (CHAOS-5309).

    deploy used to have the write-only-skip shape pinned by
    test_deploy_in_skip_families_writes_nothing_but_still_computes /
    test_deploy_skip_does_not_affect_other_families (both replaced by this
    test): deploy_metrics fed `_note_family_zero_rows("deploy", ...)`, the
    CHAOS-4246/CHAOS-4263 staleness check, so only the write could be
    skipped. DeployExecutor being native (CHAOS-4293) superseded that --
    same rule as CHAOS-5233's work_item_attribution, but unlike that case,
    compute_deploy_metrics_daily itself is ALSO deleted (from
    compute_deployments.py): rg confirmed job_daily.py was its only real
    caller. The sibling constant DEPLOYMENT_FAILURE_STATUSES in the same
    module is NOT touched -- it has a real, separate caller
    (compute_dora.py, still Python) plus its own dedicated test coverage in
    test_job_dora.py.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    assert not hasattr(job_daily, "compute_deploy_metrics_daily"), (
        "compute_deploy_metrics_daily must not be imported into "
        "job_daily.py's module namespace at all"
    )

    for skip_families in (None, {"deploy"}):
        sink.write_calls = []
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "write_deploy_metrics" not in sink.write_calls
        # Unrelated families/writes are unaffected by the deletion.
        # CHAOS-5308: no longer asserting "repo_metrics" in sink.write_calls
        # here -- repo_user_commit's own compute+write is ALSO deleted in
        # this PR (see test_repo_user_commit_compute_and_write_are_deleted_
        # from_job_daily below); team_metrics is the surviving unrelated
        # signal.
        assert "team_metrics" in sink.write_calls


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
        skip_families={"cicd"},
    )

    # CHAOS-5308: repo_metrics is no longer written (repo_user_commit's
    # compute+write is deleted) -- team_metrics above is this test's
    # unrelated-family signal now.
    assert "team_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_file_hotspots_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092.

    Chris's ruling (verbatim, twice): "work_item_attribution python doesn't
    need a skip, it just needs to be deleted" / "once go is in main that
    does the same thing, skip flags are pointless." file_hotspots's native
    Go executor (FileHotspotsExecutor, CHAOS-4277) is the only writer of
    file_metrics_daily now, so -- unlike the write-only-skip shape it used
    to have (see this module's docstring) -- its daily compute+write call is
    gone from run_daily_metrics_job entirely, in every mode. This is the
    RUNTIME counterpart to the structural guard in
    tests/metrics/test_job_daily_skip_families_structural_guard.py.

    compute_file_hotspots itself IS ALSO deleted from the codebase now
    (src/dev_health_ops/metrics/hotspots.py, removed whole-file) -- a
    correction to an earlier pass on this same family, which left the
    module in place on the (wrong) premise that its fixture-generator/test
    callers counted as a real production caller. See
    test_file_risk_hotspots_compute_and_write_are_deleted_from_job_daily
    below for the module-existence assertion (checked once there rather
    than duplicated here) and this PR's own body for the writeup.
    """
    assert not hasattr(job_daily, "compute_file_hotspots"), (
        "compute_file_hotspots must not be imported into job_daily.py's "
        "module namespace at all"
    )

    for skip_families in (None, {"file_hotspots"}):
        sink = _RecordingSink("clickhouse://test")
        _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )

        assert "write_file_metrics" not in sink.write_calls
        # Unrelated families/writes are unaffected by the deletion.
        # CHAOS-5308: repo_metrics is no longer written (repo_user_commit's
        # own compute+write is deleted too) -- team_metrics above is the
        # surviving unrelated signal.
        assert "team_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_file_risk_hotspots_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092.

    Chris's ruling (verbatim, twice): "work_item_attribution python doesn't
    need a skip, it just needs to be deleted" / "once go is in main that
    does the same thing, skip flags are pointless." file_risk_hotspots's
    native Go executor (FileRiskHotspotsExecutor, CHAOS-4277) is the only
    writer of file_hotspot_daily now, so -- unlike the write-only-skip shape
    it used to have (see this module's docstring) -- its daily compute+write
    call is gone from run_daily_metrics_job entirely, in every mode. This is
    the RUNTIME counterpart to the structural guard in
    tests/metrics/test_job_daily_skip_families_structural_guard.py.

    compute_file_risk_hotspots itself IS ALSO deleted from the codebase now
    (src/dev_health_ops/metrics/hotspots.py, removed whole-file, alongside
    compute_file_hotspots and the private job_daily.py helpers
    _hotspot_repo_ids/_load_complexity_map_for_repo/_load_blame_map_for_repo)
    -- a correction to an earlier pass on this same family, which left the
    module in place on the (wrong) premise that its fixture-generator/test
    callers counted as a real production caller the way
    compute_work_item_team_attributions' actual sync-job caller does. See
    this PR's own body for the writeup.
    """
    assert not hasattr(job_daily, "compute_file_risk_hotspots"), (
        "compute_file_risk_hotspots must not be imported into job_daily.py's "
        "module namespace at all"
    )
    import importlib.util

    assert importlib.util.find_spec("dev_health_ops.metrics.hotspots") is None, (
        "dev_health_ops.metrics.hotspots must not exist at all -- "
        "compute_file_hotspots/compute_file_risk_hotspots and their shared "
        "dataclasses have no caller left anywhere once this family's "
        "job_daily.py call site (and its sibling file_hotspots) are both "
        "deleted; if a new, deliberate caller has reappeared, this assertion "
        "should be removed and explained, not silently loosened"
    )

    for skip_families in (None, {"file_risk_hotspots"}):
        sink = _RecordingSink("clickhouse://test")
        _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )

        assert "write_file_hotspot_daily" not in sink.write_calls
        # Unrelated families/writes are unaffected by the deletion.
        # CHAOS-5308: repo_metrics is no longer written (repo_user_commit's
        # own compute+write is deleted too) -- team_metrics above is the
        # surviving unrelated signal.
        assert "team_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_compounding_risk_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092 close condition 3 (CHAOS-5308).

    compounding_risk (REPO scope) used to have the cicd/team_wellbeing
    whole-call-skip shape pinned by
    test_compounding_risk_not_skipped_writes_repo_rows /
    test_compounding_risk_in_skip_families_writes_nothing /
    test_compounding_risk_skip_does_not_perturb_other_families (all three
    replaced by this test): when "compounding_risk" was in skip_families,
    job_daily.py must not call _write_compounding_risk_for_day at all (it
    writes straight to the sinks, nothing else in this function consumes
    its output). CompoundingRiskExecutor being native (CHAOS-4287)
    superseded that: the call is deleted entirely now, not skip-gated.
    _write_compounding_risk_for_day itself is ALSO deleted -- rg confirmed
    job_daily.py was its only real caller (build_compounding_risk_rows_
    for_day and _fetch_repo_metrics_for_day, its two helpers, are SHARED
    functions with a real other caller, job_compounding_risk.py's own
    standalone job, and are NOT touched). This deletion rides alongside
    repo_user_commit's own compute+write deletion in the same PR -- see
    test_repo_user_commit_compute_and_write_are_deleted_from_job_daily
    above -- since result.repo_metrics was compounding_risk's only
    remaining Python-side input. TEAM-scope compounding_risk_team
    (CHAOS-5084) is a separate family, already deleted, unaffected.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    assert not hasattr(job_daily, "_write_compounding_risk_for_day"), (
        "_write_compounding_risk_for_day must not be imported into "
        "job_daily.py's module namespace at all"
    )

    zero_rows_calls: list[tuple[str, str]] = []

    def _spy_record(*, family: str, cause: str) -> None:
        zero_rows_calls.append((family, cause))

    monkeypatch.setattr(job_daily, "record_metrics_family_zero_rows", _spy_record)

    for skip_families in (None, {"compounding_risk"}):
        sink.write_calls = []
        zero_rows_calls.clear()
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "write_compounding_risk_daily" not in sink.write_calls
        assert not any(
            family.startswith("compounding_risk") for family, _cause in zero_rows_calls
        )
        # Unrelated families/writes are unaffected by the deletion.
        assert "team_metrics" in sink.write_calls
        assert "write_cicd_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_review_edges_not_skipped_computes_and_writes(monkeypatch: Any) -> None:
    """Baseline for the skip test below: WITHOUT review_edges in
    skip_families, compute_review_edges_daily runs, so the "never called"
    assertion below is because of the gate rather than because the fixture
    never reaches that call site."""
    compute_calls: list[Any] = []
    original_compute = job_daily.compute_review_edges_daily

    def _spy_compute(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return original_compute(*args, **kwargs)

    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())
    monkeypatch.setattr(job_daily, "compute_review_edges_daily", _spy_compute)

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_families=None,
    )

    assert len(compute_calls) == 1


@pytest.mark.asyncio
async def test_review_edges_in_skip_families_computes_nothing(
    monkeypatch: Any,
) -> None:
    """CHAOS-4279: when the Go dispatcher reports review_edges already computed
    and wrote this scope, this job must neither call compute_review_edges_daily
    nor write the family.

    Compute is skipped outright, not merely the write: nothing else in
    run_daily_metrics_job reads review_edges between the compute and the write
    block, which makes this the cicd/team_wellbeing shape rather than
    repo_user_commit's write-only skip."""
    compute_calls: list[Any] = []

    def _spy_compute(*args: Any, **kwargs: Any) -> Any:
        compute_calls.append((args, kwargs))
        return []

    sink = _RecordingSink("clickhouse://test")
    # AFTER _neutralize_daily_job, so the helper cannot overwrite the spy.
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())
    monkeypatch.setattr(job_daily, "compute_review_edges_daily", _spy_compute)

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_families={"review_edges"},
    )

    assert compute_calls == []
    assert "write_review_edges" not in sink.write_calls


@pytest.mark.asyncio
async def test_review_edges_skip_does_not_perturb_other_families(
    monkeypatch: Any,
) -> None:
    """Naming review_edges in skip_families must not change any other family's
    writes -- the gate is one conditional around one compute and one write."""
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_families={"review_edges"},
    )

    # CHAOS-5308: repo_metrics is no longer written (repo_user_commit's
    # compute+write is deleted) -- team_metrics/write_cicd_metrics below are
    # this test's unrelated-family signals now.
    assert "team_metrics" in sink.write_calls
    assert "write_cicd_metrics" in sink.write_calls


# CHAOS-5194 (astra F3, #2277): the two benchmarking skip_families tests that
# used to live here (test_benchmarking_not_skipped_runs,
# test_benchmarking_in_skip_families_runs_nothing) tested a call site that no
# longer exists -- run_benchmarking_for_day was relocated from
# run_daily_metrics_job (partition scope, this file) to
# run_daily_metrics_finalize (finalize scope), for the same "runs once per
# org/day, not once per partition" reason compounding_risk_team and
# team_cognitive_load already live there. Their red/green replacements are
# test_benchmarking_in_skip_families_runs_nothing /
# test_without_the_skip_benchmarking_still_runs in
# test_job_daily_finalize_skip_families.py.


@pytest.mark.asyncio
async def test_work_item_attribution_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5233/CHAOS-3092 close condition 3.

    Chris's ruling (verbatim, twice): "work_item_attribution python doesn't
    need a skip, it just needs to be deleted" / "once go is in main that
    does the same thing, skip flags are pointless." Unlike every other
    family in this file, work_item_attribution gets NO skip_families
    handling at all -- its daily compute+write call is gone from
    run_daily_metrics_job entirely, in every mode. This is the RUNTIME
    counterpart to test_every_native_daily_family_has_a_skip_families_branch
    (renamed structural guard, tests/metrics/
    test_job_daily_skip_families_structural_guard.py), which proves the same
    thing at the source level.

    compute_work_item_team_attributions itself is NOT deleted from the
    codebase -- job_work_items.py's run_work_items_sync_job (a full-backfill
    sync job, unrelated to this function) still calls it directly, as do its
    own dedicated unit tests and the live-Python oracle comparator. Only
    run_daily_metrics_job's own call is gone.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoaderWithWorkItem())

    assert not hasattr(job_daily, "compute_work_item_team_attributions"), (
        "compute_work_item_team_attributions must not be imported into "
        "job_daily.py's module namespace at all"
    )

    for skip_families in (None, {"work_item_attribution"}):
        sink.write_calls = []
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "write_work_item_team_attributions" not in sink.write_calls
        # work_item/work_item_estimate must be entirely unaffected by the
        # deletion -- they share the same `if work_items:` block and the
        # same attribution_context, but neither reads work_item_attribution's
        # output.
        assert "write_work_item_metrics" in sink.write_calls
        assert "write_estimate_coverage_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_ai_governance_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092 close condition 3.

    ai_governance's daily compute is DELETED from job_daily.py, not
    skip-gated -- same rule as CHAOS-5233's work_item_attribution. Unlike
    that case, build_governance_rows_for_day itself is ALSO deleted (from
    audit/ai_governance/loaders.py): job_daily.py was its only real caller.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    assert not hasattr(job_daily, "build_governance_rows_for_day"), (
        "build_governance_rows_for_day must not be imported into "
        "job_daily.py's module namespace at all"
    )

    for skip_families in (None, {"ai_governance"}):
        sink.write_calls = []
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "write_ai_policy_events" not in sink.write_calls
        assert "write_ai_governance_coverage_daily" not in sink.write_calls
        # cicd (unrelated family, same partition) must be entirely
        # unaffected by the deletion.
        assert "write_cicd_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_ai_impact_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092 close condition 3.

    ai_impact's daily compute is DELETED from job_daily.py, not skip-gated --
    same rule as CHAOS-5233's work_item_attribution. Unlike that case,
    compute_ai_impact_metrics_daily itself is ALSO deleted (from
    metrics/ai_impact.py) -- codegraph_explore + rg confirmed its only real
    callers, once job_daily.py's own reference was removed, were its Go
    bit-exact oracle rot guard (TestAIImpactMatchesLivePythonProduction +
    testdata/python_ai_impact_oracle.py, both also deleted in this PR) and
    its own dedicated tests (tests/metrics/test_ai_impact.py, also deleted
    except for its one sink-write test, moved to construct a record
    directly instead of going through the now-deleted compute function).
    Also removes the pr_commit_stats build and ai_attribution_rows load
    that existed solely to feed it.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    assert not hasattr(job_daily, "compute_ai_impact_metrics_daily"), (
        "compute_ai_impact_metrics_daily must not be imported into "
        "job_daily.py's module namespace at all"
    )

    for skip_families in (None, {"ai_impact"}):
        sink.write_calls = []
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "write_ai_impact_metrics" not in sink.write_calls
        # cicd (unrelated family, same partition) must be entirely
        # unaffected by the deletion.
        assert "write_cicd_metrics" in sink.write_calls


@pytest.mark.asyncio
async def test_work_graph_edges_compute_and_write_are_deleted_from_job_daily(
    monkeypatch: Any,
) -> None:
    """CHAOS-5234/CHAOS-3092 close condition 3 (closes CHAOS-5216 too).

    work_graph_edges's daily compute is DELETED from job_daily.py, not
    skip-gated -- same rule as CHAOS-5233's work_item_attribution. Same
    shape as ai_impact: extract_review_deployment_incident_edges itself is
    ALSO deleted (from work_graph/extractors/ai_workflow.py) -- rg confirmed
    its only real callers, once job_daily.py's own reference was removed,
    were its Go bit-exact oracle rot guard
    (TestWorkGraphEdgesMatchLivePythonProduction +
    testdata/python_work_graph_edges_oracle.py, both also deleted in this
    PR) and its own dedicated test (trimmed, not deleted --
    tests/work_graph/test_ai_workflow.py's traversal tests survive).
    WorkGraphEdgesExecutor (native Go) is now the only writer of
    work_graph_pr_review_outcome_edges/work_graph_pr_deployment_edges/
    work_graph_deployment_incident_edges, closing CHAOS-5216 by construction
    (single native reader).

    Merge note (CHAOS-5242, #2307 landed first): that PR deleted this same
    function's OTHER half (ai_workflow's runs/artifact_edges/issue_edges,
    via extract_ai_workflow_from_pull_requests). With both halves gone in
    the merge, _extract_ai_workflow_for_day itself, its module
    (work_graph/extractors/ai_workflow.py), and the now-fully-obsolete
    tests/metrics/test_job_daily_ai_workflow.py are all deleted too -- rg
    confirmed zero remaining callers of any of them.
    """
    sink = _RecordingSink("clickhouse://test")
    _neutralize_daily_job(monkeypatch, sink=sink, loader=_FakeLoader())

    assert not hasattr(job_daily, "extract_review_deployment_incident_edges"), (
        "extract_review_deployment_incident_edges must not be imported into "
        "job_daily.py's module namespace at all"
    )
    assert not hasattr(job_daily, "_extract_ai_workflow_for_day"), (
        "_extract_ai_workflow_for_day must not exist in job_daily.py at all "
        "-- both of its halves (ai_workflow, work_graph_edges) are deleted"
    )

    for skip_families in (None, {"work_graph_edges"}):
        sink.write_calls = []
        await job_daily.run_daily_metrics_job(
            db_url="clickhouse://test",
            day=DAY,
            backfill_days=1,
            provider="auto",
            org_id=ORG_ID,
            skip_families=skip_families,
        )
        assert "write_work_graph_pr_review_outcome_edges" not in sink.write_calls
        assert "write_work_graph_pr_deployment_edges" not in sink.write_calls
        assert "write_work_graph_deployment_incident_edges" not in sink.write_calls
        # cicd (unrelated family, same partition) must be entirely
        # unaffected by the deletion.
        assert "write_cicd_metrics" in sink.write_calls
