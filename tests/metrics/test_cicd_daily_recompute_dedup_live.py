"""Live-ClickHouse readback proof for CHAOS-4246's recompute fix.

Root cause (measured in prod): a `metrics.daily_partition` for day D could be
triggered by a git/work-item sync BEFORE that day's CI data had synced, write
zero `cicd_metrics_daily` rows, report `succeeded`, and never get
re-triggered -- `native_post_sync.go`'s `Daily` condition used to be
`git || hasWorkItems` only, blind to a later cicd/deployments/incidents-only
sync. The fix (`dailyMetricsTrigger`, unit-tested in
`internal/syncdispatchruntime/native_post_sync_daily_trigger_test.go`) also
re-triggers on those three targets.

That fix is only safe because a re-drive is dedup-safe end to end: this test
proves the OTHER half -- the write/read side -- against a real ClickHouse.
Two `run_daily_metrics_job`-equivalent generations are written for the SAME
(org, repo, day): one computed from a partial CI window (as a git-triggered
run would see before that day's CI synced), one computed after the rest of
that day's CI data landed (as the CHAOS-4246-fixed post-sync trigger would
recompute). `cicd_metrics_daily` is plain (append-only) MergeTree, so both
generations land as separate physical rows -- readback through the dedup
path (`clickhouse_dedup._APPEND_ONLY_DAILY_KEYS`, registered for this table
in the same change) must return exactly the LATEST generation, not a sum or
duplicate of both.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest

from dev_health_ops.metrics.schemas import PipelineRunRow

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]


def _sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_tables()
    return sink


def test_cicd_recompute_after_late_ci_sync_dedups_to_latest_generation() -> None:
    from dev_health_ops.clickhouse_dedup import dedup_from
    from dev_health_ops.metrics.compute_cicd import compute_cicd_metrics_daily

    sink = _sink()
    sink.org_id = str(
        uuid.uuid4()
    )  # throwaway random org (isolated, no cleanup needed)
    repo_id = uuid.uuid4()
    day = date(2026, 6, 15)
    computed_at_early = datetime(2026, 6, 15, 1, 5, tzinfo=timezone.utc)
    computed_at_late = datetime(2026, 6, 15, 20, 0, tzinfo=timezone.utc)

    def pipeline_row(started_at: datetime, status: str) -> PipelineRunRow:
        return {
            "repo_id": repo_id,
            "run_id": str(uuid.uuid4()),
            "status": status,
            "queued_at": None,
            "started_at": started_at,
            "finished_at": started_at + timedelta(minutes=5),
        }

    # Generation 1: a git-triggered run at 01:05 UTC (mirrors the fixed
    # daily_metrics_fanout schedule) sees only the CI runs that landed before
    # that day's sync had caught up -- 2 of the day's eventual 5.
    early_rows = [
        pipeline_row(datetime(2026, 6, 15, 0, 10, tzinfo=timezone.utc), "success"),
        pipeline_row(datetime(2026, 6, 15, 0, 40, tzinfo=timezone.utc), "success"),
    ]
    gen1 = compute_cicd_metrics_daily(
        day=day, pipeline_runs=early_rows, computed_at=computed_at_early
    )
    assert len(gen1) == 1
    assert gen1[0].pipelines_count == 2
    sink.write_cicd_metrics(gen1)  # org_id auto-injected from sink.org_id

    # Generation 2: the rest of the day's CI runs land later. Before
    # CHAOS-4246 nothing would re-trigger metrics.daily_partition for this
    # day again -- this call stands in for the fixed post-sync trigger firing
    # on the cicd-only sync that wrote the remaining 3 runs.
    late_rows = early_rows + [
        pipeline_row(datetime(2026, 6, 15, 10, 0, tzinfo=timezone.utc), "success"),
        pipeline_row(datetime(2026, 6, 15, 14, 0, tzinfo=timezone.utc), "failure"),
        pipeline_row(datetime(2026, 6, 15, 18, 0, tzinfo=timezone.utc), "success"),
    ]
    gen2 = compute_cicd_metrics_daily(
        day=day, pipeline_runs=late_rows, computed_at=computed_at_late
    )
    assert len(gen2) == 1
    assert gen2[0].pipelines_count == 5
    sink.write_cicd_metrics(gen2)

    # Both generations are now physically in the append-only table.
    raw_count = sink.client.query(
        "SELECT count() FROM cicd_metrics_daily WHERE org_id = {org_id:String} AND day = {day:Date}",
        parameters={"org_id": sink.org_id, "day": day.isoformat()},
    ).result_rows[0][0]
    assert raw_count == 2, "expected both generations to have landed as separate rows"

    # Readback proof: the dedup path (what every real reader uses per
    # clickhouse_dedup._APPEND_ONLY_DAILY_KEYS) collapses to the LATEST
    # generation -- 5 pipelines, not 2, and not 7 (a sum of both).
    from_clause = dedup_from("cicd_metrics_daily")
    row = sink.client.query(
        f"""
        SELECT pipelines_count, success_rate
        FROM {from_clause}
        WHERE org_id = {{org_id:String}} AND day = {{day:Date}}
        """,
        parameters={"org_id": sink.org_id, "day": day.isoformat()},
    ).result_rows
    assert len(row) == 1, "dedup must return exactly one row per (org, repo, day)"
    pipelines_count, success_rate = row[0]
    assert pipelines_count == 5, (
        f"expected the latest generation's pipelines_count (5), got {pipelines_count} "
        "-- a stale or summed read would double-count the re-drive"
    )
    assert success_rate == pytest.approx(4 / 5)
