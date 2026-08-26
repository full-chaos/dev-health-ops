"""Live-ClickHouse readback proof for CHAOS-4332.

``team_metrics_daily.computed_at`` was ``DateTime('UTC')`` -- whole-second
precision. A re-drive of the SAME (org_id, team_id, repo_id, day) key twice
within one wall-clock second (an ordinary post-sync recompute shape, not a
rare edge case) stored the IDENTICAL ``computed_at`` for both rows, so every
reader's ``argMax(<col>, computed_at)`` tie-break over two physically-equal
timestamps became implementation-defined. Migration 080 promotes the column
to ``DateTime64(6, 'UTC')`` (folded in alongside CHAOS-4329's ``repo_id``,
same table).

Both writers already stamp real microsecond wall-clock time
(``datetime.now(timezone.utc)`` in ``job_daily.py``) -- this test proves the
*storage* side end to end through the real ``write_team_metrics`` sink call
(not raw SQL): two writes 800 microseconds apart, both within the same
second, land as two DISTINCT stored rows and ``argMax`` resolves to the
TRUE later write. The Go-side equivalent proof (including the pre-fix tie)
is
``internal/jobs/metrics/daily/team_metrics_daily_computed_at_precision_integration_test.go``.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.metrics.schemas import TeamMetricsDailyRecord

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]

DAY = date(2026, 8, 24)
EARLIER = datetime(2026, 8, 24, 12, 0, 0, 100_000, tzinfo=timezone.utc)
LATER = datetime(2026, 8, 24, 12, 0, 0, 900_000, tzinfo=timezone.utc)  # same second


def _sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_tables()
    return sink


def _record(
    *, commits_count: int, after_hours: int, computed_at: datetime
) -> TeamMetricsDailyRecord:
    return TeamMetricsDailyRecord(
        day=DAY,
        team_id="core",
        team_name="Core",
        commits_count=commits_count,
        after_hours_commits_count=after_hours,
        weekend_commits_count=0,
        after_hours_commit_ratio=after_hours / commits_count,
        weekend_commit_ratio=0.0,
        computed_at=computed_at,
        repo_id="repo-a",
    )


def test_two_writes_in_the_same_second_stay_distinct_and_later_wins() -> None:
    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id

    # Generation 1: an earlier compute, 5 commits.
    sink.write_team_metrics(
        [_record(commits_count=5, after_hours=1, computed_at=EARLIER)]
    )
    # Generation 2: a re-drive 800 microseconds later -- still the SAME
    # wall-clock second -- 9 commits, the TRUE latest generation.
    sink.write_team_metrics(
        [_record(commits_count=9, after_hours=2, computed_at=LATER)]
    )

    row = sink.client.query(
        "SELECT count(DISTINCT computed_at) AS distinct_ts, "
        "argMax(commits_count, computed_at) AS latest_commits "
        "FROM team_metrics_daily WHERE org_id = {org_id:String} AND team_id = 'core'",
        parameters={"org_id": org_id},
    ).result_rows[0]
    distinct_timestamps, latest_commits = row

    assert distinct_timestamps == 2, (
        f"expected the two writes 800us apart (same wall-clock second) to "
        f"stay distinct in storage, got {distinct_timestamps} distinct "
        "computed_at value(s) -- a collapse here means the column lost "
        "sub-second precision (the CHAOS-4332 defect)"
    )
    assert latest_commits == 9, (
        f"argMax(commits_count, computed_at) returned {latest_commits}, "
        "expected 9 (the TRUE later write) -- a tied computed_at makes "
        "this an implementation-defined guess instead of a deterministic "
        "answer"
    )
