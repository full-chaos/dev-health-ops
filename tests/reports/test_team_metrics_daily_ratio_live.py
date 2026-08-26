"""Live-ClickHouse "no regression" proof for CHAOS-4329's report-engine
finding (codex round 2).

Before CHAOS-4329, ``team_metrics_daily`` had no ``repo_id``, so its
dedup source (``clickhouse_dedup.dedup_from``) yielded exactly one row per
``(team, day)`` and a plain ``avg(after_hours_commit_ratio)`` was trivially
correct (an average over one value). After CHAOS-4329, a team owning N
repos yields N rows per ``(team, day)`` -- an unweighted ``avg()`` over
those rows is WRONG the moment repos have different sizes/ratios.

This test writes two repos for one team with deliberately different
commit mixes (repo-a: 8 commits/2 after-hours = 0.25 ratio; repo-b: 2
commits/2 after-hours = 1.0 ratio) so an unweighted average (0.625) is
numerically distinguishable from the TRUE summed-counts ratio (4/10 =
0.4), then executes the real chart query through ``execute_chart`` end to
end. The Go engine's equivalent proof is
``internal/jobs/report/team_metrics_daily_ratio_integration_test.go``.
"""

from __future__ import annotations

import os
import uuid

import pytest

from dev_health_ops.metrics.schemas import TeamMetricsDailyRecord
from dev_health_ops.metrics.testops_schemas import ChartSpec
from dev_health_ops.reports.charts import execute_chart

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


@pytest.mark.asyncio
async def test_execute_chart_recomputes_team_metrics_daily_ratio_across_repos() -> None:
    from datetime import date, datetime, timezone

    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id
    day = date(2026, 1, 1)

    repo_a = TeamMetricsDailyRecord(
        day=day,
        team_id="core",
        team_name="Core",
        commits_count=8,
        after_hours_commits_count=2,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.25,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        repo_id="repo-a",
    )
    repo_b = TeamMetricsDailyRecord(
        day=day,
        team_id="core",
        team_name="Core",
        commits_count=2,
        after_hours_commits_count=2,
        weekend_commits_count=0,
        after_hours_commit_ratio=1.0,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 2, 0, 0, 1, tzinfo=timezone.utc),
        repo_id="repo-b",
    )
    sink.write_team_metrics([repo_a])
    sink.write_team_metrics([repo_b])

    spec = ChartSpec(
        chart_id="chart-1",
        plan_id="plan-1",
        chart_type="line",
        metric="after_hours_commit_ratio",
        group_by="day",
        filter_teams=["core"],
        time_range_start=day,
        time_range_end=day,
        title=None,
        org_id=org_id,
    )
    result = await execute_chart(spec, sink.client)

    assert len(result.data_points) == 1
    got = result.data_points[0]["y"]
    assert got == pytest.approx(0.4), (
        f"after_hours_commit_ratio y={got!r}, want ~0.4 (sum(after_hours)/"
        "sum(commits) = 4/10) -- 0.625 would mean an unweighted avg() over "
        "the two repos' ratios (the regression this test guards)"
    )


@pytest.mark.asyncio
async def test_execute_chart_org_wide_averages_teams_not_repos_or_org() -> None:
    """codex CHAOS-4329 round 2 (P1): an org-wide chart (no team filter)
    must keep averaging EACH TEAM's own ratio equally -- summing
    numerator/denominator across TEAMS (not just repos) would silently
    change this chart's existing equal-weighted semantics into a
    commit-weighted ratio across the whole org. team-a (1 commit, 1
    after-hours = ratio 1.0) and team-b (99 commits, 0 after-hours = ratio
    0.0): the correct equal-weighted average is 0.5; a commit-weighted sum
    would read 0.01 -- the regression this test guards.
    """
    from datetime import date, datetime, timezone

    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id
    day = date(2026, 1, 1)

    team_a = TeamMetricsDailyRecord(
        day=day,
        team_id="team-a",
        team_name="A",
        commits_count=1,
        after_hours_commits_count=1,
        weekend_commits_count=0,
        after_hours_commit_ratio=1.0,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        repo_id="repo-a",
    )
    team_b = TeamMetricsDailyRecord(
        day=day,
        team_id="team-b",
        team_name="B",
        commits_count=99,
        after_hours_commits_count=0,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.0,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        repo_id="repo-b",
    )
    sink.write_team_metrics([team_a])
    sink.write_team_metrics([team_b])

    spec = ChartSpec(
        chart_id="chart-org-wide",
        plan_id="plan-1",
        chart_type="line",
        metric="after_hours_commit_ratio",
        group_by="day",
        filter_teams=[],  # org-wide: no team filter
        time_range_start=day,
        time_range_end=day,
        title=None,
        org_id=org_id,
    )
    result = await execute_chart(spec, sink.client)

    assert len(result.data_points) == 1
    got = result.data_points[0]["y"]
    assert got == pytest.approx(0.5), (
        f"after_hours_commit_ratio y={got!r}, want ~0.5 (equal-weighted "
        "average of team-a's 1.0 and team-b's 0.0) -- 0.01 would mean "
        "numerator/denominator were summed across TEAMS instead of just "
        "repos within each team (the regression this test guards)"
    )


@pytest.mark.asyncio
async def test_execute_chart_drops_legacy_bucket_once_a_real_per_repo_backfill_exists() -> (
    None
):
    """codex CHAOS-4329 round 3 (P1): a historical day that has BOTH the
    migration's legacy repo_id='' aggregate AND a later real per-repo
    backfill for the same (team, day) must not sum the two together --
    that would double-count the day. Legacy row: 4 commits/2 after-hours
    (ratio 0.5). Real backfill: repo-a 8/2 (0.25) -- the only repo, so the
    correct team-day ratio is 0.25, not the double-counted
    (4+8)/(2+2)=0.333 an unfiltered SUM would produce.
    """
    from datetime import date, datetime, timezone

    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id
    day = date(2026, 1, 1)

    legacy_row = TeamMetricsDailyRecord(
        day=day,
        team_id="core",
        team_name="Core",
        commits_count=4,
        after_hours_commits_count=2,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.5,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
        repo_id="",  # pre-migration-080 write
    )
    backfill_row = TeamMetricsDailyRecord(
        day=day,
        team_id="core",
        team_name="Core",
        commits_count=8,
        after_hours_commits_count=2,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.25,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        repo_id="repo-a",  # post-migration-080 real per-repo re-drive
    )
    sink.write_team_metrics([legacy_row])
    sink.write_team_metrics([backfill_row])

    spec = ChartSpec(
        chart_id="chart-legacy",
        plan_id="plan-1",
        chart_type="line",
        metric="after_hours_commit_ratio",
        group_by="day",
        filter_teams=["core"],
        time_range_start=day,
        time_range_end=day,
        title=None,
        org_id=org_id,
    )
    result = await execute_chart(spec, sink.client)

    assert len(result.data_points) == 1
    got = result.data_points[0]["y"]
    assert got == pytest.approx(0.25), (
        f"after_hours_commit_ratio y={got!r}, want ~0.25 (repo-a's real "
        "backfill alone) -- 0.333 would mean the legacy repo_id='' bucket "
        "was summed together with the real per-repo backfill "
        "(the regression this test guards)"
    )


@pytest.mark.asyncio
async def test_execute_chart_still_surfaces_a_legacy_only_day_with_no_backfill_yet() -> (
    None
):
    """Independent-review coverage gap (post codex round 3): the legacy-
    bucket-suppression filter (WHERE repo_id != '' OR real_repo_count = 0)
    has a second branch nothing else in this file exercises -- a
    (team, day) that has ONLY the legacy repo_id='' row, no real per-repo
    backfill yet. real_repo_count is 0 for that key, so the second
    disjunct keeps the legacy row. A future refactor that simplified the
    filter to just "WHERE repo_id != ''" would silently zero out every
    not-yet-backfilled historical day -- this test guards that branch.
    """
    from datetime import date, datetime, timezone

    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id
    day = date(2026, 1, 1)

    legacy_only_row = TeamMetricsDailyRecord(
        day=day,
        team_id="core",
        team_name="Core",
        commits_count=4,
        after_hours_commits_count=2,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.5,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
        repo_id="",  # pre-migration-080 write, never re-driven
    )
    sink.write_team_metrics([legacy_only_row])

    spec = ChartSpec(
        chart_id="chart-legacy-only",
        plan_id="plan-1",
        chart_type="line",
        metric="after_hours_commit_ratio",
        group_by="day",
        filter_teams=["core"],
        time_range_start=day,
        time_range_end=day,
        title=None,
        org_id=org_id,
    )
    result = await execute_chart(spec, sink.client)

    assert len(result.data_points) == 1
    got = result.data_points[0]["y"]
    assert got == pytest.approx(0.5), (
        f"after_hours_commit_ratio y={got!r}, want ~0.5 (the legacy row's "
        "own ratio) -- 0.0 or an empty result would mean the "
        "legacy-only-day branch of the suppression filter (real_repo_count "
        "= 0) was dropped instead of kept (the regression this test guards)"
    )
