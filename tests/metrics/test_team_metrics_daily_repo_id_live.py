"""Live-ClickHouse readback proof for CHAOS-4329.

``team_metrics_daily`` had no ``repo_id`` -- the writer ran once per repo
(``api/internal/worker_metrics.py``'s ``for repo_id in repo_ids`` loop,
CHAOS-4264) and inserted one row per (team, day) PER REPO into the SAME
``(org_id, team_id, day)`` key. Every reader collapsed that key via
``argMax(<col>, computed_at)``, so a team owning N repos kept only the
LAST-WRITTEN repo's slice -- the other N-1 repos' commits were silently
invisible, not summed, not averaged in, just gone.

This test reproduces the real per-repo write pattern end to end against a
real ClickHouse (compute -> write -> read), for a team ("core") that owns
two repos, each contributing a DIFFERENT commit mix so an aggregated-away
result is numerically distinguishable from a correctly-summed one. It proves
both halves:

1. WRITE: ``compute_team_wellbeing_metrics_daily`` called once per repo (the
   real production call pattern) writes one row PER (team, repo, day), never
   colliding on write.
2. READ: every one of the four readers CHAOS-4329 names
   (``cognitive_load.py``, ``native_team_workload.py``,
   ``metrics/scoring/wellbeing.py``, ``recommendations/loader.py``) returns
   the SUM of both repos' additive counts / the ratio recomputed from that
   sum -- never a single repo's slice.

Manually verified against origin/main (pre-fix) with the exact same
per-repo call pattern (`` for repo_id in [...]: run_daily_metrics_job(...)``
against real fixtures data): the readers returned only the LAST repo's
commits_count and ratio -- a ~90% undercount for the multi-repo team. This
test is the checked-in, always-run version of that manual proof; it also
cannot even construct on origin/main (``TeamMetricsDailyRecord`` has no
``repo_id`` field there), so it fails to collect/red before this change,
not merely fails an assertion.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from datetime import date, datetime, timezone
from typing import Any, cast

import pytest

from dev_health_ops.metrics.compute_wellbeing import (
    compute_team_wellbeing_metrics_daily,
)
from dev_health_ops.metrics.schemas import CommitStatRow
from dev_health_ops.providers.teams import build_repo_pattern_resolver

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]

DAY = date(2026, 8, 24)  # Monday
COMPUTED_AT_R1 = datetime(2026, 8, 25, 1, 0, tzinfo=timezone.utc)
COMPUTED_AT_R2 = datetime(2026, 8, 25, 1, 5, tzinfo=timezone.utc)


def _sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_tables()
    return sink


def _row(
    *,
    repo_id: uuid.UUID,
    commit_hash: str,
    author_email: str,
    committer_when: datetime,
) -> dict[str, Any]:
    return {
        "repo_id": repo_id,
        "commit_hash": commit_hash,
        "author_email": author_email,
        "author_name": None,
        "committer_when": committer_when,
        "file_path": "irrelevant.py",
        "additions": 1,
        "deletions": 0,
    }


def test_team_spanning_two_repos_sums_correctly_through_every_reader() -> None:
    from dev_health_ops.api.dev.native_team_workload import (
        ClickHouseTeamWorkloadSource,
    )
    from dev_health_ops.api.graphql.resolvers.cognitive_load import (
        _fetch_team_metrics,
    )
    from dev_health_ops.metrics.scoring.wellbeing import WellbeingScorer
    from dev_health_ops.recommendations.loader import ClickHouseMetricsLoader

    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()

    repo_team_resolver = build_repo_pattern_resolver(
        [
            {
                "id": "core",
                "name": "Core",
                "members": [],
                "repo_patterns": ["chaos4329/repo-a", "chaos4329/repo-b"],
            }
        ]
    )
    repo_names_by_id = {repo_a: "chaos4329/repo-a", repo_b: "chaos4329/repo-b"}

    # Repo A: 4 commits, 1 after-hours (03:00 UTC, before business hours).
    repo_a_rows = [
        _row(
            repo_id=repo_a,
            commit_hash=f"a{i}",
            author_email="dev-a@example.com",
            committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
        )
        for i in range(3)
    ] + [
        _row(
            repo_id=repo_a,
            commit_hash="a-after-hours",
            author_email="dev-a@example.com",
            committer_when=datetime(2026, 8, 24, 3, 0, tzinfo=timezone.utc),
        )
    ]
    # Repo B: 2 commits, 0 after-hours -- deliberately a DIFFERENT mix so a
    # bug that silently drops repo B (or repo A) is numerically visible,
    # not just a coincidental match.
    repo_b_rows = [
        _row(
            repo_id=repo_b,
            commit_hash=f"b{i}",
            author_email="dev-b@example.com",
            committer_when=datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
        )
        for i in range(2)
    ]

    # Mirrors api/internal/worker_metrics.py's `for repo_id in repo_ids`
    # loop (CHAOS-4264): one real call per repo, each with its OWN
    # computed_at, exactly like job_daily.py's run_daily_metrics_job does
    # per repo-scoped call.
    records_a = compute_team_wellbeing_metrics_daily(
        day=DAY,
        commit_stat_rows=cast(Sequence[CommitStatRow], repo_a_rows),
        team_resolver=None,
        computed_at=COMPUTED_AT_R1,
        repo_team_resolver=repo_team_resolver,
        repo_names_by_id=repo_names_by_id,
    )
    records_b = compute_team_wellbeing_metrics_daily(
        day=DAY,
        commit_stat_rows=cast(Sequence[CommitStatRow], repo_b_rows),
        team_resolver=None,
        computed_at=COMPUTED_AT_R2,
        repo_team_resolver=repo_team_resolver,
        repo_names_by_id=repo_names_by_id,
    )
    assert len(records_a) == 1 and records_a[0].repo_id == str(repo_a)
    assert len(records_b) == 1 and records_b[0].repo_id == str(repo_b)
    assert records_a[0].commits_count == 4
    assert records_b[0].commits_count == 2

    sink.write_team_metrics(records_a)
    sink.write_team_metrics(records_b)

    # WRITE proof: both repos physically landed as separate rows -- never
    # collided on write even though they share (org_id, team_id, day).
    raw_rows = sink.client.query(
        "SELECT repo_id, commits_count, after_hours_commits_count "
        "FROM team_metrics_daily "
        "WHERE org_id = {org_id:String} AND team_id = 'core' AND day = {day:Date}",
        parameters={"org_id": org_id, "day": DAY.isoformat()},
    ).result_rows
    assert len(raw_rows) == 2, (
        f"expected one physical row per repo, got {raw_rows} -- a writer "
        "regression collided the two repos back onto one key"
    )

    # True totals a correct reader must reach: 6 commits, 1 after-hours.
    true_commits = 4 + 2
    true_after_hours = 1 + 0
    true_ratio = true_after_hours / true_commits

    # READ proof 1: cognitive_load.py's GraphQL resolver.
    import asyncio

    team_rows = asyncio.run(
        _fetch_team_metrics(
            sink.client,
            org_id=org_id,
            since_date=DAY.isoformat(),
            until_date=DAY.isoformat(),
            team_id="core",
        )
    )
    assert len(team_rows) == 1
    assert team_rows[0]["after_hours_commit_ratio"] == pytest.approx(true_ratio), (
        f"cognitive_load.py._fetch_team_metrics returned "
        f"{team_rows[0]['after_hours_commit_ratio']!r}, expected the "
        f"two-repo weighted ratio {true_ratio!r} -- a single-repo slice "
        "would read 0.25 (repo A alone) or 0.0 (repo B alone)"
    )

    # READ proof 2: native_team_workload.py (CHAOS-3304 dev-scope resolver).
    workload_source = ClickHouseTeamWorkloadSource(sink.client)
    workload_result = asyncio.run(
        workload_source.cognitive_load(
            org_id=org_id,
            team_id="core",
            start=datetime(2026, 8, 24, tzinfo=timezone.utc),
            end=datetime(2026, 8, 25, tzinfo=timezone.utc),
        )
    )
    assert workload_result.measured
    assert workload_result.after_hours_commit_ratio == pytest.approx(true_ratio)

    # READ proof 3: metrics/scoring/wellbeing.py (health-score dimension).
    scorer = WellbeingScorer()
    signals = scorer._fetch_signals(sink.client, org_id, DAY, "core")
    assert signals["after_hours_ratio_inverse"] == pytest.approx(1.0 - true_ratio)

    # READ proof 4: recommendations/loader.py (rule-engine metrics loader).
    # org_id is required here -- ClickHouseMetricsLoader._oc() only adds an
    # org_id filter when the loader was constructed with one; without it,
    # this reader's window function has no per-org boundary at all (its
    # own PARTITION BY day relies on team_id + org_id being pinned by the
    # WHERE clause first) and would read every org sharing team_id="core"
    # in the shared test ClickHouse instance, not just this test's own
    # throwaway org.
    loader = ClickHouseMetricsLoader(sink.client, org_id=org_id)
    after_hours, _cycle_times = loader._load_sustainability_signals(
        "core", DAY, date(2026, 8, 25)
    )
    assert after_hours == pytest.approx(true_ratio)


def test_legacy_row_is_not_double_counted_after_a_real_per_repo_backfill() -> None:
    """codex CHAOS-4329 round 1 (P1): a legacy repo_id='' row (written
    before migration 080) is its own dedup bucket, distinct from a real
    repo_id bucket. If a historical day later gets a real per-repo
    backfill/re-drive (append-only tables re-drive an existing day rather
    than rewrite it -- CHAOS-4246 made this a designed, expected
    occurrence), a naive per-(team, repo, day) SUM would add the legacy
    aggregate on top of the new per-repo rows and double-count that day.
    Every reader must drop the legacy bucket once real per-repo data exists
    for the same (team_id, day) key.
    """
    import asyncio

    from dev_health_ops.api.graphql.resolvers.cognitive_load import (
        _fetch_team_metrics,
    )
    from dev_health_ops.metrics.schemas import TeamMetricsDailyRecord

    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id

    # A legacy row (repo_id='', the migration 080 default) for team "core"
    # on DAY, as if written before that migration -- 10 commits, 3
    # after-hours.
    legacy_record = TeamMetricsDailyRecord(
        day=DAY,
        team_id="core",
        team_name="Core",
        commits_count=10,
        after_hours_commits_count=3,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.3,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 8, 24, 1, 0, tzinfo=timezone.utc),
        repo_id="",
    )
    sink.write_team_metrics([legacy_record])
    # A real per-repo backfill for the SAME day, repo_id populated -- 4
    # commits, 1 after-hours. Deliberately a DIFFERENT total than the
    # legacy row so double-counting is numerically visible.
    backfill_record = TeamMetricsDailyRecord(
        day=DAY,
        team_id="core",
        team_name="Core",
        commits_count=4,
        after_hours_commits_count=1,
        weekend_commits_count=0,
        after_hours_commit_ratio=0.25,
        weekend_commit_ratio=0.0,
        computed_at=datetime(2026, 8, 25, 1, 0, tzinfo=timezone.utc),
        repo_id=str(uuid.uuid4()),
    )
    sink.write_team_metrics([backfill_record])

    # True answer: the real per-repo backfill superseded the legacy row for
    # this day -- 4 commits, 1 after-hours (ratio 0.25). NOT 14/4 (0.2857),
    # which is what a naive sum-across-all-buckets would produce.
    team_rows = asyncio.run(
        _fetch_team_metrics(
            sink.client,
            org_id=org_id,
            since_date=DAY.isoformat(),
            until_date=DAY.isoformat(),
            team_id="core",
        )
    )
    assert len(team_rows) == 1
    assert team_rows[0]["after_hours_commit_ratio"] == pytest.approx(0.25), (
        f"cognitive_load.py._fetch_team_metrics returned "
        f"{team_rows[0]['after_hours_commit_ratio']!r}, expected 0.25 (the "
        "real per-repo backfill alone) -- 0.2857 would mean the legacy "
        "repo_id='' row was double-counted alongside the new per-repo row"
    )
