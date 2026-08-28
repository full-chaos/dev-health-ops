"""Unit tests for team-keyed cognitive load aggregation (CHAOS-4365 item 2).

Covers: ownership-only resolution (the row's own ``team_id`` field is never
read -- CHAOS-4396), correct SUM across multiple repos owned by the same
team, ratio recomputed from summed counts (never averaged), None vs 0.0
distinction for the ratio fields, and a repo with no ownership entry
contributing to no team.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone

from dev_health_ops.metrics.team_cognitive_load import (
    build_team_cognitive_load_rows_for_day,
)

DAY = date(2026, 8, 28)
NOW = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)


@dataclass
class _UserRow:
    repo_id: uuid.UUID
    author_email: str
    pr_interruption_load: int = 0
    context_spread_count: int = 0
    review_request_load: int = 0
    # A membership-derived team_id CHAOS-4396 found user_metrics_daily can
    # carry -- deliberately present on every fixture row here to prove the
    # aggregator never reads it.
    team_id: str = "membership-tainted-team"


@dataclass
class _TeamWellbeingRow:
    repo_id: str
    commits_count: int = 0
    after_hours_commits_count: int = 0
    weekend_commits_count: int = 0
    team_id: str = "membership-tainted-team"


def test_aggregates_by_ownership_never_reading_the_rows_own_team_id() -> None:
    repo_id = uuid.uuid4()
    user_rows = [
        _UserRow(
            repo_id=repo_id,
            author_email="a@example.com",
            pr_interruption_load=3,
            context_spread_count=2,
            review_request_load=1,
        )
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=user_rows,
        team_wellbeing_rows=[],
        repo_to_team={str(repo_id): "gh:platform"},
        computed_at=NOW,
    )

    assert len(records) == 1
    row = records[0]
    # Resolved via the ownership map, NOT the row's own (tainted) team_id.
    assert row.team_id == "gh:platform"
    assert row.org_id == "acme"
    assert row.day == DAY
    assert row.pr_interruption_load == 3.0
    # context_spread_count is NOT the row's own raw value (2, the author's
    # TOTAL distinct-repo count across the whole org) -- it is the count of
    # distinct (author, repo) pairs contributing to THIS team, which here
    # is 1 (one author, one owned repo). See
    # test_context_spread_count_is_distinct_author_repo_pairs_not_a_sum_of_the_raw_value
    # for the case that actually distinguishes the two.
    assert row.context_spread_count == 1.0
    assert row.review_request_load == 1.0
    assert row.sample_author_count == 1
    assert row.contributing_repo_count == 1
    # No team_metrics_daily row at all -- unmeasured, not a measured zero.
    assert row.after_hours_commit_ratio is None
    assert row.weekend_commit_ratio is None


def test_sums_across_every_repo_the_team_owns_and_recomputes_the_ratio() -> None:
    """A team owning 2 repos: load counters SUM; the ratio is recomputed
    from the summed after_hours/weekend counts, never averaged per-repo.
    """
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    user_rows = [
        _UserRow(repo_id=repo_a, author_email="a@example.com", pr_interruption_load=2),
        _UserRow(repo_id=repo_b, author_email="b@example.com", pr_interruption_load=5),
        # Same author active in both owned repos -- distinct author count
        # must still be 1, not 2.
        _UserRow(repo_id=repo_b, author_email="a@example.com", pr_interruption_load=1),
    ]
    team_rows = [
        # repo_a: 10 commits, 2 after-hours, 1 weekend.
        _TeamWellbeingRow(
            repo_id=str(repo_a),
            commits_count=10,
            after_hours_commits_count=2,
            weekend_commits_count=1,
        ),
        # repo_b: 20 commits, 4 after-hours, 3 weekend.
        _TeamWellbeingRow(
            repo_id=str(repo_b),
            commits_count=20,
            after_hours_commits_count=4,
            weekend_commits_count=3,
        ),
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=user_rows,
        team_wellbeing_rows=team_rows,
        repo_to_team={str(repo_a): "gh:platform", str(repo_b): "gh:platform"},
        computed_at=NOW,
    )

    assert len(records) == 1
    row = records[0]
    assert row.pr_interruption_load == 8.0  # 2 + 5 + 1
    assert row.sample_author_count == 2  # {a, b}
    assert row.contributing_repo_count == 2
    # Ratio recomputed from SUMMED counts: (2+4)/(10+20) = 0.2, never
    # averaging the two repos' own 0.2 and 0.2 ratios directly (which would
    # coincidentally match here -- see the next test for a case that
    # distinguishes the two methods).
    assert row.after_hours_commit_ratio == 0.2
    assert row.weekend_commit_ratio == (1 + 3) / (10 + 20)


def test_ratio_is_recomputed_from_summed_counts_not_averaged_per_repo() -> None:
    """Distinguishes SUM-then-divide from averaging two repos' own ratios:
    a naive average of per-repo ratios here would give a different (wrong)
    number than the correct summed-counts recomputation.
    """
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    team_rows = [
        # repo_a: 1 commit, 1 after-hours -> ratio 1.0 alone.
        _TeamWellbeingRow(
            repo_id=str(repo_a), commits_count=1, after_hours_commits_count=1
        ),
        # repo_b: 99 commits, 0 after-hours -> ratio 0.0 alone.
        _TeamWellbeingRow(
            repo_id=str(repo_b), commits_count=99, after_hours_commits_count=0
        ),
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=[],
        team_wellbeing_rows=team_rows,
        repo_to_team={str(repo_a): "gh:platform", str(repo_b): "gh:platform"},
        computed_at=NOW,
    )

    row = records[0]
    # Correct: (1 + 0) / (1 + 99) = 0.01. A naive average of 1.0 and 0.0
    # would give 0.5 -- very different, and wrong (repo_b's 99 commits
    # should dominate the team's true ratio).
    assert row.after_hours_commit_ratio == 0.01
    assert row.after_hours_commit_ratio != 0.5


def test_a_repo_with_no_ownership_entry_contributes_to_no_team() -> None:
    """A repo not present in repo_to_team (never resolved to any team --
    ownership genuinely doesn't cover it) must not silently land under any
    team, and must not crash.
    """
    unowned_repo = uuid.uuid4()
    user_rows = [
        _UserRow(repo_id=unowned_repo, author_email="a@example.com"),
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=user_rows,
        team_wellbeing_rows=[],
        repo_to_team={},  # no ownership resolves this repo
        computed_at=NOW,
    )

    assert records == []


def test_measured_zero_ratio_is_distinguished_from_unmeasured() -> None:
    """A team_metrics_daily row exists (measured=True) but every owned repo
    genuinely had zero commits this day -- 0.0, not None.
    """
    repo_id = uuid.uuid4()
    team_rows = [
        _TeamWellbeingRow(
            repo_id=str(repo_id),
            commits_count=0,
            after_hours_commits_count=0,
            weekend_commits_count=0,
        ),
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=[],
        team_wellbeing_rows=team_rows,
        repo_to_team={str(repo_id): "gh:platform"},
        computed_at=NOW,
    )

    row = records[0]
    assert row.after_hours_commit_ratio == 0.0
    assert row.weekend_commit_ratio == 0.0
    assert row.after_hours_commit_ratio is not None


def test_legacy_empty_repo_id_sentinel_on_team_wellbeing_rows_is_skipped() -> None:
    """A team_metrics_daily row with the migration-080 legacy repo_id=''
    sentinel is never a real, ownership-resolvable repo -- it must be
    skipped, not accidentally matched against an empty-string ownership key.
    """
    team_rows = [
        _TeamWellbeingRow(repo_id="", commits_count=5, after_hours_commits_count=5),
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=[],
        team_wellbeing_rows=team_rows,
        repo_to_team={"": "gh:should-never-match"},
        computed_at=NOW,
    )

    assert records == []


def test_context_spread_count_is_distinct_author_repo_pairs_not_a_sum_of_the_raw_value() -> (
    None
):
    """CHAOS-4365 codex R3 (P2): UserMetricsDailyRecord.context_spread_count
    is already the AUTHOR's total distinct-repo count for the day, copied
    identically onto every one of that author's per-repo rows -- it is not
    a per-repo-additive value. One author touching 2 repos owned by the
    same team, each row carrying context_spread_count=2 (their org-wide
    total), must report the team's true count as 2 (the number of distinct
    (author, repo) pairs), never 2+2=4.
    """
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    user_rows = [
        _UserRow(repo_id=repo_a, author_email="a@example.com", context_spread_count=2),
        _UserRow(repo_id=repo_b, author_email="a@example.com", context_spread_count=2),
    ]

    records = build_team_cognitive_load_rows_for_day(
        day=DAY,
        org_id="acme",
        user_metrics_rows=user_rows,
        team_wellbeing_rows=[],
        repo_to_team={str(repo_a): "gh:platform", str(repo_b): "gh:platform"},
        computed_at=NOW,
    )

    assert len(records) == 1
    row = records[0]
    assert row.context_spread_count == 2.0
    assert row.context_spread_count != 4.0
