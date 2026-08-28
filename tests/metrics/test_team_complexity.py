"""Unit tests for team-keyed cyclomatic-complexity aggregation (CHAOS-4365
item 3).

Covers: ownership-only resolution, correct SUM across multiple repos owned
by the same team, cyclomatic_per_kloc recomputed from summed totals (never
averaged per-repo), the loc_total=0 edge case, and a repo with no ownership
entry contributing to no team.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone

from dev_health_ops.metrics.team_complexity import build_team_complexity_rows_for_day

DAY = date(2026, 8, 28)
NOW = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)


@dataclass
class _RepoComplexityRow:
    repo_id: uuid.UUID
    loc_total: int = 0
    cyclomatic_total: int = 0
    high_complexity_functions: int = 0
    very_high_complexity_functions: int = 0


def test_aggregates_by_ownership_and_computes_ratio() -> None:
    repo_id = uuid.uuid4()
    rows = [
        _RepoComplexityRow(
            repo_id=repo_id,
            loc_total=2000,
            cyclomatic_total=100,
            high_complexity_functions=3,
            very_high_complexity_functions=1,
        )
    ]

    records = build_team_complexity_rows_for_day(
        day=DAY,
        org_id="acme",
        repo_complexity_rows=rows,
        repo_to_team={str(repo_id): "gh:platform"},
        computed_at=NOW,
    )

    assert len(records) == 1
    row = records[0]
    assert row.team_id == "gh:platform"
    assert row.org_id == "acme"
    assert row.day == DAY
    assert row.loc_total == 2000
    assert row.cyclomatic_total == 100
    # 100 / (2000 / 1000) = 50.0
    assert row.cyclomatic_per_kloc == 50.0
    assert row.high_complexity_functions == 3
    assert row.very_high_complexity_functions == 1
    assert row.contributing_repo_count == 1


def test_sums_across_every_repo_the_team_owns_and_recomputes_the_ratio() -> None:
    """A team owning 2 repos: absolute counts SUM; cyclomatic_per_kloc is
    recomputed from the summed totals, never averaged per-repo.
    """
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    rows = [
        # repo_a: 1000 loc, 50 cc -> 50.0 cc/kloc alone.
        _RepoComplexityRow(
            repo_id=repo_a,
            loc_total=1000,
            cyclomatic_total=50,
            high_complexity_functions=2,
            very_high_complexity_functions=0,
        ),
        # repo_b: 9000 loc, 90 cc -> 10.0 cc/kloc alone.
        _RepoComplexityRow(
            repo_id=repo_b,
            loc_total=9000,
            cyclomatic_total=90,
            high_complexity_functions=1,
            very_high_complexity_functions=1,
        ),
    ]

    records = build_team_complexity_rows_for_day(
        day=DAY,
        org_id="acme",
        repo_complexity_rows=rows,
        repo_to_team={str(repo_a): "gh:platform", str(repo_b): "gh:platform"},
        computed_at=NOW,
    )

    assert len(records) == 1
    row = records[0]
    assert row.loc_total == 10000
    assert row.cyclomatic_total == 140
    assert row.high_complexity_functions == 3
    assert row.very_high_complexity_functions == 1
    assert row.contributing_repo_count == 2
    # Loc-weighted: (50 + 90) / (10000 / 1000) = 14.0. A naive average of
    # the two repos' own ratios (50.0 and 10.0) would give 30.0 -- very
    # different, and wrong (repo_b's 9x larger codebase should dominate).
    assert row.cyclomatic_per_kloc == 14.0
    assert row.cyclomatic_per_kloc != 30.0


def test_loc_total_zero_yields_zero_ratio_not_a_division_error() -> None:
    repo_id = uuid.uuid4()
    rows = [
        _RepoComplexityRow(repo_id=repo_id, loc_total=0, cyclomatic_total=0),
    ]

    records = build_team_complexity_rows_for_day(
        day=DAY,
        org_id="acme",
        repo_complexity_rows=rows,
        repo_to_team={str(repo_id): "gh:platform"},
        computed_at=NOW,
    )

    row = records[0]
    assert row.cyclomatic_per_kloc == 0.0


def test_a_repo_with_no_ownership_entry_contributes_to_no_team() -> None:
    """A repo not present in repo_to_team (ownership genuinely doesn't
    cover it) must not silently land under any team, and must not crash.
    """
    unowned_repo = uuid.uuid4()
    rows = [
        _RepoComplexityRow(repo_id=unowned_repo, loc_total=1000, cyclomatic_total=10),
    ]

    records = build_team_complexity_rows_for_day(
        day=DAY,
        org_id="acme",
        repo_complexity_rows=rows,
        repo_to_team={},  # no ownership resolves this repo
        computed_at=NOW,
    )

    assert records == []
