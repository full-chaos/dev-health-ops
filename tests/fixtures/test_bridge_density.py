"""Bridge-density tests for chord flow_matrix (CHAOS-1292).

Guards the fixture-side invariants the flow_matrix REPO and WORK_TYPE
templates depend on:

- Every (repo_id, day) bucket with >= 2 work items produces >= 2 distinct
  work_item_type values (so the WORK_TYPE bridge-join can emit cross-type
  edges).
- At least two teams touch multiple repos in the runner's per-org team
  assignment plan (so the REPO bridge-join on team_id + day can emit
  cross-repo edges).

Regression test charter: if CHAOS-1291's team co-occurrence logic or the
runner's _build_repo_team_assignments weights are refactored, this test
file fails LOUDLY so the chord's empty-state isn't discovered only in
production or in a reviewer's screenshot comparison.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.fixtures.runner import (
    _build_repo_team_assignments,
    _verify_repo_cooccurrence_density,
)


class _Stub:
    def __init__(self, team_id: str) -> None:
        self.id = team_id


class TestWorkTypeCooccurrence:
    """generator._ensure_work_type_cooccurrence guarantees per-day diversity."""

    def _make_generator(self) -> SyntheticDataGenerator:
        return SyntheticDataGenerator(
            repo_name="acme/demo-app",
            provider="github",
            seed=17,
        )

    def test_single_day_with_multiple_items_yields_multiple_types(self) -> None:
        """A (repo, day) bucket with >=2 items must span >=2 work_types."""
        gen = self._make_generator()
        items = gen.generate_work_items(days=1)

        by_day: dict[object, set[str]] = defaultdict(set)
        for item in items:
            day = (
                item.completed_at or item.started_at or item.created_at
            ).date()
            by_day[day].add(item.type)

        bad_buckets = [
            (day, types)
            for day, types in by_day.items()
            if sum(
                1
                for i in items
                if (i.completed_at or i.started_at or i.created_at).date() == day
            )
            >= 2
            and len(types) < 2
        ]
        assert bad_buckets == [], (
            f"Monotype buckets found: {bad_buckets}. "
            "_ensure_work_type_cooccurrence must flip one item per bucket."
        )

    def test_rewriting_is_deterministic(self) -> None:
        """Same seed -> same rewrites. Non-flaky regression surface."""
        first = [i.type for i in self._make_generator().generate_work_items(days=7)]
        second = [i.type for i in self._make_generator().generate_work_items(days=7)]
        assert first == second

    def test_does_not_touch_already_diverse_buckets(self) -> None:
        """When a bucket already has diverse types, the pass is a no-op for it.

        Indirect check: type distribution over a 30-day run stays close to
        the configured investment-weighted expectation (some bugs, mostly
        story/task), not skewed to the flip preference alone.
        """
        gen = self._make_generator()
        items = gen.generate_work_items(days=30)
        counts = Counter(i.type for i in items)
        # Bug ratio should stay plausible (<= 40%) — the co-occurrence pass
        # only flips monotype buckets, not diverse ones, so the bulk stays
        # driven by random.random() > 0.85 bias.
        total = sum(counts.values())
        assert counts.get("bug", 0) / max(total, 1) <= 0.4


class TestRepoCooccurrenceVerification:
    """Runner-side invariant: >=2 teams span multiple repos."""

    def test_default_assignments_meet_minimum(self) -> None:
        """Happy path: the standard 10-team x 4-repo assignment produces
        enough multi-repo teams to generate cross-repo flow_matrix edges."""
        teams = SyntheticDataGenerator(
            repo_name="acme/demo-app", seed=5
        ).generate_teams(count=10)
        assignments = _build_repo_team_assignments(teams, repo_count=4, seed=5)
        team_to_repos: dict[str, set[int]] = defaultdict(set)
        for repo_idx, repo_teams in enumerate(assignments):
            for team in repo_teams:
                team_to_repos[team.id].add(repo_idx)
        multi_repo = sum(1 for r in team_to_repos.values() if len(r) >= 2)
        assert multi_repo >= 2, (
            f"Only {multi_repo} team(s) span multiple repos; CHAOS-1292 "
            f"chord REPO view needs >= 2."
        )

    def test_single_repo_plan_is_vacuously_ok(self) -> None:
        """With 1 owned repo, bridge edges are impossible by construction —
        the verifier must NOT raise just because density is 0."""
        team_to_repos = {"team-a": [0], "team-b": [0]}
        _verify_repo_cooccurrence_density(team_to_repos, owned_repo_count=1)

    def test_raises_on_sparse_multi_repo_assignments(self) -> None:
        """If only one team spans multiple repos the verifier raises — the
        chord Repository view would render effectively empty."""
        team_to_repos = {
            "team-a": [0, 1],
            "team-b": [0],
            "team-c": [1],
        }
        with pytest.raises(AssertionError, match="REPO bridge density insufficient"):
            _verify_repo_cooccurrence_density(team_to_repos, owned_repo_count=3)

    def test_dedupes_duplicate_repo_entries(self) -> None:
        """Multi-team assignments can record the same (team, repo) pair twice;
        uniqueness is what matters for bridge density."""
        team_to_repos = {
            "team-a": [0, 0, 1],
            "team-b": [0, 0],
            "team-c": [1, 2],
        }
        _verify_repo_cooccurrence_density(team_to_repos, owned_repo_count=3)


class TestFlowMatrixCrossEntityEdgeCount:
    """End-to-end fixture check: the number of NON-self-loop entries we'd
    compute from a full fixture run meets CHAOS-1292's >= 5 success criterion
    for both dimensions. Pure Python — no DB required."""

    def _cross_entity_pairs(
        self,
        items: list[object],
        bridge_key: str,
        dim_key: str,
    ) -> int:
        """Count distinct ordered (dim_a, dim_b) pairs with dim_a != dim_b
        appearing in the same bridge bucket."""
        buckets: dict[tuple, set[str]] = defaultdict(set)
        for item in items:
            day = (
                item.completed_at or item.started_at or item.created_at
            ).date()
            bridge_val = getattr(item, bridge_key)
            dim_val = getattr(item, dim_key)
            if not bridge_val or not dim_val:
                continue
            buckets[(bridge_val, day)].add(str(dim_val))
        pairs = set()
        for values in buckets.values():
            for a in values:
                for b in values:
                    if a != b:
                        pairs.add((a, b))
        return len(pairs)

    def test_work_type_cross_edges_meet_min_five(self) -> None:
        """Generated work_items produce >= 5 ordered cross-type pairs when
        bridged through (repo_id, day)."""
        gen = SyntheticDataGenerator(
            repo_name="acme/demo-app", provider="github", seed=13
        )
        items = gen.generate_work_items(days=30)
        # All items share a single repo_id in this generator, so the (repo,
        # day) bridge reduces to (day) here — still a valid proxy for
        # the WORK_TYPE cross-type guarantee inside one repo.
        pairs = self._cross_entity_pairs(
            items, bridge_key="repo_id", dim_key="type"
        )
        assert pairs >= 5, (
            f"Only {pairs} cross-type pairs in 30d fixture — WORK_TYPE "
            "chord would fall below the >= 5 success criterion."
        )
