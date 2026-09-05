#!/usr/bin/env python3
"""Generate the frozen golden for internal/teamresolve's oracle (CHAOS-5084).

Runs the REAL `_repo_to_team_map_for_compounding_risk` (job_daily.py:497) --
the same Python function team_cognitive_load, team_complexity, and
compounding_risk team scope all mirror -- over a corpus exercising every
precedence branch teamresolve.ResolveFromOwnershipMap claims to reproduce.

The corpus is fed pre-resolved inputs (a `team_repo_ownership_map` dict, a
`repo_team_resolver` stub) rather than reading ClickHouse: this function is
already decoupled from the ClickHouse read on the Python side (the caller
loads the map via `load_team_repo_ownership_map` first), which is exactly
what makes an in-process, no-ClickHouse oracle possible on both sides -- the
ClickHouse queries themselves are separate, already-tested components
(teamownership.OwnedRepoIDs / load_team_repo_ownership_map), not what this
oracle is proving.

Usage: generate_teamresolve_python_golden.py [--stdout]
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from dev_health_ops.metrics.job_daily import (  # noqa: E402
    _repo_to_team_map_for_compounding_risk,
)


class _FakeRow:
    def __init__(self, repo_id: uuid.UUID) -> None:
        self.repo_id = repo_id


class _FakePatternResolver:
    """Mirrors RepoPatternTeamResolver.resolve's (name) -> (id, name) shape."""

    def __init__(self, by_name: dict[str, str]) -> None:
        self._by_name = by_name

    def resolve(self, repo_name: str) -> tuple[str | None, str | None]:
        team_id = self._by_name.get(repo_name)
        if team_id is None:
            return None, None
        return team_id, team_id


def _make_uuid(tag: str) -> uuid.UUID:
    # Deterministic, readable UUIDs keyed by a short tag -- so a diff between
    # two golden runs shows WHICH case moved, not an opaque random id.
    return uuid.uuid5(uuid.NAMESPACE_URL, f"teamresolve-golden:{tag}")


def _case_ownership_hit() -> dict[str, Any]:
    """Ownership resolves the repo directly; pattern resolver is never consulted."""
    repo = _make_uuid("ownership-hit")
    return {
        "name": "ownership_hit",
        "repo_ids": [repo],
        "repo_names_by_id": {repo: "acme/ownership-hit"},
        "ownership_map": {str(repo): "team-ownership"},
        "pattern_map": {"acme/ownership-hit": "team-pattern-should-not-win"},
    }


def _case_pattern_fallback() -> dict[str, Any]:
    """No ownership row; the pattern resolver supplies the team."""
    repo = _make_uuid("pattern-fallback")
    return {
        "name": "pattern_fallback",
        "repo_ids": [repo],
        "repo_names_by_id": {repo: "acme/pattern-fallback"},
        "ownership_map": {},
        "pattern_map": {"acme/pattern-fallback": "team-pattern"},
    }


def _case_unresolved() -> dict[str, Any]:
    """Neither source resolves the repo: no ownership row, no pattern match."""
    repo = _make_uuid("unresolved")
    return {
        "name": "unresolved",
        "repo_ids": [repo],
        "repo_names_by_id": {repo: "acme/unresolved"},
        "ownership_map": {},
        "pattern_map": {},
    }


def _case_stale_ownership_absent_from_catalog() -> dict[str, Any]:
    """CHAOS-5141: ownership resolves a repo the CURRENT catalog no longer
    carries (removed/renamed since team_repo_ownership last ran) -- must be
    excluded, not attributed."""
    repo = _make_uuid("stale-ownership")
    return {
        "name": "stale_ownership_absent_from_catalog",
        "repo_ids": [repo],
        "repo_names_by_id": {},  # deliberately absent
        "ownership_map": {str(repo): "team-stale"},
        "pattern_map": {},
    }


def _case_pattern_hit_absent_from_catalog() -> dict[str, Any]:
    """A repo absent from repo_names_by_id must never reach the pattern
    resolver either -- both paths share the same gate."""
    repo = _make_uuid("pattern-absent-from-catalog")
    return {
        "name": "pattern_hit_absent_from_catalog",
        "repo_ids": [repo],
        "repo_names_by_id": {},  # deliberately absent
        "ownership_map": {},
        # Keyed by a name the resolver COULD match if ever asked -- proves it
        # never gets asked, not merely that the map lookup missed.
        "pattern_map": {"acme/never-asked": "team-should-not-appear"},
    }


def _case_multi_repo_mixed() -> dict[str, Any]:
    """One case combining all three outcomes in a single call, the shape a
    real org/day actually presents: several repos, several resolutions."""
    owned_repo = _make_uuid("multi-owned")
    pattern_repo = _make_uuid("multi-pattern")
    unresolved_repo = _make_uuid("multi-unresolved")
    return {
        "name": "multi_repo_mixed",
        "repo_ids": [owned_repo, pattern_repo, unresolved_repo],
        "repo_names_by_id": {
            owned_repo: "acme/multi-owned",
            pattern_repo: "acme/multi-pattern",
            unresolved_repo: "acme/multi-unresolved",
        },
        "ownership_map": {str(owned_repo): "team-a"},
        "pattern_map": {"acme/multi-pattern": "team-b"},
    }


CASES = [
    _case_ownership_hit(),
    _case_pattern_fallback(),
    _case_unresolved(),
    _case_stale_ownership_absent_from_catalog(),
    _case_pattern_hit_absent_from_catalog(),
    _case_multi_repo_mixed(),
]


def _run_case(case: dict[str, Any]) -> dict[str, Any]:
    rows = [_FakeRow(repo_id) for repo_id in case["repo_ids"]]
    resolver = _FakePatternResolver(case["pattern_map"])
    result = _repo_to_team_map_for_compounding_risk(
        repo_metrics_rows=rows,
        repo_names_by_id=case["repo_names_by_id"],
        repo_team_resolver=resolver,
        team_repo_ownership_map=case["ownership_map"],
        org_id="org-golden",
        day=None,
    )
    return {"case": case["name"], "result": result}


def render() -> str:
    payload = [_run_case(case) for case in CASES]
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()

    rendered = render()
    if args.stdout:
        sys.stdout.write(rendered)
        return 0

    out_path = REPO_ROOT / "tests" / "fixtures" / "teamresolve_python_golden.json"
    out_path.write_text(rendered)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
