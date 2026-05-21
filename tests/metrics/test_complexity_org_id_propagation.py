"""Regression tests for CHAOS-1744 — complexity rows must carry org_id.

The original CHAOS-1744 fix wired Compounding Risk into the daily job but did
not catch that ``RepoComplexityDaily`` rows were being persisted with an
empty-string ``org_id`` because ``_build_snapshots`` dropped the parameter
before constructing the dataclass. When ``load_repo_complexity_delta_30d``
later filtered by the running org's UUID, every query returned zero rows,
``complexity_delta`` was always ``None``, ``has_required_inputs()`` returned
``False``, and every Compounding Risk row ended up with ``compounding_risk
= NULL`` and ``severity = 'unknown'``.

These tests assert org_id propagation **without mocking the sink's query
behaviour**, which was the gap in the original PR #759 test.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from dev_health_ops.analytics.complexity import FileComplexity
from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.metrics.compounding_risk import load_repo_complexity_delta_30d
from dev_health_ops.metrics.job_complexity_db import _build_snapshots

ORG = "11111111-1111-1111-1111-111111111111"
OTHER_ORG = "22222222-2222-2222-2222-222222222222"
DAY = date(2026, 5, 21)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


def _file(path: str = "src/a.py") -> FileComplexity:
    return FileComplexity(
        file_path=path,
        language="python",
        loc=120,
        functions_count=10,
        cyclomatic_total=30,
        cyclomatic_avg=3.0,
        high_complexity_functions=1,
        very_high_complexity_functions=0,
    )


def test_build_snapshots_propagates_org_id_to_repo_daily() -> None:
    repo_id = uuid.uuid4()
    snapshots, repo_daily = _build_snapshots(
        repo_id, DAY, "abc123", [_file()], NOW, ORG
    )
    assert repo_daily.org_id == ORG, (
        f"RepoComplexityDaily.org_id must equal {ORG!r}, "
        f"got {repo_daily.org_id!r}. This is the CHAOS-1744 bug."
    )


def test_build_snapshots_propagates_org_id_to_file_snapshots() -> None:
    repo_id = uuid.uuid4()
    snapshots, _ = _build_snapshots(
        repo_id, DAY, "abc123", [_file("a.py"), _file("b.py")], NOW, ORG
    )
    assert len(snapshots) == 2
    for snap in snapshots:
        assert snap.org_id == ORG, (
            f"FileComplexitySnapshot.org_id must equal {ORG!r}, "
            f"got {snap.org_id!r}."
        )


def test_fixtures_generator_propagates_org_id() -> None:
    repo_id = uuid.uuid4()
    gen = SyntheticDataGenerator(repo_name="acme/demo-app", seed=42)
    data = gen.generate_complexity_metrics(days=2, org_id=ORG)

    assert data["snapshots"], "fixture generator must produce snapshots"
    assert data["dailies"], "fixture generator must produce dailies"
    for snap in data["snapshots"]:
        assert snap.org_id == ORG
    for daily in data["dailies"]:
        assert daily.org_id == ORG


# ---------------------------------------------------------------------------
# Real-world-style fake sink: stores rows AND enforces the same WHERE filters
# that ClickHouse does. The original PR #759 test mocked query_dicts to
# return the same dict regardless of params — this is what we are fixing.
# ---------------------------------------------------------------------------


class _OrgFilteringSink:
    """Fake sink that mimics ClickHouse's WHERE filtering.

    The whole point of this test class is to NOT rubber-stamp every query
    with a fixed return value (which is what the PR #759 test did). It
    actually applies the WHERE filters from the parameters, so mismatched
    org_id behaves exactly the way real ClickHouse does — by returning
    zero rows.
    """

    def __init__(self) -> None:
        # rows is a list of (repo_id_str, day, cpk, org_id) tuples,
        # the four fields ``load_repo_complexity_delta_30d`` cares about.
        self.rows: list[tuple[str, date, float, str]] = []

    def add(self, *, repo_id: str, day: date, cpk: float, org_id: str) -> None:
        self.rows.append((repo_id, day, cpk, org_id))

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # Apply WHERE filter using the parameters the production query
        # would set: repo_id, org_id, [start, end] date window with a
        # midpoint split.
        repo_id = parameters["repo_id"]
        start = parameters["start"]
        end = parameters["end"]
        mid = parameters["mid"]
        org_id = parameters["org_id"]

        matched = [
            (d, cpk)
            for (rid, d, cpk, oid) in self.rows
            if rid == repo_id and start <= d <= end and oid == org_id
        ]
        if not matched:
            return []

        first = [cpk for (d, cpk) in matched if d < mid]
        second = [cpk for (d, cpk) in matched if d >= mid]
        first_avg = sum(first) / len(first) if first else None
        second_avg = sum(second) / len(second) if second else None
        return [{"first_half": first_avg, "second_half": second_avg}]


def test_load_complexity_delta_returns_value_when_org_id_matches() -> None:
    """Demonstrates the happy path that PR #759's test never actually exercised."""
    sink = _OrgFilteringSink()
    repo_id = uuid.uuid4()

    # Populate the trailing 30-day window with rising cyclomatic_per_kloc.
    for i in range(30):
        d = DAY.fromordinal(DAY.toordinal() - 29 + i)
        cpk = 100.0 + i * 2.0  # rising trend, second half higher
        sink.add(repo_id=str(repo_id), day=d, cpk=cpk, org_id=ORG)

    delta = load_repo_complexity_delta_30d(
        sink, repo_id=str(repo_id), day=DAY, org_id=ORG
    )
    assert delta is not None
    assert delta > 0.0, "rising trend must produce positive delta"


def test_load_complexity_delta_returns_none_when_org_id_mismatches() -> None:
    """The exact CHAOS-1744 bug: rows tagged with the wrong org return None.

    Pre-fix, every existing row had ``org_id=''`` because ``_build_snapshots``
    dropped the parameter. The running daily job queried with the actual
    org UUID and matched nothing.
    """
    sink = _OrgFilteringSink()
    repo_id = uuid.uuid4()
    # Data is stored under empty-string org_id (the pre-fix state).
    for i in range(30):
        d = DAY.fromordinal(DAY.toordinal() - 29 + i)
        cpk = 100.0 + i * 2.0
        sink.add(repo_id=str(repo_id), day=d, cpk=cpk, org_id="")

    # Query with the real running org_id returns nothing.
    delta = load_repo_complexity_delta_30d(
        sink, repo_id=str(repo_id), day=DAY, org_id=ORG
    )
    assert delta is None, (
        "Rows persisted with mismatched org_id must not be discoverable "
        "by the daily job's org_id-scoped query. This is the failure mode "
        "PR #759 missed."
    )


def test_load_complexity_delta_isolates_orgs() -> None:
    """Multi-tenant safety: one org's complexity must not leak into another."""
    sink = _OrgFilteringSink()
    repo_id = uuid.uuid4()

    for i in range(30):
        d = DAY.fromordinal(DAY.toordinal() - 29 + i)
        sink.add(
            repo_id=str(repo_id), day=d, cpk=100.0 + i, org_id=ORG
        )

    # Same data tagged for ORG; another org querying must see nothing.
    delta_other = load_repo_complexity_delta_30d(
        sink, repo_id=str(repo_id), day=DAY, org_id=OTHER_ORG
    )
    assert delta_other is None
