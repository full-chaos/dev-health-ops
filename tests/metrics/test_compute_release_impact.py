from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from dev_health_ops.metrics.release_impact import (
    _compute_confidence,
    _compute_day,
    compute_release_impact_daily,
)


class FakeQueryResult:
    def __init__(self, column_names: list[str], result_rows: list[list]):
        self.column_names = column_names
        self.result_rows = result_rows


def _make_client(responses: list[FakeQueryResult]) -> MagicMock:
    client = MagicMock()
    client.query = MagicMock(side_effect=responses)
    return client


def test_compute_confidence_perfect_conditions():
    score = _compute_confidence(
        coverage_ratio=1.0,
        total_sessions=500,
        concurrent_deploys=0,
    )
    assert score == pytest.approx(1.0)


def test_compute_confidence_no_sessions():
    score = _compute_confidence(
        coverage_ratio=1.0,
        total_sessions=0,
        concurrent_deploys=0,
    )
    assert 0.0 < score < 1.0
    assert score == pytest.approx(0.35 * 1.0 + 0.35 * 0.0 + 0.30 * 1.0)


def test_compute_confidence_high_concurrency_degrades():
    score_clean = _compute_confidence(1.0, 500, 0)
    score_noisy = _compute_confidence(1.0, 500, 5)
    assert score_noisy < score_clean


def test_compute_day_no_telemetry_returns_empty():
    client = _make_client(
        [
            FakeQueryResult(["release_ref", "environment"], []),
        ]
    )
    records = _compute_day(
        client, "org1", date(2026, 3, 15), datetime.now(tz=timezone.utc)
    )
    assert records == []


def test_compute_day_single_release():
    repo_id = uuid4()
    deploy_ts = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)

    responses = [
        FakeQueryResult(["release_ref", "environment"], [["v1.0.0", "production"]]),
        FakeQueryResult(["cnt"], [[2]]),
        FakeQueryResult(["deploy_ts"], [[deploy_ts]]),
        FakeQueryResult(["repo_id"], [[str(repo_id)]]),
        FakeQueryResult(["total_signals", "total_sessions"], [[10, 500]]),
        FakeQueryResult(["total_signals", "total_sessions"], [[15, 600]]),
        FakeQueryResult(["total_signals", "total_sessions"], [[5, 400]]),
        FakeQueryResult(["total_signals", "total_sessions"], [[8, 500]]),
        FakeQueryResult(["total_signals", "total_sessions"], [[15, 600]]),
        FakeQueryResult(["total_signals", "total_sessions"], [[8, 500]]),
        FakeQueryResult(["first_friction_ts"], [[deploy_ts + timedelta(hours=2)]]),
        FakeQueryResult(["cnt"], [[1]]),
        FakeQueryResult(["bucket_hours"], [[20]]),
    ]

    client = _make_client(responses)
    computed_at = datetime(2026, 3, 16, 0, 0, tzinfo=timezone.utc)
    records = _compute_day(client, "org1", date(2026, 3, 15), computed_at)

    assert len(records) == 1
    rec = records[0]
    assert rec.day == date(2026, 3, 15)
    assert rec.release_ref == "v1.0.0"
    assert rec.environment == "production"
    assert rec.repo_id == repo_id
    assert rec.computed_at == computed_at
    assert rec.org_id == "org1"
    assert rec.coverage_ratio == pytest.approx(0.5)
    assert rec.data_completeness == pytest.approx(20 / 24.0)
    assert rec.concurrent_deploy_count == 1
    assert rec.release_impact_confidence_score is not None
    assert rec.release_impact_confidence_score > 0.0
    assert rec.release_impact_confidence_score <= 1.0


@pytest.mark.asyncio
async def test_compute_release_impact_daily_writes_to_sink():
    client = _make_client(
        [
            FakeQueryResult(["release_ref", "environment"], []),
        ]
    )
    sink = MagicMock()
    sink.client = client
    sink.write_release_impact_daily = MagicMock()

    written = await compute_release_impact_daily(
        ch_client=client,
        sink=sink,
        org_id="org1",
        day=date(2026, 3, 15),
        recomputation_window_days=1,
    )
    assert written == 0
    sink.write_release_impact_daily.assert_not_called()


@pytest.mark.asyncio
async def test_compute_release_impact_daily_recomputation_window():
    call_count = 0

    def fake_query(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return FakeQueryResult(["release_ref", "environment"], [])

    client = MagicMock()
    client.query = MagicMock(side_effect=fake_query)
    sink = MagicMock()
    sink.client = client
    sink.write_release_impact_daily = MagicMock()

    await compute_release_impact_daily(
        ch_client=client,
        sink=sink,
        org_id="org1",
        day=date(2026, 3, 15),
        recomputation_window_days=3,
    )
    assert call_count == 3


def test_compute_day_missing_deploy_timestamp():
    responses = [
        FakeQueryResult(["release_ref", "environment"], [["v2.0.0", "staging"]]),
        FakeQueryResult(["cnt"], [[1]]),
        FakeQueryResult(["deploy_ts"], []),
        FakeQueryResult(["repo_id"], []),
        FakeQueryResult(["bucket_hours"], [[12]]),
    ]

    client = _make_client(responses)
    records = _compute_day(
        client, "org1", date(2026, 3, 15), datetime.now(tz=timezone.utc)
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.release_user_friction_delta is None
    assert rec.release_error_rate_delta is None
    assert rec.time_to_first_user_issue_after_release is None
    assert rec.concurrent_deploy_count == 0
    assert rec.missing_required_fields_count == 4


# ---------------------------------------------------------------------------
# Cross-tenant isolation (CHAOS-2381)
#
# Two orgs each have a deployment for the SAME release_ref + environment but a
# DIFFERENT repo_id. The ``deployments`` raw table has no ``org_id`` column, so
# the only thing that keeps orgA's compute from reading orgB's deployment is the
# ``repo_id IN (SELECT id FROM repos WHERE org_id = ...)`` predicate. This fake
# models the real two-org dataset: every ``deployments`` read is resolved
# THROUGH the repos sub-select, so if the production query dropped the predicate
# (or passed the wrong org_id) it would surface orgB's repo_id / inflate the
# release-count denominator and the assertions below would fail.
# ---------------------------------------------------------------------------

_ORG_A = "orgA"
_ORG_B = "orgB"
_SHARED_RELEASE = "v1.0.0"
_SHARED_ENV = "production"


class _TwoOrgFakeClient:
    """Fake ClickHouse client that enforces repo->org scoping on deployments."""

    def __init__(self, repo_a, repo_b):
        # repos table: repo_id -> org_id
        self._repo_org = {str(repo_a): _ORG_A, str(repo_b): _ORG_B}
        self._repo_a = repo_a
        self._repo_b = repo_b
        # Both orgs deployed the SAME release_ref+environment.
        self._deploy_ts = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)
        self.deployments_queries: list[tuple[str, dict]] = []

    def _scoped_repo_ids(self, params: dict) -> set[str]:
        """Resolve `repo_id IN (SELECT id FROM repos WHERE org_id = :org_id)`."""
        org_id = params.get("org_id")
        return {rid for rid, org in self._repo_org.items() if org == org_id}

    def query(self, query: str, parameters: dict):
        params = parameters or {}
        q = " ".join(query.split())

        if "FROM deployments" in q:
            self.deployments_queries.append((q, params))
            scoped = self._scoped_repo_ids(params)
            # release-count denominator
            if "count(DISTINCT release_ref)" in q and "AS cnt" in q:
                cnt = 1 if scoped else 0
                return FakeQueryResult(["cnt"], [[cnt]])
            # repo_id lookup
            if q.strip().startswith("SELECT repo_id FROM deployments") or (
                "SELECT repo_id" in q and "FROM deployments" in q
            ):
                repo_rows: list[list[Any]] = [[rid] for rid in scoped]
                return FakeQueryResult(["repo_id"], repo_rows[:1])
            # deploy timestamp
            if "AS deploy_ts" in q:
                ts_rows: list[list[Any]] = [[self._deploy_ts]] if scoped else []
                return FakeQueryResult(["deploy_ts"], ts_rows)
            # concurrent deploy count
            return FakeQueryResult(["cnt"], [[0]])

        # telemetry_signal_bucket: distinct release/env pairs for the day
        if "SELECT DISTINCT release_ref, environment" in q:
            return FakeQueryResult(
                ["release_ref", "environment"],
                [[_SHARED_RELEASE, _SHARED_ENV]],
            )
        # telemetry signal rate windows
        if "total_signals" in q and "total_sessions" in q:
            return FakeQueryResult(["total_signals", "total_sessions"], [[5, 400]])
        # first friction spike
        if "first_friction_ts" in q:
            return FakeQueryResult(["first_friction_ts"], [])
        # data completeness
        if "bucket_hours" in q:
            return FakeQueryResult(["bucket_hours"], [[24]])

        return FakeQueryResult([], [])


def test_compute_day_isolates_orgs_sharing_release_ref():
    repo_a = uuid4()
    repo_b = uuid4()
    client = _TwoOrgFakeClient(repo_a, repo_b)

    records = _compute_day(
        client, _ORG_A, date(2026, 3, 15), datetime.now(tz=timezone.utc)
    )

    assert len(records) == 1
    rec = records[0]

    # orgA must read ONLY its own deployment's repo_id, never orgB's.
    assert rec.repo_id == repo_a
    assert rec.repo_id != repo_b
    assert rec.org_id == _ORG_A

    # Coverage denominator excludes orgB's deployment: 1 covered / 1 total = 1.0
    # (if orgB leaked into _count_total_releases the denominator would be 2 and
    # coverage_ratio would drop to 0.5).
    assert rec.coverage_ratio == pytest.approx(1.0)

    # EVERY deployments read must carry the repo->org scoping predicate and the
    # correct org_id param (orgA, never orgB).
    assert client.deployments_queries, "expected at least one deployments read"
    scoping_predicate = (
        "repo_id IN (SELECT id FROM repos WHERE org_id = {org_id:String})"
    )
    for q, params in client.deployments_queries:
        assert scoping_predicate in q
        assert params.get("org_id") == _ORG_A
        assert params.get("org_id") != _ORG_B


def test_get_repo_id_for_release_is_org_scoped():
    """Direct check: the repo_id lookup never returns another org's repo."""
    from dev_health_ops.metrics.release_impact import _get_repo_id_for_release

    repo_a = uuid4()
    repo_b = uuid4()
    client = _TwoOrgFakeClient(repo_a, repo_b)

    # orgA's compute resolves orgA's repo only.
    got_a = _get_repo_id_for_release(client, _ORG_A, _SHARED_RELEASE, _SHARED_ENV)
    assert got_a == repo_a

    # orgB's compute resolves orgB's repo only — same release_ref+env, no bleed.
    got_b = _get_repo_id_for_release(client, _ORG_B, _SHARED_RELEASE, _SHARED_ENV)
    assert got_b == repo_b
