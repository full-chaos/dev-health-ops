from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
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
