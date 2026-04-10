from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from dev_health_ops.metrics.benchmarking.anomalies import detect_anomalies
from dev_health_ops.metrics.benchmarking.baselines import (
    _percentile_value,
    _rank_value,
    compute_baseline,
)
from dev_health_ops.metrics.benchmarking.correlations import (
    _pearson,
    compute_correlation,
)
from dev_health_ops.metrics.benchmarking.maturity import classify_maturity
from dev_health_ops.metrics.benchmarking.period_comparison import compare_periods
from dev_health_ops.metrics.benchmarking.schemas import (
    AnomalyDirection,
    AnomalySeverity,
    MaturityBand,
    Trend,
)


def _mock_client(result_rows: list[tuple]) -> MagicMock:
    client = MagicMock()
    result = MagicMock()
    result.result_rows = result_rows
    client.query.return_value = result
    return client


class TestPeriodComparison:
    def test_upward_trend(self) -> None:
        calls = iter(
            [
                [(80.0,)],
                [(50.0,)],
            ]
        )

        def side_effect(*_args, **_kwargs):
            mock = MagicMock()
            mock.result_rows = next(calls)
            return mock

        client = MagicMock()
        client.query.side_effect = side_effect

        result = compare_periods(
            client,
            metric="success_rate",
            scope="repo-1",
            current_start=date(2026, 3, 1),
            current_end=date(2026, 3, 31),
            prev_start=date(2026, 2, 1),
            prev_end=date(2026, 2, 28),
        )

        assert result.trend == Trend.UP
        assert result.current_value == 80.0
        assert result.previous_value == 50.0
        assert result.absolute_delta == 30.0
        assert result.pct_change == pytest.approx(60.0)

    def test_downward_trend(self) -> None:
        calls = iter(
            [
                [(40.0,)],
                [(50.0,)],
            ]
        )

        def side_effect(*_args, **_kwargs):
            mock = MagicMock()
            mock.result_rows = next(calls)
            return mock

        client = MagicMock()
        client.query.side_effect = side_effect

        result = compare_periods(
            client,
            metric="success_rate",
            scope="repo-1",
            current_start=date(2026, 3, 1),
            current_end=date(2026, 3, 31),
            prev_start=date(2026, 2, 1),
            prev_end=date(2026, 2, 28),
        )

        assert result.trend == Trend.DOWN
        assert result.pct_change == pytest.approx(-20.0)

    def test_flat_trend(self) -> None:
        calls = iter(
            [
                [(51.0,)],
                [(50.0,)],
            ]
        )

        def side_effect(*_args, **_kwargs):
            mock = MagicMock()
            mock.result_rows = next(calls)
            return mock

        client = MagicMock()
        client.query.side_effect = side_effect

        result = compare_periods(
            client,
            metric="success_rate",
            scope="repo-1",
            current_start=date(2026, 3, 1),
            current_end=date(2026, 3, 31),
            prev_start=date(2026, 2, 1),
            prev_end=date(2026, 2, 28),
        )

        assert result.trend == Trend.FLAT

    def test_zero_previous_value(self) -> None:
        calls = iter(
            [
                [(10.0,)],
                [(0.0,)],
            ]
        )

        def side_effect(*_args, **_kwargs):
            mock = MagicMock()
            mock.result_rows = next(calls)
            return mock

        client = MagicMock()
        client.query.side_effect = side_effect

        result = compare_periods(
            client,
            metric="success_rate",
            scope="repo-1",
            current_start=date(2026, 3, 1),
            current_end=date(2026, 3, 31),
            prev_start=date(2026, 2, 1),
            prev_end=date(2026, 2, 28),
        )

        assert result.pct_change == 0.0
        assert result.trend == Trend.FLAT


class TestBaselines:
    def test_percentile_computation(self) -> None:
        vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        assert _percentile_value(vals, 50.0) == pytest.approx(55.0)
        assert _percentile_value(vals, 0.0) == pytest.approx(10.0)
        assert _percentile_value(vals, 100.0) == pytest.approx(100.0)
        assert _percentile_value(vals, 25.0) == pytest.approx(32.5)

    def test_rank_computation(self) -> None:
        vals = [10.0, 20.0, 30.0, 40.0, 50.0]
        assert _rank_value(vals, 30.0) == pytest.approx(50.0)
        assert _rank_value(vals, 10.0) == pytest.approx(10.0)
        assert _rank_value(vals, 50.0) == pytest.approx(90.0)

    def test_empty_values(self) -> None:
        assert _percentile_value([], 50.0) == 0.0
        assert _rank_value([], 30.0) == 0.0

    def test_compute_baseline_returns_records(self) -> None:
        client = _mock_client([("team-a", 80.0), ("team-b", 60.0), ("team-c", 40.0)])

        records = compute_baseline(client, "success_rate", "team_id", period_days=30)

        assert len(records) == 3
        assert records[0].metric == "success_rate"
        assert records[0].period_days == 30
        assert records[0].mean == pytest.approx(60.0)

    def test_compute_baseline_empty(self) -> None:
        client = _mock_client([])
        records = compute_baseline(client, "success_rate", "team_id")
        assert records == []


class TestMaturity:
    @pytest.mark.parametrize(
        ("rank", "expected_band"),
        [
            (10.0, MaturityBand.EMERGING),
            (24.9, MaturityBand.EMERGING),
            (25.0, MaturityBand.DEVELOPING),
            (49.9, MaturityBand.DEVELOPING),
            (50.0, MaturityBand.ESTABLISHED),
            (74.9, MaturityBand.ESTABLISHED),
            (75.0, MaturityBand.LEADING),
            (99.0, MaturityBand.LEADING),
        ],
    )
    def test_band_boundaries(self, rank: float, expected_band: MaturityBand) -> None:
        result = classify_maturity("success_rate", "repo-1", rank)
        assert result.band == expected_band
        assert result.score == rank

    def test_confidence_clamped(self) -> None:
        result = classify_maturity(
            "success_rate", "repo-1", 50.0, data_completeness=1.5
        )
        assert result.confidence == 1.0

        result = classify_maturity(
            "success_rate", "repo-1", 50.0, data_completeness=-0.5
        )
        assert result.confidence == 0.0


class TestAnomalyDetection:
    def test_detects_known_outlier(self) -> None:
        normal_rows = [(date(2026, 3, i), 50.0) for i in range(1, 29)]
        normal_rows.append((date(2026, 3, 29), 200.0))
        normal_rows.append((date(2026, 3, 30), 50.0))
        client = _mock_client(normal_rows)

        anomalies = detect_anomalies(
            client, "p95_duration_seconds", "repo-1", lookback_days=30, threshold=2.0
        )

        assert len(anomalies) >= 1
        outlier = [a for a in anomalies if a.day == date(2026, 3, 29)]
        assert len(outlier) == 1
        assert outlier[0].severity in {AnomalySeverity.HIGH, AnomalySeverity.MEDIUM}
        assert outlier[0].direction == AnomalyDirection.REGRESSION

    def test_no_anomalies_in_stable_data(self) -> None:
        rows = [(date(2026, 3, i), 50.0) for i in range(1, 31)]
        client = _mock_client(rows)

        anomalies = detect_anomalies(client, "success_rate", "repo-1")
        assert anomalies == []

    def test_insufficient_data(self) -> None:
        client = _mock_client([(date(2026, 3, 1), 50.0)])
        anomalies = detect_anomalies(client, "success_rate", "repo-1")
        assert anomalies == []

    def test_improvement_direction(self) -> None:
        rows = [(date(2026, 3, i), 50.0) for i in range(1, 29)]
        rows.append((date(2026, 3, 29), 95.0))
        rows.append((date(2026, 3, 30), 50.0))
        client = _mock_client(rows)

        anomalies = detect_anomalies(
            client, "success_rate", "repo-1", lookback_days=30, threshold=2.0
        )
        improvements = [
            a for a in anomalies if a.direction == AnomalyDirection.IMPROVEMENT
        ]
        assert len(improvements) >= 1


class TestCorrelations:
    def test_perfect_positive_correlation(self) -> None:
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [2.0, 4.0, 6.0, 8.0, 10.0]
        assert _pearson(xs, ys) == pytest.approx(1.0)

    def test_perfect_negative_correlation(self) -> None:
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [10.0, 8.0, 6.0, 4.0, 2.0]
        assert _pearson(xs, ys) == pytest.approx(-1.0)

    def test_zero_correlation(self) -> None:
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [5.0, 5.0, 5.0, 5.0, 5.0]
        assert _pearson(xs, ys) == pytest.approx(0.0)

    def test_insufficient_data(self) -> None:
        assert _pearson([1.0, 2.0], [3.0, 4.0]) == 0.0

    def test_compute_correlation_hedging_language(self) -> None:
        rows = [(date(2026, 3, i), float(i), float(i * 2)) for i in range(1, 31)]
        client = _mock_client(rows)

        result = compute_correlation(
            client, "flake_rate", "p95_duration_seconds", "repo-1"
        )

        assert result.coefficient == pytest.approx(1.0)
        assert result.significant is True
        assert (
            "suggests" in result.interpretation.lower()
            or "appears" in result.interpretation.lower()
        )

    def test_insignificant_correlation_language(self) -> None:
        rows = [(date(2026, 3, i), 50.0, 50.0) for i in range(1, 31)]
        client = _mock_client(rows)

        result = compute_correlation(
            client, "flake_rate", "p95_duration_seconds", "repo-1"
        )

        assert result.significant is False
        assert (
            "does not appear" in result.interpretation.lower()
            or "appears" in result.interpretation.lower()
        )
