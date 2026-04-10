from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from dev_health_ops.metrics.benchmarking import (
    DEFAULT_CORRELATION_PAIRS,
    classify_maturity_bands,
    compute_internal_baselines,
    compute_metric_correlation,
    compute_period_comparison,
    detect_metric_anomalies,
    generate_benchmark_insights,
)
from dev_health_ops.metrics.benchmarking._common import MetricPoint

NOW = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)


def _series(start_day: date, values: list[float]) -> list[MetricPoint]:
    return [
        MetricPoint(day=start_day + timedelta(days=index), value=value)
        for index, value in enumerate(values)
    ]


def test_period_comparison_with_known_values() -> None:
    current = _series(date(2026, 4, 7), [0.90, 0.95, 1.00])
    previous = _series(date(2026, 4, 4), [0.60, 0.70, 0.80])

    record = compute_period_comparison(
        metric_name="success_rate",
        scope_type="repo",
        scope_key="repo-a",
        current_period_start=date(2026, 4, 7),
        current_period_end=date(2026, 4, 9),
        comparison_period_start=date(2026, 4, 4),
        comparison_period_end=date(2026, 4, 6),
        current_period_points=current,
        comparison_period_points=previous,
        computed_at=NOW,
    )

    assert record is not None
    assert record.current_value == pytest.approx(0.95)
    assert record.comparison_value == pytest.approx(0.7)
    assert record.absolute_delta == pytest.approx(0.25)
    assert record.percentage_change == pytest.approx(35.7143)
    assert record.trend_direction == "improving"


def test_baseline_computation_accuracy() -> None:
    as_of_day = date(2026, 4, 10)
    series_by_scope = {
        "team-a": _series(date(2026, 4, 1), [1.0, 2.0, 3.0, 4.0]),
        "team-b": _series(date(2026, 4, 1), [2.0, 2.0, 2.0, 2.0]),
        "team-c": _series(date(2026, 4, 1), [4.0, 4.0, 4.0, 4.0]),
        "team-d": _series(date(2026, 4, 1), [5.0, 5.0, 5.0, 5.0]),
    }

    records = compute_internal_baselines(
        metric_name="flake_rate",
        scope_type="team",
        series_by_scope=series_by_scope,
        as_of_day=as_of_day,
        computed_at=NOW,
        windows=(30,),
    )

    team_b = next(record for record in records if record.scope_key == "team-b")
    assert team_b.baseline_value == pytest.approx(2.0)
    assert team_b.current_value == pytest.approx(2.0)
    assert team_b.p25_value == pytest.approx(3.5)
    assert team_b.p50_value == pytest.approx(4.0)
    assert team_b.p75_value == pytest.approx(4.25)
    assert team_b.p90_value == pytest.approx(4.7)
    assert team_b.percentile_rank == pytest.approx(12.5)


def test_maturity_band_classification_at_boundaries() -> None:
    as_of_day = date(2026, 4, 10)
    series_by_scope = {
        "team-a": _series(date(2026, 4, 10), [1.0]),
        "team-b": _series(date(2026, 4, 10), [2.0]),
        "team-c": _series(date(2026, 4, 10), [3.0]),
        "team-d": _series(date(2026, 4, 10), [4.0]),
    }
    baselines = compute_internal_baselines(
        metric_name="line_coverage_pct",
        scope_type="team",
        series_by_scope=series_by_scope,
        as_of_day=as_of_day,
        computed_at=NOW,
        windows=(30,),
    )
    maturity = {
        record.scope_key: record for record in classify_maturity_bands(baselines)
    }

    assert maturity["team-a"].maturity_band == "emerging"
    assert maturity["team-b"].maturity_band == "developing"
    assert maturity["team-c"].maturity_band == "established"
    assert maturity["team-d"].maturity_band == "leading"


def test_anomaly_detection_with_synthetic_data() -> None:
    series_by_scope = {
        "repo-a": _series(
            date(2026, 3, 1),
            [0.10, 0.11, 0.09, 0.10, 0.12, 0.11, 0.10, 0.09, 0.45],
        )
    }
    records = detect_metric_anomalies(
        metric_name="flake_rate",
        scope_type="repo",
        series_by_scope=series_by_scope,
        as_of_day=date(2026, 3, 9),
        computed_at=NOW,
        rolling_window_days=30,
        z_threshold=2.0,
        min_history_points=5,
    )

    regression = next(
        record for record in records if record.anomaly_type == "regression"
    )
    assert regression.direction == "up"
    assert regression.severity in {"warning", "critical"}
    assert regression.z_score > 2.0


def test_correlation_computation_with_known_series() -> None:
    left = {
        "team-a": _series(date(2026, 4, 1), [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        "team-b": _series(date(2026, 4, 1), [1.0, 3.0, 2.0, 5.0, 4.0, 6.0]),
    }
    right = {
        "team-a": _series(date(2026, 4, 1), [2.0, 4.0, 6.0, 8.0, 10.0, 12.0]),
        "team-b": _series(date(2026, 4, 1), [5.0, 1.0, 4.0, 2.0, 6.0, 3.0]),
    }
    records = compute_metric_correlation(
        metric_name="flake_rate",
        paired_metric_name="cycle_time_hours",
        scope_type="team",
        left_series_by_scope=left,
        right_series_by_scope=right,
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 6),
        computed_at=NOW,
    )

    team_a = next(record for record in records if record.scope_key == "team-a")
    team_b = next(record for record in records if record.scope_key == "team-b")
    assert team_a.coefficient == pytest.approx(1.0)
    assert team_a.is_significant is True
    assert abs(team_b.coefficient) < 0.5  # weak or no correlation
    assert team_b.is_significant is False


def test_benchmark_insight_generation_uses_hedged_language() -> None:
    comparison = compute_period_comparison(
        metric_name="success_rate",
        scope_type="repo",
        scope_key="repo-a",
        current_period_start=date(2026, 4, 7),
        current_period_end=date(2026, 4, 9),
        comparison_period_start=date(2026, 4, 4),
        comparison_period_end=date(2026, 4, 6),
        current_period_points=_series(date(2026, 4, 7), [0.6, 0.7, 0.8]),
        comparison_period_points=_series(date(2026, 4, 4), [0.9, 0.9, 0.9]),
        computed_at=NOW,
    )
    assert comparison is not None
    anomaly = detect_metric_anomalies(
        metric_name="flake_rate",
        scope_type="repo",
        series_by_scope={
            "repo-a": _series(
                date(2026, 3, 1), [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.5]
            )
        },
        as_of_day=date(2026, 3, 8),
        computed_at=NOW,
        min_history_points=5,
    )
    correlations = compute_metric_correlation(
        metric_name=DEFAULT_CORRELATION_PAIRS[0][0],
        paired_metric_name=DEFAULT_CORRELATION_PAIRS[0][1],
        scope_type="team",
        left_series_by_scope={"team-a": _series(date(2026, 4, 1), [1, 2, 3, 4, 5])},
        right_series_by_scope={"team-a": _series(date(2026, 4, 1), [2, 4, 6, 8, 10])},
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 5),
        computed_at=NOW,
    )

    insights = generate_benchmark_insights(
        period_comparisons=[comparison],
        anomalies=anomaly,
        correlations=correlations,
        computed_at=NOW,
    )

    assert insights
    for insight in insights:
        assert any(
            token in insight.summary for token in ("appears", "suggests", "leans")
        )
        json.loads(insight.evidence_json)
