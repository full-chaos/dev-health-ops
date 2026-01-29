from datetime import date, datetime, timedelta, timezone

import pytest

from dev_health_ops.metrics.compute_capacity import (
    ForecastResult,
    ThroughputHistory,
    ThroughputSample,
    compute_percentiles,
    forecast_capacity,
    monte_carlo_forecast_days,
    monte_carlo_forecast_items,
)


def _make_samples(
    throughputs: list[int], start_date: date | None = None
) -> list[ThroughputSample]:
    if start_date is None:
        start_date = date(2025, 1, 1)
    return [
        ThroughputSample(day=start_date + timedelta(days=i), items_completed=t)
        for i, t in enumerate(throughputs)
    ]


class TestThroughputHistory:
    def test_empty_history(self):
        history = ThroughputHistory([])
        assert history.days_of_history == 0
        assert history.mean == 0.0
        assert history.stddev == 0.0
        assert history.coefficient_of_variation == 0.0
        assert not history.is_sufficient()

    def test_single_sample(self):
        samples = _make_samples([5])
        history = ThroughputHistory(samples)
        assert history.days_of_history == 1
        assert history.mean == 5.0
        assert history.stddev == 0.0
        assert not history.is_sufficient()

    def test_multiple_samples(self):
        samples = _make_samples([2, 4, 6, 8, 10])
        history = ThroughputHistory(samples)
        assert history.days_of_history == 5
        assert history.mean == 6.0
        assert history.stddev == pytest.approx(2.828, rel=0.01)
        assert not history.is_sufficient(min_days=10)
        assert history.is_sufficient(min_days=5)

    def test_high_variance_detection(self):
        stable_samples = _make_samples([5, 5, 5, 5, 5])
        stable_history = ThroughputHistory(stable_samples)
        assert not stable_history.is_high_variance()

        variable_samples = _make_samples([0, 0, 0, 20, 0])
        variable_history = ThroughputHistory(variable_samples)
        assert variable_history.is_high_variance()


class TestMonteCarloForecasts:
    def test_forecast_days_empty_history_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            monte_carlo_forecast_days([], target_items=10)

    def test_forecast_days_zero_target_returns_zero(self):
        days, hit_max = monte_carlo_forecast_days(
            [5, 5, 5], target_items=0, simulations=100
        )
        assert len(days) == 100
        assert all(d == 0 for d in days)
        assert not hit_max

    def test_forecast_days_deterministic_with_seed(self):
        throughputs = [1, 2, 3, 4, 5]

        days1, _ = monte_carlo_forecast_days(
            throughputs, target_items=10, simulations=100, seed=42
        )
        days2, _ = monte_carlo_forecast_days(
            throughputs, target_items=10, simulations=100, seed=42
        )

        assert days1 == days2

    def test_forecast_days_reasonable_results(self):
        throughputs = [5] * 30
        days, hit_max = monte_carlo_forecast_days(
            throughputs, target_items=50, simulations=1000, seed=42
        )

        assert not hit_max
        assert min(days) == 10
        assert max(days) == 10

    def test_forecast_days_variable_throughput(self):
        throughputs = [0, 5, 10]
        days, _ = monte_carlo_forecast_days(
            throughputs, target_items=20, simulations=1000, seed=42
        )

        assert min(days) >= 2
        assert max(days) <= 365

    def test_forecast_days_hits_max_on_zero_throughput(self):
        throughputs = [0, 0, 0]
        days, hit_max = monte_carlo_forecast_days(
            throughputs, target_items=10, simulations=10, max_days=50
        )

        assert hit_max
        assert all(d == 50 for d in days)

    def test_forecast_items_empty_history_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            monte_carlo_forecast_items([], days_available=10)

    def test_forecast_items_zero_days_returns_zero(self):
        items = monte_carlo_forecast_items([5, 5, 5], days_available=0, simulations=100)
        assert len(items) == 100
        assert all(i == 0 for i in items)

    def test_forecast_items_deterministic_with_seed(self):
        throughputs = [1, 2, 3, 4, 5]

        items1 = monte_carlo_forecast_items(
            throughputs, days_available=10, simulations=100, seed=42
        )
        items2 = monte_carlo_forecast_items(
            throughputs, days_available=10, simulations=100, seed=42
        )

        assert items1 == items2

    def test_forecast_items_reasonable_results(self):
        throughputs = [5] * 30
        items = monte_carlo_forecast_items(
            throughputs, days_available=10, simulations=1000, seed=42
        )

        assert all(i == 50 for i in items)


class TestComputePercentiles:
    def test_empty_list(self):
        result = compute_percentiles([], [50, 85, 95])
        assert result == [0, 0, 0]

    def test_single_value(self):
        result = compute_percentiles([10], [50, 85, 95])
        assert result == [10, 10, 10]

    def test_known_distribution(self):
        values = list(range(1, 101))
        result = compute_percentiles(values, [50, 85, 95])
        assert result[0] == 50
        assert result[1] == 85
        assert result[2] == 95


class TestForecastCapacity:
    def test_requires_target(self):
        history = ThroughputHistory(_make_samples([5, 5, 5]))
        with pytest.raises(ValueError, match="Must provide either"):
            forecast_capacity(history)

    def test_requires_non_empty_history(self):
        history = ThroughputHistory([])
        with pytest.raises(ValueError, match="empty throughput history"):
            forecast_capacity(history, target_items=10)

    def test_fixed_scope_forecast(self):
        samples = _make_samples([5] * 30)
        history = ThroughputHistory(samples)

        result = forecast_capacity(
            history,
            target_items=50,
            team_id="team-1",
            work_scope_id="scope-1",
            simulations=1000,
            seed=42,
        )

        assert result.target_items == 50
        assert result.target_date is None
        assert result.team_id == "team-1"
        assert result.work_scope_id == "scope-1"
        assert result.simulation_count == 1000
        assert result.history_days == 30

        assert result.p50_days == 10
        assert result.p85_days == 10
        assert result.p95_days == 10
        assert result.p50_date is not None

        assert result.p50_items is None

        assert result.throughput_mean == 5.0
        assert result.throughput_stddev == 0.0
        assert not result.insufficient_history
        assert not result.high_variance

    def test_fixed_date_forecast(self):
        samples = _make_samples([5] * 30)
        history = ThroughputHistory(samples)

        target = date.today() + timedelta(days=10)
        result = forecast_capacity(
            history,
            target_date=target,
            simulations=1000,
            seed=42,
        )

        assert result.target_date == target
        assert result.target_items is None

        assert result.p50_items == 50
        assert result.p85_items == 50
        assert result.p95_items == 50

        assert result.p50_days is None
        assert result.p50_date is None

    def test_both_targets(self):
        samples = _make_samples([5] * 30)
        history = ThroughputHistory(samples)

        target_date = date.today() + timedelta(days=10)
        result = forecast_capacity(
            history,
            target_items=50,
            target_date=target_date,
            simulations=1000,
            seed=42,
        )

        assert result.p50_days is not None
        assert result.p50_items is not None

    def test_insufficient_history_flag(self):
        samples = _make_samples([5, 5, 5])
        history = ThroughputHistory(samples)

        result = forecast_capacity(history, target_items=10, simulations=100, seed=42)

        assert result.insufficient_history
        assert result.history_days == 3

    def test_high_variance_flag(self):
        samples = _make_samples([0, 0, 0, 20, 0] * 6)
        history = ThroughputHistory(samples)

        result = forecast_capacity(history, target_items=10, simulations=100, seed=42)

        assert result.high_variance

    def test_past_target_date_returns_zero_items(self):
        samples = _make_samples([5] * 30)
        history = ThroughputHistory(samples)

        past_date = date.today() - timedelta(days=10)
        result = forecast_capacity(history, target_date=past_date, simulations=100)

        assert result.p50_items == 0
        assert result.p85_items == 0
        assert result.p95_items == 0


class TestForecastResultDataclass:
    def test_forecast_result_is_frozen(self):
        result = ForecastResult(
            forecast_id="test-id",
            computed_at=datetime.now(timezone.utc),
            team_id=None,
            work_scope_id=None,
            backlog_size=0,
            target_items=10,
            target_date=None,
            history_days=30,
            simulation_count=1000,
            p50_days=5,
            p85_days=6,
            p95_days=7,
            p50_date=date.today(),
            p85_date=date.today(),
            p95_date=date.today(),
            p50_items=None,
            p85_items=None,
            p95_items=None,
            throughput_mean=5.0,
            throughput_stddev=1.0,
        )

        with pytest.raises(AttributeError):
            result.forecast_id = "new-id"
