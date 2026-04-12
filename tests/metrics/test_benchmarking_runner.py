"""Unit tests for the benchmarking runner.

Uses a fake sink that returns canned ``query_dicts`` results per metric, and
asserts that every benchmark ``write_*`` method receives non-empty rows.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dev_health_ops.metrics.benchmarking._common import METRIC_DEFINITIONS
from dev_health_ops.metrics.benchmarking.runner import (
    DEFAULT_BENCHMARK_METRICS,
    compute_benchmarking_for_day,
    run_benchmarking_for_day,
)

AS_OF = date(2026, 4, 10)
COMPUTED_AT = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
ORG = "test-org"


class FakeSink:
    """Minimal sink that returns deterministic time-series via query_dicts."""

    def __init__(self) -> None:
        self.baselines: list[Any] = []
        self.maturity_bands: list[Any] = []
        self.anomalies: list[Any] = []
        self.period_comparisons: list[Any] = []
        self.correlations: list[Any] = []
        self.insights: list[Any] = []

    def query_dicts(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        start_day = params["start_day"] if params else AS_OF - timedelta(days=40)
        end_day = params["end_day"] if params else AS_OF

        # Produce three scopes with a mild linear trend so anomalies may trigger
        # when the final day spikes.
        rows: list[dict[str, Any]] = []
        scopes = ["scope-a", "scope-b", "scope-c"]
        day = start_day
        idx = 0
        while day <= end_day:
            for i, scope in enumerate(scopes):
                # Base trend + per-scope offset + a spike on the last day of scope-a
                value = 0.5 + (i * 0.1) + (idx * 0.005)
                if scope == "scope-a" and day == end_day:
                    value += 0.8  # force an anomaly
                rows.append({"scope_key": scope, "day": day, "value": value})
            idx += 1
            day += timedelta(days=1)
        return rows

    def write_benchmark_baselines(self, rows: Sequence[Any]) -> None:
        self.baselines.extend(rows)

    def write_maturity_bands(self, rows: Sequence[Any]) -> None:
        self.maturity_bands.extend(rows)

    def write_benchmark_anomalies(self, rows: Sequence[Any]) -> None:
        self.anomalies.extend(rows)

    def write_period_comparisons(self, rows: Sequence[Any]) -> None:
        self.period_comparisons.extend(rows)

    def write_metric_correlations(self, rows: Sequence[Any]) -> None:
        self.correlations.extend(rows)

    def write_benchmark_insights(self, rows: Sequence[Any]) -> None:
        self.insights.extend(rows)


def test_default_metrics_are_supported() -> None:
    for metric_name, scope_type in DEFAULT_BENCHMARK_METRICS:
        definition = METRIC_DEFINITIONS[metric_name]
        assert scope_type in definition.scope_support, (
            f"{metric_name} does not support scope {scope_type}"
        )


def test_compute_benchmarking_populates_all_categories() -> None:
    sink = FakeSink()
    outputs = compute_benchmarking_for_day(
        sink,
        as_of_day=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG,
    )
    assert outputs["baselines"], "expected baselines"
    assert outputs["maturity_bands"], "expected maturity bands"
    assert outputs["anomalies"], "expected anomalies (spike was injected)"
    assert outputs["period_comparisons"], "expected period comparisons"
    assert outputs["correlations"], "expected correlations"
    assert outputs["insights"], "expected insights derived from outputs"


def test_run_benchmarking_writes_to_all_six_sinks() -> None:
    sink = FakeSink()
    run_benchmarking_for_day(
        sink,
        as_of_day=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG,
    )
    assert sink.baselines
    assert sink.maturity_bands
    assert sink.anomalies
    assert sink.period_comparisons
    assert sink.correlations
    assert sink.insights


def test_failing_metric_does_not_halt_run() -> None:
    class FlakySink(FakeSink):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def query_dicts(
            self, query: str, params: dict[str, Any] | None = None
        ) -> list[dict[str, Any]]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("simulated query failure")
            return super().query_dicts(query, params)

    sink = FlakySink()
    outputs = compute_benchmarking_for_day(
        sink,
        as_of_day=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG,
    )
    # Despite one failure, the rest of the metrics still produce records.
    assert outputs["baselines"]
