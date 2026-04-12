"""Orchestrator that wires the benchmarking compute functions into the daily job.

The benchmarking compute primitives (baselines, anomalies, correlations, maturity
bands, period comparisons) all exist as pure functions. This module fetches the
underlying time-series from the sink, invokes each primitive across a default
metric set, and persists the results via the sink's ``write_*`` methods.

Each metric is wrapped in its own try/except so a single failure does not halt
the overall run.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from dev_health_ops.metrics.benchmarking import generate_benchmark_insights
from dev_health_ops.metrics.benchmarking._common import (
    fetch_metric_series_by_scope,
)
from dev_health_ops.metrics.benchmarking.anomalies import (
    build_metric_anomalies_from_clickhouse,
)
from dev_health_ops.metrics.benchmarking.baselines import (
    build_internal_baselines_from_clickhouse,
)
from dev_health_ops.metrics.benchmarking.correlations import (
    DEFAULT_CORRELATION_PAIRS,
    build_metric_correlation_from_clickhouse,
)
from dev_health_ops.metrics.benchmarking.maturity import classify_maturity_bands
from dev_health_ops.metrics.benchmarking.period_comparison import (
    compute_period_comparison,
)
from dev_health_ops.metrics.testops_schemas import (
    BenchmarkAnomalyRecord,
    BenchmarkBaselineRecord,
    MaturityBandRecord,
    MetricCorrelationRecord,
    PeriodComparisonRecord,
)

logger = logging.getLogger(__name__)

# (metric_name, scope_type) pairs to benchmark. Covers pipeline/test/coverage
# tables so each of the six benchmark tables gets populated.
DEFAULT_BENCHMARK_METRICS: tuple[tuple[str, str], ...] = (
    ("success_rate", "repo"),
    ("failure_rate", "repo"),
    ("p95_duration_seconds", "repo"),
    ("rerun_rate", "team"),
    ("pass_rate", "repo"),
    ("flake_rate", "team"),
    ("failure_recurrence_score", "team"),
    ("line_coverage_pct", "repo"),
    ("branch_coverage_pct", "repo"),
    ("coverage_delta_pct", "repo"),
)

# Period comparison windows: current 7d vs prior 7d.
PERIOD_COMPARISON_CURRENT_DAYS = 7
PERIOD_COMPARISON_PRIOR_DAYS = 7

# Correlation window (days).
CORRELATION_WINDOW_DAYS = 30


def _build_period_comparisons(
    sink: Any,
    *,
    metric_name: str,
    scope_type: str,
    as_of_day: date,
    computed_at: datetime,
    org_id: str,
) -> list[PeriodComparisonRecord]:
    """Loop scopes manually since the ``_from_clickhouse`` helper is single-scope."""
    current_end = as_of_day
    current_start = as_of_day - timedelta(days=PERIOD_COMPARISON_CURRENT_DAYS - 1)
    prior_end = current_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=PERIOD_COMPARISON_PRIOR_DAYS - 1)

    current_series = fetch_metric_series_by_scope(
        sink,
        metric_name=metric_name,
        start_day=current_start,
        end_day=current_end,
        scope_type=scope_type,
    )
    prior_series = fetch_metric_series_by_scope(
        sink,
        metric_name=metric_name,
        start_day=prior_start,
        end_day=prior_end,
        scope_type=scope_type,
    )

    records: list[PeriodComparisonRecord] = []
    for scope_key in sorted(set(current_series) & set(prior_series)):
        record = compute_period_comparison(
            metric_name=metric_name,
            scope_type=scope_type,
            scope_key=scope_key,
            current_period_start=current_start,
            current_period_end=current_end,
            comparison_period_start=prior_start,
            comparison_period_end=prior_end,
            current_period_points=current_series[scope_key],
            comparison_period_points=prior_series[scope_key],
            computed_at=computed_at,
            org_id=org_id,
        )
        if record is not None:
            records.append(record)
    return records


def compute_benchmarking_for_day(
    sink: Any,
    *,
    as_of_day: date,
    computed_at: datetime,
    org_id: str,
    metrics: tuple[tuple[str, str], ...] = DEFAULT_BENCHMARK_METRICS,
    correlation_pairs: tuple[tuple[str, str, str], ...] = DEFAULT_CORRELATION_PAIRS,
) -> dict[str, list[Any]]:
    """Compute all benchmarking records for the given day.

    Returns a dict with keys ``baselines``, ``maturity_bands``, ``anomalies``,
    ``period_comparisons``, ``correlations``, ``insights``. Each metric and
    correlation pair is isolated via try/except so one failure cannot halt the
    rest of the run.
    """
    baselines: list[BenchmarkBaselineRecord] = []
    maturity_bands: list[MaturityBandRecord] = []
    anomalies: list[BenchmarkAnomalyRecord] = []
    period_comparisons: list[PeriodComparisonRecord] = []
    correlations: list[MetricCorrelationRecord] = []

    for metric_name, scope_type in metrics:
        try:
            metric_baselines = build_internal_baselines_from_clickhouse(
                sink,
                metric_name=metric_name,
                scope_type=scope_type,
                as_of_day=as_of_day,
                computed_at=computed_at,
                org_id=org_id,
            )
            baselines.extend(metric_baselines)
            maturity_bands.extend(
                classify_maturity_bands(metric_baselines, computed_at=computed_at)
            )
        except Exception as exc:
            logger.warning(
                "Benchmark baselines failed: metric=%s scope=%s err=%s",
                metric_name,
                scope_type,
                exc,
            )

        try:
            anomalies.extend(
                build_metric_anomalies_from_clickhouse(
                    sink,
                    metric_name=metric_name,
                    scope_type=scope_type,
                    as_of_day=as_of_day,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )
        except Exception as exc:
            logger.warning(
                "Benchmark anomalies failed: metric=%s scope=%s err=%s",
                metric_name,
                scope_type,
                exc,
            )

        try:
            period_comparisons.extend(
                _build_period_comparisons(
                    sink,
                    metric_name=metric_name,
                    scope_type=scope_type,
                    as_of_day=as_of_day,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )
        except Exception as exc:
            logger.warning(
                "Period comparison failed: metric=%s scope=%s err=%s",
                metric_name,
                scope_type,
                exc,
            )

    corr_end = as_of_day
    corr_start = as_of_day - timedelta(days=CORRELATION_WINDOW_DAYS - 1)
    for metric_name, paired_metric_name, scope_type in correlation_pairs:
        try:
            correlations.extend(
                build_metric_correlation_from_clickhouse(
                    sink,
                    metric_name=metric_name,
                    paired_metric_name=paired_metric_name,
                    scope_type=scope_type,
                    period_start=corr_start,
                    period_end=corr_end,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )
        except Exception as exc:
            logger.warning(
                "Correlation failed: %s vs %s scope=%s err=%s",
                metric_name,
                paired_metric_name,
                scope_type,
                exc,
            )

    insights = generate_benchmark_insights(
        period_comparisons=period_comparisons,
        anomalies=anomalies,
        correlations=correlations,
        computed_at=computed_at,
    )

    return {
        "baselines": baselines,
        "maturity_bands": maturity_bands,
        "anomalies": anomalies,
        "period_comparisons": period_comparisons,
        "correlations": correlations,
        "insights": insights,
    }


def write_benchmarking_outputs(sink: Any, outputs: dict[str, list[Any]]) -> None:
    """Persist benchmarking records to the sink."""
    if outputs.get("baselines"):
        sink.write_benchmark_baselines(outputs["baselines"])
    if outputs.get("maturity_bands"):
        sink.write_maturity_bands(outputs["maturity_bands"])
    if outputs.get("anomalies"):
        sink.write_benchmark_anomalies(outputs["anomalies"])
    if outputs.get("period_comparisons"):
        sink.write_period_comparisons(outputs["period_comparisons"])
    if outputs.get("correlations"):
        sink.write_metric_correlations(outputs["correlations"])
    if outputs.get("insights"):
        sink.write_benchmark_insights(outputs["insights"])


def run_benchmarking_for_day(
    sink: Any,
    *,
    as_of_day: date,
    computed_at: datetime,
    org_id: str,
) -> dict[str, list[Any]]:
    """Convenience wrapper: compute + write for a single day."""
    outputs = compute_benchmarking_for_day(
        sink,
        as_of_day=as_of_day,
        computed_at=computed_at,
        org_id=org_id,
    )
    write_benchmarking_outputs(sink, outputs)
    return outputs


__all__ = [
    "CORRELATION_WINDOW_DAYS",
    "DEFAULT_BENCHMARK_METRICS",
    "PERIOD_COMPARISON_CURRENT_DAYS",
    "PERIOD_COMPARISON_PRIOR_DAYS",
    "compute_benchmarking_for_day",
    "run_benchmarking_for_day",
    "write_benchmarking_outputs",
]
